package raft

import (
	"github.com/satori/go.uuid"
	"log"
	"net/rpc"
	"time"
)

type State int

const (
	FOLLOWER State = iota
	LEADER
	CANDIDATE
)

type Peer struct {
	Addr   string
	Client *rpc.Client
}

type LogEntry struct {
	Command string
	Term    int
}

type Node struct {
	Id              string
	CurrentState    State
	Peers           []*Peer
	PeersNum        int
	CurrentTerm     int
	CommitIndex     int
	LastApplied     int
	Log             []LogEntry
	VotedFor        string
	LastMessageTime time.Time

	// actor
	MessageChan   chan Message
	HeartbeatChan chan int

	// leader only
	NextIndex  [] int
	MatchIndex [] int

	// candidate only
	Votes int
}

func NewFollowerNode() *Node {
	return &Node{
		CurrentState:    FOLLOWER,
		Id:              uuid.NewV4().String(),
		MessageChan:     make(chan Message, 10),
		Log:             []LogEntry{{Command: "", Term: 0}},
		LastMessageTime: time.Now().UTC(),
	}
}

func (node *Node) Send(m Message) {
	node.MessageChan <- m
}

func (node *Node) AddPeer(peer *Peer) {
	node.Peers = append(node.Peers, peer)
	node.PeersNum += 1
}

func (node *Node) Run() {
	go node.startElectionTimer()

	for {
		msg := <-node.MessageChan
		log.Printf("node: %+v, received: %+v, type %T", node, msg, msg.Payload)

		switch msg.Payload.(type) {
		case ChangeStateCmd:
			node.changeState(msg)

		case SendHeartbeatsCmd:
			node.sendHeartbeats()

		case AppendEntriesCmd:
			node.LastMessageTime = time.Now().UTC()
			node.appendEntries(msg)

		case RequestVoteCmd:
			node.LastMessageTime = time.Now().UTC()
			node.requestVote(msg)

		case HandshakeCmd:
			go node.handShake(msg)

		case AddPeerCmd:
			node.addPeer(msg)

		case RemovePeerCmd:
			node.removePeer(msg)

		case AddLogCmd:
			node.addLog(msg)

		case IncrementPeerIndexesCmd:
			node.incrementPeerIndexes(msg)

		case DecrementPeerIndexesCmd:
			node.decrementPeerIndexes(msg)

		case AddVoteCmd:
			node.addVote()

		case StartElectionCmd:
			if node.CurrentState != LEADER && time.Since(node.LastMessageTime).Seconds()*1e3 >= 150 {
				node.MessageChan <- Message{Payload: ChangeStateCmd{State: CANDIDATE}}
			}
		}
	}
}

func (node *Node) changeState(message Message) {
	payload := message.Payload.(ChangeStateCmd)
	node.Votes = 0

	switch payload.State {
	case LEADER:
		node.CurrentState = LEADER

		log.Printf("become leader")
		go node.startHeartbeat()

		node.MatchIndex = make([]int, len(node.Peers))
		node.NextIndex = make([]int, len(node.Peers))
		for i := range node.Peers {
			if node.Peers[i] == nil {
				continue
			}

			node.NextIndex[i] = len(node.Log)
			node.MatchIndex[i] = 0
		}

	case FOLLOWER:
		log.Printf("become follower")
		if node.CurrentState == LEADER {
			node.stopHeartbeat()
		}
		node.CurrentTerm = payload.Term
		node.CurrentState = FOLLOWER
		node.VotedFor = ""

	case CANDIDATE:
		log.Printf("become candidate")
		node.CurrentState = CANDIDATE
		node.CurrentTerm += 1
		lastLogIndex := len(node.Log) - 1
		lastLogTem := node.Log[lastLogIndex].Term

		node.addVote()
		node.VotedFor = node.Id
		for i, peer := range node.Peers {
			if node.Peers[i] == nil {
				continue
			}

			go node.vote(peer, i, node.Id, node.CurrentTerm, lastLogIndex, lastLogTem, node.MessageChan)
		}
	}
}

func (node *Node) startHeartbeat() {
	node.HeartbeatChan = make(chan int)

	for {
		select {
		case <-node.HeartbeatChan:
			return

		case <-time.After(time.Duration(100 * time.Millisecond)):
			node.Send(Message{Payload: SendHeartbeatsCmd{}, Sender: nil})
		}
	}
}

func (node *Node) stopHeartbeat() {
	node.HeartbeatChan <- 1
}

func (node *Node) sendHeartbeats() {
	commitIndex := node.CommitIndex

	for i := range node.Peers {
		if node.Peers[i] == nil {
			continue
		}

		nextIndex := node.NextIndex[i]
		prevLogIndex := nextIndex - 1
		prevLogTerm := node.Log[prevLogIndex].Term

		go node.sendHeartbeat(i, node.Id, node.CurrentTerm, prevLogIndex, prevLogTerm, commitIndex, nil)
	}
}

func (node *Node) sendHeartbeat(
	peerPos int,
	nodeId string,
	currentTerm, lastLogIndex, lastLogTerm, commitIndex int,
	sender chan Message) {

	node.sendEntries(node.Peers[peerPos], peerPos, nodeId, currentTerm, lastLogIndex, lastLogTerm, commitIndex, make([]Entry, 0), sender)
}

func (node *Node) appendEntries(message Message) {
	req := message.Payload.(AppendEntriesCmd)
	sender := message.Sender

	log.Printf("AppendEntries: node: %+v, req: %+v", node, req)

	if req.Term < node.CurrentTerm {
		log.Printf("invalid Term")
		sender <- Message{Payload: AppendEntriesResponse{Term: node.CurrentTerm, Success: false}}
	}

	if req.PrevLogIndex >= len(node.Log) {
		log.Printf("invalid prev log")
		sender <- Message{Payload: AppendEntriesResponse{Term: node.CurrentTerm, Success: true}}
		return
	}

	for _, entry := range req.Entries {
		node.Log = append(node.Log[:req.PrevLogIndex+1], LogEntry{Term: entry.Term, Command: entry.Data})
	}

	if req.LeaderCommit > node.CommitIndex {
		node.CommitIndex = min(req.LeaderCommit, len(node.Log)-1)
	}

	if node.CommitIndex > node.LastApplied {
		for i := node.LastApplied + 1; i < node.CommitIndex+1; i++ {
			log.Printf("applying %v", node.Log[i])
		}
		node.LastApplied = node.CommitIndex
	}

	if req.Term > node.CurrentTerm || node.CurrentState == CANDIDATE {
		node.MessageChan <- Message{Payload: ChangeStateCmd{Term: req.Term, State: FOLLOWER}}
	}

	sender <- Message{Payload: AppendEntriesResponse{Term: node.CurrentTerm, Success: true}}
}

func (node *Node) requestVote(message Message) {
	req := message.Payload.(RequestVoteCmd)
	sender := message.Sender

	log.Printf("RequestVote: node: %+v, req: %+v", node, req)

	if req.Term < node.CurrentTerm {
		log.Printf("request Term is less than current term")
		sender <- Message{Payload: RequestVoteResponse{Term: node.CurrentTerm, VoteGranted: false}}
		return
	}

	if node.VotedFor == "" || node.VotedFor == req.CandidateId {
		if req.LastLogIndex+1 != len(node.Log) {
			log.Printf("last log index is out of sync")
			sender <- Message{Payload: RequestVoteResponse{Term: node.CurrentTerm, VoteGranted: false}}
			return
		}

		entry := node.Log[req.LastLogIndex]
		if entry.Term != req.LastLogTerm {
			log.Printf("last log term is out of sync")
			sender <- Message{Payload: RequestVoteResponse{Term: node.CurrentTerm, VoteGranted: false}}
			return
		}
	}

	if req.Term > node.CurrentTerm {
		node.MessageChan <- Message{Payload: ChangeStateCmd{Term: req.Term, State: FOLLOWER}}
	}

	if node.CurrentState != LEADER {
		sender <- Message{Payload: RequestVoteResponse{Term: node.CurrentTerm, VoteGranted: true}}

	} else {
		sender <- Message{Payload: RequestVoteResponse{Term: node.CurrentTerm, VoteGranted: false}}
	}
}

func (node *Node) handShake(message Message) {
	req := message.Payload.(HandshakeCmd)
	sender := message.Sender

	log.Printf("handshaking with %s", req.Addr)
	client, err := rpc.Dial("tcp", req.Addr)
	if err != nil {
		log.Printf("cannot dial")
		sender <- Message{Payload: HandshakeResponse{}}
		return
	}

	log.Printf("connected to %s", req.Addr)
	peer := Peer{Addr: req.Addr, Client: client}

	node.MessageChan <- Message{Payload: AddPeerCmd{Peer: peer}}
	sender <- Message{Payload: HandshakeResponse{}}
}

func (node *Node) addPeer(message Message) {
	req := message.Payload.(AddPeerCmd)
	node.AddPeer(&req.Peer)

	if node.CurrentState == LEADER {
		node.NextIndex = append(node.NextIndex, len(node.Log))
		node.MatchIndex = append(node.MatchIndex, 0)
	}
}

func (node *Node) removePeer(message Message) {
	req := message.Payload.(RemovePeerCmd)
	node.Peers[req.Pos] = nil
	node.PeersNum -= 1
}

func (node *Node) sendEntries(
	peer *Peer,
	peerPos int,
	nodeId string,
	currentTerm, lastLogIndex, lastLogTerm, commitIndex int,
	entries []Entry,
	sender chan Message) {

	log.Printf("sending AppendEntries to %v", peer)

	var reply AppendEntriesResponse
	err := peer.Client.Call("RpcConfig.AppendEntries", &AppendEntriesCmd{
		Term:         currentTerm,
		LeaderId:     nodeId,
		PrevLogIndex: lastLogIndex,
		PrevLogTerm:  lastLogTerm,
		Entries:      entries,
		LeaderCommit: commitIndex,
	}, &reply)

	log.Printf("append response %+v", reply)

	if err != nil {
		log.Printf("error while calling %v, err: %s", peer, err)
		if err == rpc.ErrShutdown {
			node.MessageChan <- Message{Payload: RemovePeerCmd{Pos: peerPos}, Sender: nil}
		}

	} else if reply.Term > node.CurrentTerm {
		node.MessageChan <- Message{Payload: ChangeStateCmd{Term: reply.Term, State: FOLLOWER}}

	} else if reply.Success {
		node.MessageChan <- Message{Payload: IncrementPeerIndexesCmd{Pos: peerPos}, Sender: nil}

	} else {
		node.MessageChan <- Message{Payload: DecrementPeerIndexesCmd{Pos: peerPos}, Sender: nil}
	}
}

func (node *Node) addLog(message Message) {
	req := message.Payload.(AddLogCmd)
	sender := message.Sender
	cmd := req.Command

	if node.CurrentState != LEADER {
		return
	}

	log.Printf("appending command to log %+v", cmd)

	node.Log = append(node.Log, LogEntry{Command: cmd, Term: node.CurrentTerm})
	lastLogIndex := len(node.Log)
	commitIndex := node.CommitIndex

	for i := 0; i < len(node.Peers); i++ {
		if node.Peers[i] == nil {
			continue
		}

		nextIndex := node.NextIndex[i]
		prevLogIndex := nextIndex - 1
		prevLogTerm := node.Log[prevLogIndex].Term

		var entries []Entry
		if lastLogIndex >= nextIndex {
			for x := nextIndex; x < lastLogIndex; x++ {
				logEntry := node.Log[x]
				entries = append(entries, Entry{logEntry.Command, logEntry.Term})
			}
		}

		go node.sendEntries(node.Peers[i], i, node.Id, node.CurrentTerm, prevLogIndex, prevLogTerm, commitIndex, entries, sender)
	}
}

func (node *Node) incrementPeerIndexes(message Message) {
	req := message.Payload.(IncrementPeerIndexesCmd)

	node.NextIndex[req.Pos] = len(node.Log)
	node.MatchIndex[req.Pos] = len(node.Log) - 1

	for n := node.CommitIndex + 1; n < len(node.Log) && node.Log[n].Term == node.CurrentTerm; n++ {
		matches := 0
		for i := range node.Peers {
			if node.Peers[i] == nil {
				continue
			}

			if node.MatchIndex[i] >= n {
				matches += 1
			}
		}

		if matches > node.PeersNum/2 {
			log.Printf("update commit index to: %d", n)
			node.CommitIndex = n
			break
		}
	}

	if node.CommitIndex > node.LastApplied {
		for i := node.LastApplied + 1; i < node.CommitIndex+1; i++ {
			log.Printf("applying %v", node.Log[i])
		}
		node.LastApplied = node.CommitIndex
	}
}

func (node *Node) decrementPeerIndexes(message Message) {
	req := message.Payload.(DecrementPeerIndexesCmd)

	node.NextIndex[req.Pos] -= 1
}

func (node *Node) vote(peer *Peer, peerPos int, nodeId string, currentTerm, lastLogIndex, lastLogTerm int, sender chan Message) {
	var reply RequestVoteResponse
	err := peer.Client.Call("RpcConfig.RequestVote", &RequestVoteCmd{
		Term:         currentTerm,
		CandidateId:  nodeId,
		LastLogTerm:  lastLogTerm,
		LastLogIndex: lastLogIndex,
	}, &reply)

	if err != nil {
		log.Printf("error while calling %v, err: %s", peer, err)
		if err == rpc.ErrShutdown {
			sender <- Message{Payload: RemovePeerCmd{Pos: peerPos}, Sender: nil}
		}

	} else if reply.Term > node.CurrentTerm {
		node.MessageChan <- Message{Payload: ChangeStateCmd{Term: reply.Term, State: FOLLOWER}}

	} else if sender != nil && reply.VoteGranted {
		sender <- Message{Payload: AddVoteCmd{}, Sender: nil}
	}
}

func (node *Node) addVote() {
	log.Printf("add vote")
	node.Votes += 1
	if node.Votes > (node.PeersNum+1)/2 && node.CurrentState == CANDIDATE {
		log.Printf("won voting")
		node.MessageChan <- Message{Payload: ChangeStateCmd{Term: node.CurrentTerm, State: LEADER}}
	}
}

func (node *Node) startElectionTimer() {
	for {
		_ = <-time.After(time.Duration(randInt(150, 300)) * time.Millisecond)
		node.MessageChan <- Message{Payload: StartElectionCmd{}}
	}
}
