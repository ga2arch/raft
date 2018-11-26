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
	Id           string
	CurrentState State
	Peers        []*Peer
	CurrentTerm  int
	CommitIndex  int
	LastApplied  int
	Log          []LogEntry
	VotedFor     string

	// actor
	MessageChan   chan Message
	HeartbeatChan chan int

	// leader only
	NextIndex  [] int
	MatchIndex [] int
}

func NewFollowerNode() *Node {
	return &Node{CurrentState: FOLLOWER,
		Id:          uuid.NewV4().String(),
		MessageChan: make(chan Message),
		Log:         []LogEntry{{Command: "", Term: 0}},
		CurrentTerm: 0,
		CommitIndex: 0,
		LastApplied: 0,
		VotedFor:    ""}
}

func (node *Node) Send(m Message) {
	node.MessageChan <- m
}

func (node *Node) Run() {
	for {
		msg := <-node.MessageChan
		switch msg.Payload.(type) {
		case ChangeStateCmd:
			node.changeState(msg)

		case SendHeartbeatsCmd:
			node.sendHeartbeats()

		case AppendEntriesCmd:
			node.appendEntries(msg)

		case RequestVoteCmd:
			node.requestVote(msg)

		case HandshakeCmd:
			go node.handShake(msg)

		case AddPeerCmd:
			node.addPeer(msg)
		}
	}
}

func (node *Node) changeState(message Message) {
	payload := message.Payload.(ChangeStateCmd)

	switch payload.State {
	case LEADER:
		node.startHeartbeat()

	case FOLLOWER:
		if node.CurrentState == LEADER {
			node.stopHeartbeat()
		}

	case CANDIDATE:

	}

	node.CurrentState = payload.State
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
	lastLogIndex := len(node.Log) - 1
	lastLogTem := node.Log[lastLogIndex].Term
	commitIndex := node.CommitIndex

	for i := range node.Peers {
		go node.sendHeartbeat(node.Peers[i], node.Id, node.CurrentTerm, lastLogIndex, lastLogTem, commitIndex, node.MessageChan)
	}
}

func (node *Node) sendHeartbeat(
	peer *Peer,
	nodeId string,
	currentTerm, lastLogIndex, lastLogTerm, commitIndex int,
	sender chan Message) {

	log.Printf("sending AppendEntries to %v", peer)

	var reply AppendEntriesResponse
	err := peer.Client.Call("Node.AppendEntries", &AppendEntriesCmd{
		Term:         currentTerm,
		LeaderId:     nodeId,
		PrevLogIndex: lastLogIndex,
		PrevLogTerm:  lastLogTerm,
		Entries:      make([]Entry, 0),
		LeaderCommit: commitIndex,
	}, &reply)

	if err != nil {
		log.Printf("error while calling %v, err: %s", peer, err)

	} else {
		sender <- Message{Payload: reply, Sender: nil}
	}
}

func (node *Node) appendEntries(message Message) {
	req := message.Payload.(AppendEntriesCmd)
	sender := message.Sender

	log.Printf("AppendEntries: node: %+v, req: %+v", node, req)

	if req.Term < node.CurrentTerm {
		log.Printf("invalid Term")
		sender <- Message{Payload: AppendEntriesResponse{Term: node.CurrentTerm, Success: false}}
	}

	if len(req.Entries) == 0 {
		sender <- Message{Payload: AppendEntriesResponse{Term: node.CurrentTerm, Success: true}}
		return
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
		for i := 0; i < (node.CommitIndex - node.LastApplied); i++ {
			log.Printf("applying %v", node.Log[node.LastApplied+i])
		}
		node.LastApplied = node.CommitIndex
	}

	if req.Term > node.CurrentTerm {
		node.MessageChan <- Message{Payload: ChangeStateCmd{Term: req.Term, State: FOLLOWER}}
	}
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

	sender <- Message{Payload: RequestVoteResponse{Term: node.CurrentTerm, VoteGranted: true}}

	if req.Term > node.CurrentTerm {
		node.MessageChan <- Message{Payload: ChangeStateCmd{Term: req.Term, State: FOLLOWER}}
	}
}

type AddPeerCmd struct {
	Peer Peer
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
}

func (node *Node) addPeer(message Message) {
	req := message.Payload.(AddPeerCmd)
	node.Peers = append(node.Peers, &req.Peer)

	if node.CurrentState == LEADER {
		node.NextIndex = append(node.NextIndex, len(node.Log))
		node.MatchIndex = append(node.MatchIndex, 0)
	}
}
