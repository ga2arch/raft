package main

import (
	"log"
	"math/rand"
	"net"
	"net/rpc"
	"os"
	"raft/raft"
	"time"
)


func main() {
	rand.Seed(time.Now().UTC().UnixNano())
	node := raft.NewFollowerNode()

	// starting rpc server
	err := rpc.Register(node)
	if err != nil {
		panic(err)
	}

	l, e := net.Listen("tcp", os.Args[1])
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go func() {
		for {
			conn, _ := l.Accept()
			go rpc.ServeConn(conn)
		}
	}()

	go node.Run()

	// iterate Peers to connect from arguments
	for i := 2; i < len(os.Args); i++ {
		addr := os.Args[i]

		// connect to peer
		log.Printf("dialing %s", addr)
		client, err := rpc.Dial("tcp", addr)
		if err != nil {
			log.Printf("cannot dial %s", addr)
			continue
		}
		log.Printf("connected to %s", addr)

		var reply HandshakeResponse
		err = client.Call("Node.Handshake", &HandshakeRequest{Addr: "127.0.0.1" + os.Args[1]}, &reply)
		if err != nil {
			log.Printf("failed handshake with %s, err: %s", addr, err)
		}

		// create peer
		peer := &Peer{Addr: os.Args[i], Client: client}
		node.Peers = append(node.Peers, peer)
	}

	log.Printf("connected to %d Peers", len(node.Peers))

}

//func (node *Node) Handshake(req *HandshakeCmd, reply *HandshakeResponse) error {
//	node.Mutex.Lock()
//	defer node.Mutex.Unlock()
//
//	// connect to peer
//	log.Printf("handshaking with %s", req.Addr)
//	client, err := rpc.Dial("tcp", req.Addr)
//	if err != nil {
//		return errors.New("cannot dial")
//	}
//	log.Printf("connected to %s", req.Addr)
//
//	// create peer
//	peer := &Peer{Addr: req.Addr, Client: client}
//	node.Peers = append(node.Peers, peer)
//
//	if node.CurrentState == LEADER {
//		node.NextIndex = append(node.NextIndex, len(node.Log))
//		node.MatchIndex = append(node.MatchIndex, 0)
//	}
//
//	reply = &HandshakeResponse{}
//
//	return nil
//}
//
//func (node *Node) AppendEntries(req *AppendEntriesCmd, reply *AppendEntriesResponse) error {
//	log.Printf("AppendEntries: node: %+v, req: %+v", node, req)
//
//	node.PingChan <- 1
//	node.Mutex.Lock()
//	defer node.Mutex.Unlock()
//
//	if req.Term < node.CurrentTerm {
//		return errors.New("invalid Term")
//	}
//
//	if len(req.Entries) == 0 {
//		reply = &AppendEntriesResponse{Term: node.CurrentTerm}
//		return nil
//	}
//
//	if req.PrevLogIndex >= len(node.Log) {
//		return errors.New("invalid prev log")
//	}
//
//	node.Log = append(node.Log[:req.PrevLogIndex+1], req.Entries...)
//
//	if req.LeaderCommit > node.CommitIndex {
//		node.CommitIndex = min(req.LeaderCommit, len(node.Log)-1)
//	}
//
//	if node.CommitIndex > node.LastApplied {
//		for i := 0; i < (node.CommitIndex - node.LastApplied); i++ {
//			log.Printf("applying %v", node.Log[node.LastApplied+i])
//		}
//		node.LastApplied = node.CommitIndex
//	}
//
//	if req.Term > node.CurrentTerm {
//		node.CurrentState = FOLLOWER
//		node.CurrentTerm = req.Term
//	}
//
//	reply = &AppendEntriesResponse{Term: node.CurrentTerm}
//	return nil
//}
//
//func (node *Node) RequestVote(req *RequestVoteCmd, reply *RequestVoteResponse) error {
//	log.Printf("RequestVote: node: %+v, req: %+v", node, req)
//
//	node.Mutex.Lock()
//	defer node.Mutex.Unlock()
//
//	if req.Term < node.CurrentTerm {
//		return errors.New("request Term is less than current Term")
//	}
//
//	if node.VotedFor == "" || node.VotedFor == req.CandidateId {
//		if req.LastLogIndex+1 != len(node.Log) {
//			return errors.New("last log index is out of sync")
//		}
//
//		entry := node.Log[req.LastLogIndex]
//		if entry.Term != req.LastLogTerm {
//			return errors.New("last log term is out of sync")
//		}
//	}
//
//	if req.Term > node.CurrentTerm {
//		node.CurrentState = FOLLOWER
//		node.CurrentTerm = req.Term
//	}
//
//	reply = &RequestVoteResponse{Term: node.CurrentTerm}
//	return nil
//}
//
//func main() {
//	rand.Seed(time.Now().UTC().UnixNano())
//	node := NewFollowerNode()
//	var once sync.Once
//
//	// starting rpc server
//	err := rpc.Register(node)
//	if err != nil {
//		panic(err)
//	}
//
//	l, e := net.Listen("tcp", os.Args[1])
//	if e != nil {
//		log.Fatal("listen error:", e)
//	}
//	go func() {
//		for {
//			conn, _ := l.Accept()
//			go rpc.ServeConn(conn)
//		}
//	}()
//
//	// iterate Peers to connect from arguments
//	for i := 2; i < len(os.Args); i++ {
//		addr := os.Args[i]
//
//		// connect to peer
//		log.Printf("dialing %s", addr)
//		client, err := rpc.Dial("tcp", addr)
//		if err != nil {
//			log.Printf("cannot dial %s", addr)
//			continue
//		}
//		log.Printf("connected to %s", addr)
//
//		var reply HandshakeResponse
//		err = client.Call("Node.Handshake", &HandshakeCmd{Addr: "127.0.0.1" + os.Args[1]}, &reply)
//		if err != nil {
//			log.Printf("failed handshake with %s, err: %s", addr, err)
//		}
//
//		// create peer
//		peer := &Peer{Addr: os.Args[i], Client: client}
//		node.Peers = append(node.Peers, peer)
//	}
//
//	log.Printf("connected to %d Peers", len(node.Peers))
//
//	// run main loop
//	for {
//		if node.CurrentState == LEADER {
//			node.sendHeartbeat()
//			go func() {
//				once.Do(func() {
//					time.Sleep(10 * time.Second)
//					node.AppendCommand("SET kek lol")
//
//					time.Sleep(15 * time.Second)
//					node.AppendCommand("SET g b")
//
//				})
//			}()
//
//			time.Sleep(100 * time.Millisecond)
//
//		} else {
//			select {
//			case _ = <-node.PingChan:
//				log.Printf("received hearthbeat from leader")
//				continue
//
//			case <-time.After(time.Duration(randInt(1500, 3000)) * time.Millisecond):
//				node.Mutex.Lock()
//				if node.CurrentState == LEADER {
//					node.Mutex.Unlock()
//					continue
//				}
//				log.Printf("starting election")
//				node.CurrentState = CANDIDATE
//				node.CurrentTerm += 1
//
//				node.Mutex.Unlock()
//
//				go func() {
//					quorumResult := 1
//
//					for _, peer := range node.Peers {
//
//						node.Mutex.Lock()
//						lastLogIndex := len(node.Log) - 1
//						lastLogTem := 0
//						if lastLogIndex >= 0 {
//							lastLogTem = node.Log[lastLogIndex].Term
//
//						} else {
//							lastLogIndex = 0
//						}
//						node.Mutex.Unlock()
//
//						var reply RequestVoteResponse
//						err := peer.Client.Call("Node.RequestVote", &RequestVoteCmd{
//							Term:         node.CurrentTerm,
//							CandidateId:  node.Id,
//							LastLogTerm:  lastLogTem,
//							LastLogIndex: lastLogIndex,
//						}, &reply)
//
//						if err != nil {
//							log.Printf("error while calling %v, err: %s", peer, err)
//
//						} else {
//							quorumResult += 1
//						}
//					}
//
//					if quorumResult > len(node.Peers)/2 {
//						node.Mutex.Lock()
//						if node.CurrentState == CANDIDATE {
//							node.Become(LEADER)
//						}
//						node.Mutex.Unlock()
//						node.sendHeartbeat()
//					}
//				}()
//			}
//		}
//	}
//}
//
//func (node *Node) sendHeartbeat() {
//	log.Printf("sending heartbeat to %v", node.Peers)
//
//	node.Mutex.Lock()
//	lastLogIndex := len(node.Log) - 1
//	lastLogTem := node.Log[lastLogIndex].Term
//	commitIndex := node.CommitIndex
//	node.Mutex.Unlock()
//
//	for _, peer := range node.Peers {
//		go pingPeer(peer, node, lastLogIndex, lastLogTem, commitIndex)
//	}
//}
//
//func (node *Node) AppendCommand(cmd string) {
//	log.Printf("appending command to log %+v", cmd)
//
//	node.Mutex.Lock()
//	node.Log = append(node.Log, LogEntry{Command: cmd, Term: node.CurrentTerm})
//	lastLogIndex := len(node.Log)
//	commitIndex := node.CommitIndex
//	node.Mutex.Unlock()
//
//	log.Printf("%+v", node.NextIndex)
//
//	for i := 0; i < len(node.Peers); i++ {
//		peer := node.Peers[i]
//		nextIndex := node.NextIndex[i] // ni: 1 lli: 1
//		prevLogIndex := nextIndex-1
//		var entries []LogEntry
//		if lastLogIndex >= nextIndex {
//			for x := nextIndex; x < lastLogIndex; x++ {
//				entries = append(entries, node.Log[x])
//			}
//		}
//
//		var reply AppendEntriesResponse
//		log.Printf("sending AppendEntries to %v, entries: %+v", peer, entries)
//		err := peer.Client.Call("Node.AppendEntries", &AppendEntriesCmd{
//			Term:         node.CurrentTerm,
//			LeaderId:     node.Id,
//			PrevLogIndex: prevLogIndex,
//			PrevLogTerm:  node.Log[prevLogIndex].Term,
//			Entries:      entries,
//			LeaderCommit: commitIndex,
//		}, &reply)
//
//		if err != nil {
//			log.Printf("error while calling %v, err: %s", peer, err)
//			node.NextIndex[i] -= 1
//			i -= 1
//
//		} else {
//			node.NextIndex[i] = len(node.Log)
//			node.MatchIndex[i] = len(node.Log) - 1
//		}
//	}
//
//	for n := commitIndex + 1; n < len(node.Log) && node.Log[n].Term == node.CurrentTerm; n++ {
//		matches := 0
//		for i := range node.Peers {
//			if node.MatchIndex[i] >= n {
//				matches += 1
//			}
//		}
//
//		if matches > len(node.Peers)/2 {
//			log.Printf("update commit index to: %d", n)
//			node.CommitIndex = n
//			break
//		}
//	}
//
//	if node.CommitIndex > node.LastApplied {
//		for i := 0; i < (node.CommitIndex - node.LastApplied); i++ {
//			log.Printf("applying %v", node.Log[node.LastApplied+i])
//		}
//		node.LastApplied = node.CommitIndex
//	}
//}
//
//func (node *Node) Become(state State) {
//	node.CurrentState = state
//	if state == LEADER {
//		log.Printf("i'm leader")
//
//		node.MatchIndex = make([]int, len(node.Peers))
//		node.NextIndex = make([]int, len(node.Peers))
//		for i := range node.Peers {
//			node.NextIndex[i] = len(node.Log)
//			node.MatchIndex[i] = 0
//		}
//	}
//}
//
//func pingPeer(peer *Peer, node *Node, lastLogIndex int, lastLogTem int, commitIndex int) {
//	var reply AppendEntriesResponse
//	log.Printf("sending AppendEntries to %v", peer)
//	err := peer.Client.Call("Node.AppendEntries", &AppendEntriesCmd{
//		Term:         node.CurrentTerm,
//		LeaderId:     node.Id,
//		PrevLogIndex: lastLogIndex,
//		PrevLogTerm:  lastLogTem,
//		Entries:      make([]LogEntry, 0),
//		LeaderCommit: commitIndex,
//	}, &reply)
//	if err != nil {
//		log.Printf("error while calling %v, err: %s", peer, err)
//
//	}
//}
