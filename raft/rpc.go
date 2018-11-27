package raft

type Entry struct {
	Data string
	Term int
}

type AppendEntriesCmd struct {
	Term         int
	LeaderId     string
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []Entry
	LeaderCommit int
}

type AppendEntriesResponse struct {
	Term    int
	Success bool
}

type RequestVoteCmd struct {
	Term         int
	CandidateId  string
	LastLogIndex int
	LastLogTerm  int
}

type RequestVoteResponse struct {
	Term        int
	VoteGranted bool
}

type HandshakeCmd struct {
	Addr string
}

type HandshakeResponse struct {
}

type RpcConfig struct {
	NodeChan chan Message
}

func (config *RpcConfig) AppendEntries(req *AppendEntriesCmd, reply *AppendEntriesResponse) error {
	sender := make(chan Message)
	config.NodeChan <- Message{Sender: sender, Payload: *req}

	resp := <-sender
	*reply = resp.Payload.(AppendEntriesResponse)

	return nil
}

func (config *RpcConfig) RequestVote(req *RequestVoteCmd, reply *RequestVoteResponse) error {
	sender := make(chan Message)
	config.NodeChan <- Message{Sender: sender, Payload: *req}

	resp := <-sender
	*reply = resp.Payload.(RequestVoteResponse)

	return nil
}

func (config *RpcConfig) Handshake(req *HandshakeCmd, reply *HandshakeResponse) error {
	sender := make(chan Message)
	config.NodeChan <- Message{Sender: sender, Payload: *req}

	resp := <-sender
	*reply = resp.Payload.(HandshakeResponse)

	return nil
}