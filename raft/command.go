package raft

type ChangeStateCmd struct {
	State State
	Term  int
}

type SendHeartbeatsCmd struct {
}

type AddLogCmd struct {
	Command string
}

type AddPeerCmd struct {
	Peer Peer
}

type RemovePeerCmd struct {
	Pos int
}

type IncrementPeerIndexesCmd struct {
	Pos int
}

type DecrementPeerIndexesCmd struct {
	Pos int
}

type AddVoteCmd struct {

}

type StartElectionCmd struct {

}