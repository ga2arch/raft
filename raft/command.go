package raft

type ChangeStateCmd struct {
	State State
	Term  int
}

type SendHeartbeatsCmd struct {
}
