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