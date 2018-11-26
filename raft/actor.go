package raft

type Message struct {
	Sender  chan Message
	Payload interface{}
}

