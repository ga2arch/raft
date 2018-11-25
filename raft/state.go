package raft

const (
	FOLLOWER = iota
	LEADER
	CANDIDATE
)

type Log struct {

}

type PersistentState struct {
	CurrentTerm int64
	VotedFor string
	Logs []Log
}

type State struct {
	CurrentState int64
	CommitIndex int64
	LastApplied int64
}

type LeaderState struct {
	NextIndexes []int64
	MatchIndexes []int64
}
