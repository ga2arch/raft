package raft

import "math/rand"

func randInt(min int, max int) int {
	return min + rand.Intn(max-min)
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}

