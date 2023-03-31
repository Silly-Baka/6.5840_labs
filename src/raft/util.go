package raft

import "log"

// Debugging
const Debug = true

const Lock = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}
