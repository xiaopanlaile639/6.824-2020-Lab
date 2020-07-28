package raft

import "log"

// Debugging
//const Debug = 1
const Debug = 0
const Level = 2


func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

func MyDPrintf(format string, level int,a ...interface{}) (n int, err error) {
	if level>Level{
		DPrintf(format,a)
	}
	return
}