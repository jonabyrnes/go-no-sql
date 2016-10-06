package main

import (
	"time"
	"fmt"
)

func makeTimestamp() int64 {
	return time.Now().UnixNano()
}

func main() {
	time, _ := time.Parse(time.RFC3339, "2012-11-01T22:08:41+00:00")
	fmt.Printf("%d", time.UnixNano()) // 1351807721000000000
}
