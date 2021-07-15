package main

import (
	"fmt"
	"time"
)

func test() {
	start := time.Now()
	sum := 0
	for i := 0; i < 100000000*35; i++ {
		sum++
	}
	elapsed := time.Since(start)
	fmt.Println("run for: ", elapsed)
}

func main() {
	for {
		time.Sleep(1 * time.Second)
		fmt.Println("sleep for: ", 1*time.Second)
		test()
	}
}
