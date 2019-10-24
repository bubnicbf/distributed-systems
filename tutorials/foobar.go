package mapreduce

import (
	"fmt"
	//"time"
)

func say(s string) {

	for i := 0; i < 100; i++ {
		//time.Sleep(100 * time.Millisecond)
		fmt.Println(s)
	}
}

func gen(nums ...int) <-chan int {

	out := make(chan int)
	go func() {
		for _, n := range nums {
			out <- n
		}
		close(out)

	}()
	return out
}

func doF(x int, y int, fnF func(a int, b int) int) int {

	res := fnF(x, y)
	return res
}

func plus(x int, y int) int {
	return x + y
}

func main() {

	/*
		fmt.Println("hello world")
		out := make(chan int)
		fmt.Printf("{}\n", out)
		seqChan := gen(10, 20, 30)
		go func() {
			for n := range seqChan {
				fmt.Printf("{}\n", n)
				out <- n * n
			}
			close(out)
		}()
	*/
	//go say("go go go")
	//say("seq seq seq")
	res := doF(1, 2, plus)
	fmt.Printf("%d\n", res)
}
