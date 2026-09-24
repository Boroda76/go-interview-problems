package main

import (
	"fmt"

	"golang.org/x/tour/tree"
)

func walker(t *tree.Tree, ch chan int) {
	if t == nil {
		return
	}

	walker(t.Left, ch)
	ch <- t.Value
	fmt.Println(t.Value)

	walker(t.Right, ch)
}

// Walk walks the tree t sending all values
// from the tree to the channel ch.
func Walk(t *tree.Tree, ch chan int) {
	defer close(ch)

	walker(t, ch)
}

// Same determines whether the trees
// t1 and t2 contain the same values.
func Same(t1, t2 *tree.Tree) bool {
	ch1 := make(chan int)
	ch2 := make(chan int)

	go func() {
		Walk(t1, ch1)
	}()
	go func() {
		Walk(t2, ch2)
	}()

	var isDifferent bool
	for {
		v1, ok1 := <-ch1
		v2, ok2 := <-ch2
		if !ok1 && !ok2 && !isDifferent {
			return true
		}
		if v1 != v2 {
			isDifferent = true
		}
		if isDifferent && !ok1 && !ok2 {
			return false
		}
	}
}

func main() {
	t1 := tree.New(1)
	t2 := tree.New(2)
	fmt.Print(Same(t1, t2))
}
