package main

import (
	"fmt"

	"golang.org/x/tour/tree"
)

func walker(t *tree.Tree, ch chan int) {
	if t == nil {
		return
	}

	if t.Left != nil {
		walker(t.Left, ch)
	}
	ch <- t.Value
	fmt.Println(t.Value)

	if t.Right != nil {
		walker(t.Right, ch)
	}
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

	for {
		v1, ok1 := <-ch1
		v2, ok2 := <-ch2
		if v1 != v2 {
			return false
		}
		if !ok1 && !ok2 {
			return true
		}
	}
}

func main() {
	t1 := tree.New(1)
	t2 := tree.New(1)
	fmt.Print(Same(t1, t2))
}
