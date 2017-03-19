package main

import (
	"fmt"
)

//import "fmt"

func main() {
	var s1 string = "abc"
	var s2 string = "abc"
	fmt.Println(&s1 == &s2)
}
