package main

import (
	"bufio"
	"crypto/rand"
	"fmt"
	"math/big"
	"os"
	"strings"
	"time"

	"copskv"
)

type context struct {
	cluster int
	clerk *copskv.Clerk
}

var conf *copskv.Config
var contexts map[int64]context

var clientA, clientB, clientC, clientD int64

func main() {
	fmt.Println("Welcome to the COPS demo system")
	conf = copskv.MakeSystem()
	contexts = make(map[int64]context)

	// client A & B: clients of cluster 0
	clientA = CreateContext(0)
	clientB = CreateContext(0)
	// client C & D: clients of cluster 1
	clientC = CreateContext(1)
	clientD = CreateContext(1)

	if len(os.Args) > 1 && os.Args[1] == "i" {
		interactive()
	} else {
		noninteractive()
	}

	DeleteContext(clientA)
	DeleteContext(clientB)
	DeleteContext(clientC)
	DeleteContext(clientD)
	conf.Cleanup()
}

func noninteractive() {
	Put("0", "abcd", clientB)
	res := Get("0", clientA)
	fmt.Println(res)
	
	Put("0", "efgh", clientA)
	res = Get("0", clientB)
	fmt.Println(res)

	time.Sleep(5000 * time.Millisecond)

	res = Get("0", clientC)
	fmt.Println(res)

	time.Sleep(5000 * time.Millisecond)

	res = Get("0", clientC)
	fmt.Println(res)
}

func interactive() {
	fmt.Println("'quit' to exit")
	fmt.Println("client A, B, are in cluster 0, C, D are in cluster 1")
	fmt.Println("usage: A/B/C/D put/get key [value]")
	reader := bufio.NewReader(os.Stdin)
	for {
		fmt.Printf("> ")
		text, err := reader.ReadString('\n')
		if err != nil {
			continue
		}
		text = text[:len(text) - 1]
		if text == "quit" {
			return
		}
		args := strings.Split(text, " ")
		if len(args) == 4 {
			client := clientByName(args[0])
			if client == -1 {
				continue
			}
			if args[1] != "put" {
				continue
			}
			key := args[2]
			value := args[3]
			res := Put(key, value, client)
			fmt.Println(res)
		} else if len(args) == 3 {
			client := clientByName(args[0])
			if client == -1 {
				continue
			}
			if args[1] != "get" {
				continue
			}
			key := args[2]
			val := Get(key, client)
			fmt.Println(val)
		}
	}
}

func clientByName(name string) int64 {
	switch name {
	case "A":
		return clientA
	case "B":
		return clientB
	case "C":
		return clientC
	case "D":
		return clientD
	default:
		return -1
	}
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}
