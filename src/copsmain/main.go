package main

import (
	"crypto/rand"
	"fmt"
	"math/big"

	"copskv"
)

type context struct {
	cluster int
	clerk *copskv.Clerk
}

var conf *copskv.Config
var contexts map[int64]context

func main() {
	fmt.Println("Hello!")
	conf = copskv.MakeSystem()
	contexts = make(map[int64]context)

	// client A & B: clients of cluster 0
	clientA := CreateContext(0)
	clientB := CreateContext(0)
	// client C & D: clients of cluster 1
	clientC := CreateContext(1)
	clientD := CreateContext(1)

	//

	Put("0", "abcd", clientA)
	res := Get("0", clientA)
	fmt.Println(res)

	res = Get("0", clientB)
	fmt.Println(res)

	res = Get("0", clientC)
	fmt.Println(res)

	//

	DeleteContext(clientA)
	DeleteContext(clientB)
	DeleteContext(clientC)
	DeleteContext(clientD)
	conf.Cleanup()
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func CreateContext(c int) int64 {
	ctx_id := nrand()
	clerk := conf.MakeClient(c)
	contexts[ctx_id] = context{ c, clerk }
	return ctx_id
}

func DeleteContext(ctx_id int64) bool {
	if context, ok := contexts[ctx_id]; ok {
		conf.DeleteClient(context.cluster, context.clerk)
		delete(contexts, ctx_id)
		return true
	} else {
		return false
	}
}

func Put(key string, val string, ctx_id int64) bool {
	ctx, ok := contexts[ctx_id]
	if !ok {
		return false
	}
	clerk := ctx.clerk
	clerk.Put(key, val)
	return true
}

func Get(key string, ctx_id int64) string {
	ctx, ok := contexts[ctx_id]
	if !ok {
		return ""
	}
	clerk := ctx.clerk
	val := clerk.Get(key)
	return val
}
