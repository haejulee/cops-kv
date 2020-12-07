package copskv

// var clerks map[int]Clerk // ctx_id : clerk


// func nrand() int64 {
// 	max := big.NewInt(int64(1) << 62)
// 	bigx, _ := rand.Int(rand.Reader, max)
// 	x := bigx.Int64()
// 	return x
// }

// func make_end() *labrpc.ClientEnd {
// 	// TODO
// }

// func CreateContext() int64 {
// 	ctx_id := nrand()
// 	clerks[ctx_id] = MakeClerk(masters, make_end)
// 	return ctx_id
// }

// func DeleteContext(ctx_id int) bool {
// 	if _, ok := clerks[ctx_id]; ok {
// 		delete(clerks, ctx_id)
// 		return true
// 	} else {
// 		return false
// 	}
// }

// func Put(key string, val string, ctx_id int) bool {
// 	clerk, ok := clerks[ctx_id]
// 	if !ok {
// 		return false
// 	}
// 	ok = clerk.put(key, val)
// 	return ok
// }

// func Get(key string, ctx_id int) string {
// 	clerk, ok := clerks[ctx_id]
// 	if !ok {
// 		return ""
// 	}
// 	val := clerk.get(key)
// 	return val
// }


// // Two datacenters with three shards of three servers + three master servers each

