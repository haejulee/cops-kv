package main


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
