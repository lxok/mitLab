package shardkv

//
// Sharded key/value server.
// Lots of replica groups, each running op-at-a-time paxos.
// Shardmaster decides which group serves each shard.
// Shardmaster may change shard assignment from time to time.
//
// You will have to modify these definitions.
//

//Err Type
const (
	OK            = "OK"
	ErrNoKey      = "ErrNoKey"
	ErrWrongGroup = "ErrWrongGroup"
	ErrNotInitial = "ErrNotInitial"
)

//Operation Type
const (
	Put          = "Put"
	Get          = "Get"
	Append       = "Append"
	ConfigChange = "ConfigChange" //config change
	ShardIntial  = "ShardInitial" //new shard be intialed
)

type Err string

type PutAppendArgs struct {
	Key   string
	Value string
	Op    string // "Put" or "Append"
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Id int64
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
	Id int64
}

type GetReply struct {
	Err   Err
	Value string
}

type SendShardArgs struct {
	ConfigNum int
	ShardNum  int
	Data      map[string]string
	Filter    map[int64]int
}

type SendShardReply struct {
	Err Err
}

type AcceptorSendShardArgs struct {
	ConfigNum int
	ShardNum  int
}

type AcceptorSendShardReply struct {
	Err Err
}

type ShardInitalArgs struct {
	ConfigNum int
	ShardNum  int
	SendState []bool
}

type ShardInitialReply struct {
	Err Err
}

/*
type CallBackSendShardArgs struct {
	MyGid    int64
	Me       string
	ShardNum int
}

type CallBackSendShardReply struct {
	Err Err
}
*/
