package pbservice

const (
	OK               = "OK"
	ErrNoKey         = "ErrNoKey"
	ErrWrongServer   = "ErrWrongServer"
	NotBecomeBackup  = "NotBecomeBackup"
	BackupWriteWrong = "BackupWriteWrong"
)
const (
	Put    = "Put"
	Append = "Append"
)
const (
	Direct  = "Direct"
	Forward = "Forward"
)

type Err string

// Put or Append
type PutAppendArgs struct {
	Key   string
	Value string

	// You'll have to add definitions here.
	Op string
	Id int64
	// Field names must start with capital letters,
	// otherwise RPC will break.
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	Key string

	// You'll have to add definitions here.
	Id     int64
	Source string
}

type GetReply struct {
	Err   Err
	Value string
}

// Your RPC definitions here.
type GetCompleteArgs struct {
	Database map[string]string
	Filter   map[int64]int
}

type GetCompleteReply struct {
	Err
}
