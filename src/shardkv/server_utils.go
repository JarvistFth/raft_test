package shardkv

import (
	"time"
)

func (kv *ShardKV) getOpIndexCh(index int, createIfNotExist bool) chan Op{
	kv.mu.Lock()
	defer kv.mu.Unlock()
	_,found := kv.OpMap[index]
	if !found{
		if createIfNotExist {
			kv.OpMap[index] = make(chan Op, 1)
		}else{
			return nil
		}
	}
	return kv.OpMap[index]
}

func sendCh(ch chan Op, op Op) {
	select {
	case <-ch:
	default:
	}
	ch <- op
}

func getRaftOp(ch chan Op) Op{
	var ret Op
	select {
	case raftOp := <-ch:
		ret = raftOp
		return ret

	case <- time.After(time.Duration(600) * time.Millisecond):
		return ret
	}
}

func MaxInt64(x,y int64) int64 {
	if x > y{
		return x
	}else{
		return y
	}
}