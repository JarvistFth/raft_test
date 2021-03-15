package shardmaster

import "time"

func (sm *ShardMaster) getOpIndexCh(index int) chan Op{
	sm.mu.Lock()
	defer sm.mu.Unlock()
	_,found := sm.OpMap[index]
	if !found{
		sm.OpMap[index] = make(chan Op, 1)
	}
	return sm.OpMap[index]
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
