package shardkv

import (
	"testing"
)

func TestSet(t *testing.T)  {
	s := Set{}
	s.Add(1)
	s.Add(5)
	s.Add(2)
	s.Add(12)

	m := make(map[int]string)
	m[1] = "ok"
	log.Info("m:",m)
}
