package shardkv

import (
	"strconv"
)

type Set map[interface{}]struct{}

func NewSet() Set {
	ret := make(map[interface{}]struct{})
	return ret
}

func (s Set) Contain(value interface{}) bool {
	_,exist := s[value]
	return exist
}

func (s Set) Add(value interface{}) {
	s[value] = struct{}{}
}

func (s Set) Empty() bool {
	return len(s) == 0
}

func (s Set) Remove(value interface{})  {
	delete(s,value)
}

func (s Set) String() string {
	var ret string

	for k,_ := range s{
		ret += strconv.Itoa(k.(int)) + " "
	}
	return ret
}

type DB map[string]string

type DBWithShard map[int]DB

type DBWithCfgAndShard map[int]DBWithShard

func NewDB() DB {
	return make(map[string]string)
}

func (d DB) Add(key,value string)  {
	d[key] = value
}

func (d DB) Empty() bool{
	return len(d) == 0
}

func (d DB) Remove(key string) {
	delete(d,key)
}

func (d DB) Contain(key string) bool {
	_,exist := d[key]
	return exist
}




