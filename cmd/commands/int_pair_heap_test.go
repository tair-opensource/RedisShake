package commands

import (
	"container/heap"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestHeap(t *testing.T) {
	h := &IntPairHeap{}
	heap.Init(h)
	heap.Push(h,Pair{12, 2})
	heap.Push(h,Pair{11, 1})
	heap.Push(h,Pair{13, 3})
	assert.Equal(t, Pair{11,1},heap.Pop(h))
	assert.Equal(t, Pair{12,2},heap.Pop(h))
	assert.Equal(t, Pair{13,3},heap.Pop(h))
}
