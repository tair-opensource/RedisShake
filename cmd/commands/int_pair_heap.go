package commands

type IntPairHeap []Pair

type Pair struct {
	Left  int
	Right int
}

func (h IntPairHeap) Len() int { return len(h) }
func (h IntPairHeap) Less(i, j int) bool {
	return h[i].Right < h[j].Right // 小根堆
}
func (h IntPairHeap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
}

func (h *IntPairHeap) Pop() interface{} {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[0 : n-1]
	return x
}

func (h *IntPairHeap) Push(x interface{}) {
	*h = append(*h, x.(Pair))
}
