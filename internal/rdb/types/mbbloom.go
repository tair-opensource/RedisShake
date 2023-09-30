package types

import (
	"RedisShake/internal/rdb/structure"
	"io"
	"strconv"
	"unsafe"
)

// BloomObject for MBbloom--
type BloomObject struct {
	encver int
	key    string
	sb     chain
}

type chain struct {
	filters  []link
	size     uint64
	nfilters uint64
	options  uint64
	growth   uint64
}

type link struct {
	inner bloom
	size  uint64
}

type bloom struct {
	hashes  uint64
	n2      uint64
	entries uint64
	err     float64
	bpe     float64
	bf      string
	bits    uint64
}

type dumpedChainHeader struct {
	size     uint64
	nfilters uint32
	options  uint32
}

type dumpedChainLink struct {
	bytes   uint64
	bits    uint64
	size    uint64
	err     float64
	bpe     float64
	hashes  uint32
	entries uint32
	n2      uint8
}

const (
	BF_MIN_OPTIONS_ENC = 2
	BF_MIN_GROWTH_ENC  = 4
)

const (
	DUMPED_CHAIN_HEADER_SIZE = 16
	DUMPED_CHAIN_LINK_SIZE   = 49
)

const MAX_SCANDUMP_SIZE = 10485760 // 10MB

func (o *BloomObject) LoadFromBuffer(rd io.Reader, key string, typeByte byte) {
	o.key = key
	var sb chain
	sb.size = readUnsigned(rd)
	sb.nfilters = readUnsigned(rd)
	if o.encver >= BF_MIN_OPTIONS_ENC {
		sb.options = readUnsigned(rd)
	}
	if o.encver >= BF_MIN_GROWTH_ENC {
		sb.growth = readUnsigned(rd)
	} else {
		sb.growth = 2
	}
	for i := uint64(0); i < sb.nfilters; i++ {
		var lb link
		bm := &lb.inner
		bm.entries = readUnsigned(rd)
		bm.err = readDouble(rd)
		bm.hashes = readUnsigned(rd)
		bm.bpe = readDouble(rd)
		if o.encver == 0 {
			bm.bits = uint64(float64(bm.entries) * bm.bpe)
		} else {
			bm.bits = readUnsigned(rd)
			bm.n2 = readUnsigned(rd)
		}
		bm.bf = structure.ReadModuleString(rd)
		lb.size = readUnsigned(rd)
		sb.filters = append(sb.filters, lb)
	}
	o.sb = sb
	structure.ReadModuleEof(rd)
	return
}

func readUnsigned(rd io.Reader) uint64 {
	v := structure.ReadModuleUnsigned(rd)
	u, err := strconv.ParseUint(v, 10, 64)
	if err != nil {
		panic(err)
	}
	return u
}

func readDouble(rd io.Reader) float64 {
	v := structure.ReadModuleDouble(rd)
	f, err := strconv.ParseFloat(v, 64)
	if err != nil {
		panic(err)
	}
	return f
}

func (o *BloomObject) Rewrite() []RedisCmd {
	var cs []RedisCmd
	h := getEncodedHeader(&o.sb)
	cmd := RedisCmd{"BF.LOADCHUNK", o.key, "0", dumpBytes(h)}
	cs = append(cs, cmd)
	curIter := uint64(1)
	for {
		c := getEncodedChunk(&o.sb, &curIter, MAX_SCANDUMP_SIZE)
		if c == "" {
			break
		}
		cmd := RedisCmd{"BF.LOADCHUNK", o.key, strconv.FormatUint(curIter, 10), dumpBytes(c)}
		cs = append(cs, cmd)
	}
	return cs
}

func getEncodedHeader(sb *chain) string {
	h := make([]byte, DUMPED_CHAIN_LINK_SIZE*sb.nfilters+DUMPED_CHAIN_HEADER_SIZE)
	ph := (*dumpedChainHeader)(unsafe.Pointer(&h[0]))
	ph.size = sb.size
	ph.nfilters = uint32(sb.nfilters)
	ph.options = uint32(sb.options)
	for i := uint64(0); i < sb.nfilters; i++ {
		pl := (*dumpedChainLink)(unsafe.Add(unsafe.Pointer(&h[0]), DUMPED_CHAIN_HEADER_SIZE+DUMPED_CHAIN_LINK_SIZE*i))
		sl := sb.filters[i]
		pl.bytes = uint64(len(sl.inner.bf))
		pl.bits = sl.inner.bits
		pl.size = sl.size
		pl.err = sl.inner.err
		pl.hashes = uint32(sl.inner.hashes)
		pl.bpe = sl.inner.bpe
		pl.entries = uint32(sl.inner.entries)
		pl.n2 = uint8(sl.inner.n2)
	}
	return *(*string)(unsafe.Pointer(&h))
}

func getEncodedChunk(sb *chain, curIter *uint64, maxChunkSize uint64) string {
	pl, off := getLinkPos(sb, *curIter)
	if pl == nil {
		*curIter = 0
		return ""
	}
	l := maxChunkSize
	lr := uint64(len(pl.inner.bf)) - off
	if lr < l {
		l = lr
	}
	*curIter += l
	return pl.inner.bf[off : off+l]
}

func getLinkPos(sb *chain, curIter uint64) (pl *link, offset uint64) {
	curIter--
	var seekPos uint64
	for i := uint64(0); i < sb.nfilters; i++ {
		if seekPos+uint64(len(sb.filters[i].inner.bf)) > curIter {
			pl = &sb.filters[i]
			break
		} else {
			seekPos += uint64(len(sb.filters[i].inner.bf))
		}
	}
	if pl == nil {
		return
	}
	offset = curIter - seekPos
	return
}

func dumpBytes(s string) string {
	hex := "0123456789abcdef"
	d := make([]byte, 0, 4*len(s))
	for i := 0; i < len(s); i++ {
		b := s[i]
		if isPrint(b) {
			d = append(d, b)
			continue
		}
		d = append(d, '\'', 'x', hex[(b>>4)&0xf], hex[b&0xf])
	}
	return *(*string)(unsafe.Pointer(&d))
}

func isPrint(b byte) bool {
	return 0x20 <= b && b <= 0x7E && b != 0x5C
}
