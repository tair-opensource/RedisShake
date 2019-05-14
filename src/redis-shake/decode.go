// Copyright 2016 CodisLabs. All Rights Reserved.
// Licensed under the MIT (MIT-LICENSE.txt) license.

package run

import (
	"bufio"
	"bytes"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"time"

	"pkg/libs/atomic2"
	"pkg/libs/log"
	"pkg/rdb"
	"redis-shake/common"
	"redis-shake/configure"
	"redis-shake/base"
)

type CmdDecode struct {
	rbytes, wbytes, nentry atomic2.Int64
}

type cmdDecodeStat struct {
	rbytes, wbytes, nentry int64
}

func (cmd *CmdDecode) Stat() *cmdDecodeStat {
	return &cmdDecodeStat{
		rbytes: cmd.rbytes.Get(),
		wbytes: cmd.wbytes.Get(),
		nentry: cmd.nentry.Get(),
	}
}

func (cmd *CmdDecode) GetDetailedInfo() interface{} {
	return nil
}

func (cmd *CmdDecode) Main() {
	log.Infof("decode from '%s' to '%s'\n", conf.Options.RdbInput, conf.Options.RdbOutput)

	for i, input := range conf.Options.RdbInput {
		// decode one by one. By now, we don't support decoding concurrence.
		output := fmt.Sprintf("%s.%d", conf.Options.RdbOutput, i)
		cmd.decode(input, output)
	}

	log.Info("decode: done")
}

func (cmd *CmdDecode) decode(input, output string) {
	readin, nsize := utils.OpenReadFile(input)
	defer readin.Close()

	saveto := utils.OpenWriteFile(output)
	defer saveto.Close()

	reader := bufio.NewReaderSize(readin, utils.ReaderBufferSize)
	writer := bufio.NewWriterSize(saveto, utils.WriterBufferSize)

	ipipe := utils.NewRDBLoader(reader, &cmd.rbytes, base.RDBPipeSize)
	opipe := make(chan string, cap(ipipe))

	go func() {
		defer close(opipe)
		group := make(chan int, conf.Options.Parallel)
		for i := 0; i < cap(group); i++ {
			go func() {
				defer func() {
					group <- 0
				}()
				cmd.decoderMain(ipipe, opipe)
			}()
		}
		for i := 0; i < cap(group); i++ {
			<-group
		}
	}()

	wait := make(chan struct{})
	go func() {
		defer close(wait)
		for s := range opipe {
			cmd.wbytes.Add(int64(len(s)))
			if _, err := writer.WriteString(s); err != nil {
				log.PanicError(err, "write string failed")
			}
			utils.FlushWriter(writer)
		}
	}()

	for done := false; !done; {
		select {
		case <-wait:
			done = true
		case <-time.After(time.Second):
		}
		stat := cmd.Stat()
		var b bytes.Buffer
		fmt.Fprintf(&b, "decode: ")
		if nsize != 0 {
			fmt.Fprintf(&b, "total = %d - %12d [%3d%%]", nsize, stat.rbytes, 100*stat.rbytes/nsize)
		} else {
			fmt.Fprintf(&b, "total = %12d", stat.rbytes)
		}
		fmt.Fprintf(&b, "  write=%-12d", stat.wbytes)
		fmt.Fprintf(&b, "  entry=%-12d", stat.nentry)
		log.Info(b.String())
	}
}

func (cmd *CmdDecode) decoderMain(ipipe <-chan *rdb.BinEntry, opipe chan<- string) {
	toText := func(p []byte) string {
		var b bytes.Buffer
		for _, c := range p {
			switch {
			case c >= '#' && c <= '~':
				b.WriteByte(c)
			default:
				b.WriteByte('.')
			}
		}
		return b.String()
	}
	toBase64 := func(p []byte) string {
		return base64.StdEncoding.EncodeToString(p)
	}
	toJson := func(o interface{}) string {
		b, err := json.Marshal(o)
		if err != nil {
			log.PanicError(err, "encode to json failed")
		}
		return string(b)
	}
	for e := range ipipe {
		o, err := rdb.DecodeDump(e.Value)
		if err != nil {
			log.PanicError(err, "decode failed")
		}
		var b bytes.Buffer
		switch obj := o.(type) {
		default:
			log.Panicf("unknown object %v", o)
		case rdb.String:
			o := &struct {
				DB       uint32 `json:"db"`
				Type     string `json:"type"`
				ExpireAt uint64 `json:"expireat"`
				Key      string `json:"key"`
				Key64    string `json:"key64"`
				Value64  string `json:"value64"`
			}{
				e.DB, "string", e.ExpireAt, toText(e.Key), toBase64(e.Key),
				toBase64(obj),
			}
			fmt.Fprintf(&b, "%s\n", toJson(o))
		case rdb.List:
			for i, ele := range obj {
				o := &struct {
					DB       uint32 `json:"db"`
					Type     string `json:"type"`
					ExpireAt uint64 `json:"expireat"`
					Key      string `json:"key"`
					Key64    string `json:"key64"`
					Index    int    `json:"index"`
					Value64  string `json:"value64"`
				}{
					e.DB, "list", e.ExpireAt, toText(e.Key), toBase64(e.Key),
					i, toBase64(ele),
				}
				fmt.Fprintf(&b, "%s\n", toJson(o))
			}
		case rdb.Hash:
			for _, ele := range obj {
				o := &struct {
					DB       uint32 `json:"db"`
					Type     string `json:"type"`
					ExpireAt uint64 `json:"expireat"`
					Key      string `json:"key"`
					Key64    string `json:"key64"`
					Field    string `json:"field"`
					Field64  string `json:"field64"`
					Value64  string `json:"value64"`
				}{
					e.DB, "hash", e.ExpireAt, toText(e.Key), toBase64(e.Key),
					toText(ele.Field), toBase64(ele.Field), toBase64(ele.Value),
				}
				fmt.Fprintf(&b, "%s\n", toJson(o))
			}
		case rdb.Set:
			for _, mem := range obj {
				o := &struct {
					DB       uint32 `json:"db"`
					Type     string `json:"type"`
					ExpireAt uint64 `json:"expireat"`
					Key      string `json:"key"`
					Key64    string `json:"key64"`
					Member   string `json:"member"`
					Member64 string `json:"member64"`
				}{
					e.DB, "set", e.ExpireAt, toText(e.Key), toBase64(e.Key),
					toText(mem), toBase64(mem),
				}
				fmt.Fprintf(&b, "%s\n", toJson(o))
			}
		case rdb.ZSet:
			for _, ele := range obj {
				o := &struct {
					DB       uint32  `json:"db"`
					Type     string  `json:"type"`
					ExpireAt uint64  `json:"expireat"`
					Key      string  `json:"key"`
					Key64    string  `json:"key64"`
					Member   string  `json:"member"`
					Member64 string  `json:"member64"`
					Score    float64 `json:"score"`
				}{
					e.DB, "zset", e.ExpireAt, toText(e.Key), toBase64(e.Key),
					toText(ele.Member), toBase64(ele.Member), ele.Score,
				}
				fmt.Fprintf(&b, "%s\n", toJson(o))
			}
		}
		cmd.nentry.Incr()
		opipe <- b.String()
	}
}
