// Copyright 2016 CodisLabs. All Rights Reserved.
// Licensed under the MIT (MIT-LICENSE.txt) license.

package run

import (
	"bufio"
	"bytes"
	"fmt"
	"io/ioutil"
	"time"

	"pkg/libs/atomic2"
	"pkg/libs/log"
	"pkg/redis"
	"redis-shake/configure"
	"redis-shake/common"
	"strconv"
	"redis-shake/base"
)

type CmdRestore struct {
	rbytes, ebytes, nentry, ignore atomic2.Int64

	forward, nbypass atomic2.Int64
}

type cmdRestoreStat struct {
	rbytes, ebytes, nentry, ignore int64

	forward, nbypass int64
}

func (cmd *CmdRestore) Stat() *cmdRestoreStat {
	return &cmdRestoreStat{
		rbytes: cmd.rbytes.Get(),
		ebytes: cmd.ebytes.Get(),
		nentry: cmd.nentry.Get(),
		ignore: cmd.ignore.Get(),

		forward: cmd.forward.Get(),
		nbypass: cmd.nbypass.Get(),
	}
}

func (cmd *CmdRestore) GetDetailedInfo() interface{} {
	return nil
}

func (cmd *CmdRestore) Main() {
	log.Infof("restore from '%s' to '%s'\n",  conf.Options.InputRdb, conf.Options.TargetAddress)

	base.Status = "waitRestore"
	for _, input := range conf.Options.InputRdb {
		// restore one by one
		cmd.restore(input)
	}

	if conf.Options.HttpProfile > 0 {
		//fake status if set http_port. and wait forever
		base.Status = "incr"
		log.Infof("Enabled http stats, set status (incr), and wait forever.")
		select{}
	}
}

func (cmd *CmdRestore) restore(input string) {
	readin, nsize := utils.OpenReadFile(input)
	defer readin.Close()
	base.Status = "restore"

	reader := bufio.NewReaderSize(readin, utils.ReaderBufferSize)

	cmd.restoreRDBFile(reader, conf.Options.TargetAddress, conf.Options.TargetAuthType, conf.Options.TargetPasswordRaw,
		nsize)

	base.Status = "extra"
	if conf.Options.ExtraInfo && (nsize == 0 || nsize != cmd.rbytes.Get()) {
		cmd.restoreCommand(reader, conf.Options.TargetAddress, conf.Options.TargetAuthType,
			conf.Options.TargetPasswordRaw)
	}
}

func (cmd *CmdRestore) restoreRDBFile(reader *bufio.Reader, target, auth_type, passwd string, nsize int64) {
	pipe := utils.NewRDBLoader(reader, &cmd.rbytes, base.RDBPipeSize)
	wait := make(chan struct{})
	go func() {
		defer close(wait)
		group := make(chan int, conf.Options.Parallel)
		for i := 0; i < cap(group); i++ {
			go func() {
				defer func() {
					group <- 0
				}()
				c := utils.OpenRedisConn(target, auth_type, passwd)
				defer c.Close()
				var lastdb uint32 = 0
				for e := range pipe {
					if !base.AcceptDB(e.DB) {
						cmd.ignore.Incr()
					} else {
						cmd.nentry.Incr()
						if conf.Options.TargetDB != -1 {
							if conf.Options.TargetDB != int(lastdb) {
								lastdb = uint32(conf.Options.TargetDB)
								utils.SelectDB(c, lastdb)
							}
						} else {
							if e.DB != lastdb {
								lastdb = e.DB
								utils.SelectDB(c, lastdb)
							}
						}
						utils.RestoreRdbEntry(c, e)
					}
				}
			}()
		}
		for i := 0; i < cap(group); i++ {
			<-group
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
		if nsize != 0 {
			fmt.Fprintf(&b, "total = %d - %12d [%3d%%]", nsize, stat.rbytes, 100*stat.rbytes/nsize)
		} else {
			fmt.Fprintf(&b, "total = %12d", stat.rbytes)
		}
		fmt.Fprintf(&b, "  entry=%-12d", stat.nentry)
		if stat.ignore != 0 {
			fmt.Fprintf(&b, "  ignore=%-12d", stat.ignore)
		}
		log.Info(b.String())
	}
	log.Info("restore: rdb done")
}

func (cmd *CmdRestore) restoreCommand(reader *bufio.Reader, target, auth_type, passwd string) {
	c := utils.OpenNetConn(target, auth_type, passwd)
	defer c.Close()

	writer := bufio.NewWriterSize(c, utils.WriterBufferSize)
	defer utils.FlushWriter(writer)

	go func() {
		p := make([]byte, utils.ReaderBufferSize)
		for {
			utils.Iocopy(c, ioutil.Discard, p, len(p))
		}
	}()

	go func() {
		var bypass bool = false
		for {
			resp := redis.MustDecode(reader)
			if scmd, args, err := redis.ParseArgs(resp); err != nil {
				log.PanicError(err, "parse command arguments failed")
			} else if scmd != "ping" {
				if scmd == "select" {
					if len(args) != 1 {
						log.Panicf("select command len(args) = %d", len(args))
					}
					s := string(args[0])
					n, err := strconv.Atoi(s)
					if err != nil {
						log.PanicErrorf(err, "parse db = %s failed", s)
					}
					bypass = !base.AcceptDB(uint32(n))
				}
				if bypass {
					cmd.nbypass.Incr()
					continue
				}
			}
			cmd.forward.Incr()
			redis.MustEncode(writer, resp)
			utils.FlushWriter(writer)
		}
	}()

	for lstat := cmd.Stat(); ; {
		time.Sleep(time.Second)
		nstat := cmd.Stat()
		var b bytes.Buffer
		fmt.Fprintf(&b, "restore: ")
		fmt.Fprintf(&b, " +forward=%-6d", nstat.forward-lstat.forward)
		fmt.Fprintf(&b, " +nbypass=%-6d", nstat.nbypass-lstat.nbypass)
		log.Info(b.String())
		lstat = nstat
	}
}
