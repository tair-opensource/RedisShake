// Copyright 2016 CodisLabs. All Rights Reserved.
// Licensed under the MIT (MIT-LICENSE.txt) license.

package run

import (
	"bufio"
	"bytes"
	"fmt"
	"io/ioutil"
	"strconv"
	"sync"
	"time"

	"pkg/libs/atomic2"
	"pkg/libs/log"
	"pkg/redis"

	"redis-shake/base"
	"redis-shake/common"
	"redis-shake/configure"
)

type CmdRestore struct {
}

type cmdRestoreStat struct {
	rbytes, ebytes, nentry, ignore int64

	forward, nbypass int64
}

func (cmd *CmdRestore) GetDetailedInfo() interface{} {
	return nil
}

func (cmd *CmdRestore) Main() {
	log.Infof("restore from '%s' to '%s'\n", conf.Options.RdbInput, conf.Options.TargetAddressList)

	type restoreNode struct {
		id    int
		input string
	}
	base.Status = "waitRestore"
	total := utils.GetTotalLink()
	restoreChan := make(chan restoreNode, total)

	for i, rdb := range conf.Options.RdbInput {
		restoreChan <- restoreNode{id: i, input: rdb}
	}

	var wg sync.WaitGroup
	wg.Add(len(conf.Options.RdbInput))
	for i := 0; i < conf.Options.RdbParallel; i++ {
		go func() {
			for {
				node, ok := <-restoreChan
				if !ok {
					break
				}

				var target []string
				if conf.Options.TargetType == conf.RedisTypeCluster {
					target = conf.Options.TargetAddressList
				} else {
					// round-robin pick
					pick := utils.PickTargetRoundRobin(len(conf.Options.TargetAddressList))
					target = []string{conf.Options.TargetAddressList[pick]}
				}

				dr := &dbRestorer{
					id:             node.id,
					input:          node.input,
					target:         target,
					targetPassword: conf.Options.TargetPasswordRaw,
				}
				log.Infof("routine[%v] starts restoring data from %v to %v",
					dr.id, dr.input, dr.target)
				dr.restore()

				wg.Done()
			}
		}()
	}

	wg.Wait()
	close(restoreChan)

	if conf.Options.HttpProfile > 0 {
		//fake status if set http_port. and wait forever
		base.Status = "incr"
		log.Infof("Enabled http stats, set status (incr), and wait forever.")
		select {}
	}
}

/*------------------------------------------------------*/
// one restore link corresponding to one dbRestorer
type dbRestorer struct {
	id             int      // id
	input          string   // input rdb
	target         []string // len >= 1 when target type is cluster, otherwise len == 1
	targetPassword string

	// metric
	rbytes, ebytes, nentry, ignore atomic2.Int64
	forward, nbypass               atomic2.Int64
}

func (dr *dbRestorer) Stat() *cmdRestoreStat {
	return &cmdRestoreStat{
		rbytes: dr.rbytes.Get(),
		ebytes: dr.ebytes.Get(),
		nentry: dr.nentry.Get(),
		ignore: dr.ignore.Get(),

		forward: dr.forward.Get(),
		nbypass: dr.nbypass.Get(),
	}
}

func (dr *dbRestorer) restore() {
	readin, nsize := utils.OpenReadFile(dr.input)
	defer readin.Close()
	base.Status = "restore"

	reader := bufio.NewReaderSize(readin, utils.ReaderBufferSize)

	dr.restoreRDBFile(reader, dr.target, conf.Options.TargetAuthType, conf.Options.TargetPasswordRaw,
		nsize, conf.Options.TargetTLSEnable)

	base.Status = "extra"
	if conf.Options.ExtraInfo && (nsize == 0 || nsize != dr.rbytes.Get()) {
		// inner usage
		dr.restoreCommand(reader, dr.target, conf.Options.TargetAuthType,
			conf.Options.TargetPasswordRaw, conf.Options.TargetTLSEnable)
	}
}


func (dr *dbRestorer) restoreRDBFile(reader *bufio.Reader, target []string, auth_type, passwd string, nsize int64,
		tlsEnable bool) {
	pipe := utils.NewRDBLoader(reader, &dr.rbytes, base.RDBPipeSize)
	wait := make(chan struct{})
	go func() {
		var wg sync.WaitGroup
		wg.Add(conf.Options.Parallel)
		for i := 0; i < conf.Options.Parallel; i++ {
			go func() {
				defer wg.Done()
				c := utils.OpenRedisConn(target, auth_type, passwd, conf.Options.TargetType == conf.RedisTypeCluster,
					tlsEnable)
				defer c.Close()
				var lastdb uint32 = 0
				for e := range pipe {
					if !base.AcceptDB(e.DB) {
						dr.ignore.Incr()
					} else {
						dr.nentry.Incr()
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
		wg.Wait()
		close(wait)
	}()

	for done := false; !done; {
		select {
		case <-wait:
			done = true
		case <-time.After(time.Second):
		}
		stat := dr.Stat()
		var b bytes.Buffer
		if nsize != 0 {
			fmt.Fprintf(&b, "routine[%v] total = %d - %12d [%3d%%]", dr.id, nsize, stat.rbytes, 100*stat.rbytes/nsize)
		} else {
			fmt.Fprintf(&b, "routine[%v] total = %12d", dr.id, stat.rbytes)
		}
		fmt.Fprintf(&b, "  entry=%-12d", stat.nentry)
		if stat.ignore != 0 {
			fmt.Fprintf(&b, "  ignore=%-12d", stat.ignore)
		}
		log.Info(b.String())
	}
	log.Infof("routine[%v] restore: rdb done", dr.id)
}

func (dr *dbRestorer) restoreCommand(reader *bufio.Reader, target []string, auth_type, passwd string, tlsEnable bool) {
	// inner usage. only use on targe
	c := utils.OpenNetConn(target[0], auth_type, passwd, tlsEnable)
	defer c.Close()

	writer := bufio.NewWriterSize(c, utils.WriterBufferSize)
	defer utils.FlushWriter(writer)

	// discard target returning
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
				log.PanicError(err, "routine[%v] parse command arguments failed", dr.id)
			} else if scmd != "ping" {
				if scmd == "select" {
					if len(args) != 1 {
						log.Panicf("routine[%v] select command len(args) = %d", dr.id, len(args))
					}
					s := string(args[0])
					n, err := strconv.Atoi(s)
					if err != nil {
						log.PanicErrorf(err, "routine[%v] parse db = %s failed", dr.id, s)
					}
					bypass = !base.AcceptDB(uint32(n))
				}
				if bypass {
					dr.nbypass.Incr()
					continue
				}
			}
			dr.forward.Incr()
			redis.MustEncode(writer, resp)
			utils.FlushWriter(writer)
		}
	}()

	for lstat := dr.Stat(); ; {
		time.Sleep(time.Second)
		nstat := dr.Stat()
		var b bytes.Buffer
		fmt.Fprintf(&b, "routine[%v] restore: ", dr.id)
		fmt.Fprintf(&b, " +forward=%-6d", nstat.forward-lstat.forward)
		fmt.Fprintf(&b, " +nbypass=%-6d", nstat.nbypass-lstat.nbypass)
		log.Info(b.String())
		lstat = nstat
	}
}
