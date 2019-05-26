// Copyright 2016 CodisLabs. All Rights Reserved.
// Licensed under the MIT (MIT-LICENSE.txt) license.

package run

import (
	"bufio"
	"fmt"
	"net"
	"sync"
	"time"

	"pkg/libs/atomic2"
	"pkg/libs/log"
	"redis-shake/common"
	"redis-shake/configure"
)

type CmdDump struct {
	dumpChan chan node
}

type node struct {
	id     int
	source string
	output string
}

func (cmd *CmdDump) GetDetailedInfo() interface{} {
	return nil
}

func (cmd *CmdDump) Main() {
	cmd.dumpChan = make(chan node, len(conf.Options.SourceAddressList))

	for i, source := range conf.Options.SourceAddressList {
		nd := node{
			id:     i,
			source: source,
			output: fmt.Sprintf("%s.%d", conf.Options.RdbOutput, i),
		}
		cmd.dumpChan <- nd
	}

	var (
		reader *bufio.Reader
		writer *bufio.Writer
		nsize  int64
		wg     sync.WaitGroup
	)
	wg.Add(len(conf.Options.SourceAddressList))
	for i := 0; i < int(conf.Options.SourceParallel); i++ {
		go func(idx int) {
			log.Infof("start routine[%v]", idx)
			for {
				select {
				case nd, ok := <-cmd.dumpChan:
					if !ok {
						log.Infof("close routine[%v]", idx)
						return
					}

					dd := &dbDumper{
						id:             nd.id,
						source:         nd.source,
						sourcePassword: conf.Options.SourcePasswordRaw,
						output:         nd.output,
					}
					reader, writer, nsize = dd.dump()
					wg.Done()
				}
			}
		}(i)
	}

	wg.Wait()

	// all dump finish
	close(cmd.dumpChan)

	if len(conf.Options.SourceAddressList) != 1 || !conf.Options.ExtraInfo {
		return
	}

	// inner usage
	cmd.dumpCommand(reader, writer, nsize)
}

func (cmd *CmdDump) dumpCommand(reader *bufio.Reader, writer *bufio.Writer, nsize int64) {
	var nread atomic2.Int64
	go func() {
		p := make([]byte, utils.ReaderBufferSize)
		for {
			ncopy := int64(utils.Iocopy(reader, writer, p, len(p)))
			nread.Add(ncopy)
			utils.FlushWriter(writer)
		}
	}()

	for {
		time.Sleep(time.Second)
		log.Infof("dump: total = %d\n", nsize+nread.Get())
	}
}

/*------------------------------------------------------*/
// one dump link corresponding to one dbDumper
type dbDumper struct {
	id             int    // id
	source         string // source address
	sourcePassword string
	output         string // output
}

func (dd *dbDumper) dump() (*bufio.Reader, *bufio.Writer, int64) {
	log.Infof("routine[%v] dump from '%s' to '%s'\n", dd.id, dd.source, dd.output)

	dumpto := utils.OpenWriteFile(dd.output)
	defer dumpto.Close()

	// send command and get the returned channel
	master, nsize := dd.sendCmd(dd.source, conf.Options.SourceAuthType, dd.sourcePassword, conf.Options.SourceTLSEnable)
	defer master.Close()

	log.Infof("routine[%v] source db[%v] dump rdb file-size[%d]\n", dd.id, dd.source, nsize)

	reader := bufio.NewReaderSize(master, utils.ReaderBufferSize)
	writer := bufio.NewWriterSize(dumpto, utils.WriterBufferSize)

	dd.dumpRDBFile(reader, writer, nsize)

	return reader, writer, nsize
}

func (dd *dbDumper) sendCmd(master, auth_type, passwd string, tlsEnable bool) (net.Conn, int64) {
	c, wait := utils.OpenSyncConn(master, auth_type, passwd, tlsEnable)
	var nsize int64

	// wait rdb dump finish
	for nsize == 0 {
		select {
		case nsize = <-wait:
			if nsize == 0 {
				log.Infof("routine[%v] +", dd.id)
			}
		case <-time.After(time.Second):
			log.Infof("routine[%v] -", dd.id)
		}
	}
	return c, nsize
}

func (dd *dbDumper) dumpRDBFile(reader *bufio.Reader, writer *bufio.Writer, nsize int64) {
	var nread atomic2.Int64
	wait := make(chan struct{})

	// read from reader and write into writer int stream way
	go func() {
		defer close(wait)
		p := make([]byte, utils.WriterBufferSize)
		for nsize != nread.Get() {
			nstep := int(nsize - nread.Get())
			ncopy := int64(utils.Iocopy(reader, writer, p, nstep))
			nread.Add(ncopy)
			utils.FlushWriter(writer)
		}
	}()

	// print stat
	for done := false; !done; {
		select {
		case <-wait:
			done = true
		case <-time.After(time.Second):
		}
		n := nread.Get()
		p := 100 * n / nsize
		log.Infof("routine[%v] total = %d - %12d [%3d%%]\n", dd.id, nsize, n, p)
	}
	log.Infof("routine[%v] dump: rdb done", dd.id)
}
