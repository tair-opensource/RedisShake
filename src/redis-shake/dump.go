// Copyright 2016 CodisLabs. All Rights Reserved.
// Licensed under the MIT (MIT-LICENSE.txt) license.

package run

import (
	"bufio"
	"net"
	"time"
	"fmt"
	"sync"

	"pkg/libs/atomic2"
	"pkg/libs/log"
	"redis-shake/configure"
	"redis-shake/common"
)

type CmdDump struct {
	dumpChan chan node
	wg       sync.WaitGroup
}

type node struct {
	source string
	output string
}

func (cmd *CmdDump) GetDetailedInfo() interface{} {
	return nil
}

func (cmd *CmdDump) Main() {
	cmd.dumpChan = make(chan node, len(conf.Options.SourceAddress))

	for i, source := range conf.Options.SourceAddress {
		nd := node{
			source: source,
			output: fmt.Sprintf("%s.%d", conf.Options.OutputRdb, i),
		}
		cmd.dumpChan <- nd
	}

	var (
		reader *bufio.Reader
		writer *bufio.Writer
		nsize  int64
	)
	cmd.wg.Add(len(conf.Options.SourceAddress))
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
					reader, writer, nsize = cmd.dump(nd.source, nd.output)
					cmd.wg.Done()
				}
			}
		}(i)
	}

	cmd.wg.Wait()

	close(cmd.dumpChan)

	if len(conf.Options.SourceAddress) != 1 || !conf.Options.ExtraInfo {
		return
	}

	// inner usage
	cmd.dumpCommand(reader, writer, nsize)
}

func (cmd *CmdDump) dump(source, output string) (*bufio.Reader, *bufio.Writer, int64) {
	log.Infof("dump from '%s' to '%s'\n", source, output)

	dumpto := utils.OpenWriteFile(output)
	defer dumpto.Close()

	master, nsize := cmd.sendCmd(source, conf.Options.SourceAuthType, conf.Options.SourcePasswordRaw)
	defer master.Close()

	log.Infof("source db[%v] dump rdb file-size[%d]\n", source, nsize)

	reader := bufio.NewReaderSize(master, utils.ReaderBufferSize)
	writer := bufio.NewWriterSize(dumpto, utils.WriterBufferSize)

	cmd.dumpRDBFile(reader, writer, nsize)

	return reader, writer, nsize
}

func (cmd *CmdDump) sendCmd(master, auth_type, passwd string) (net.Conn, int64) {
	c, wait := utils.OpenSyncConn(master, auth_type, passwd)
	var nsize int64
	for nsize == 0 {
		select {
		case nsize = <-wait:
			if nsize == 0 {
				log.Info("+")
			}
		case <-time.After(time.Second):
			log.Info("-")
		}
	}
	return c, nsize
}

func (cmd *CmdDump) dumpRDBFile(reader *bufio.Reader, writer *bufio.Writer, nsize int64) {
	var nread atomic2.Int64
	wait := make(chan struct{})
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

	for done := false; !done; {
		select {
		case <-wait:
			done = true
		case <-time.After(time.Second):
		}
		n := nread.Get()
		p := 100 * n / nsize
		log.Infof("total = %d - %12d [%3d%%]\n", nsize, n, p)
	}
	log.Info("dump: rdb done")
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
