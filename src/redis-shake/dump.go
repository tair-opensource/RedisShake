// Copyright 2016 CodisLabs. All Rights Reserved.
// Licensed under the MIT (MIT-LICENSE.txt) license.

package run

import (
	"bufio"
	"io"
	"net"
	"os"
	"time"

	"pkg/libs/atomic2"
	"pkg/libs/log"
	"redis-shake/configure"
	"redis-shake/common"
)

type CmdDump struct {
}

func (cmd *CmdDump) GetDetailedInfo() []interface{} {
	return nil
}

func (cmd *CmdDump) Main() {
	from, output := conf.Options.SourceAddress, conf.Options.OutputRdb
	if len(from) == 0 {
		log.Panic("invalid argument: from")
	}
	if len(output) == 0 {
		output = "/dev/stdout"
	}

	log.Infof("dump from '%s' to '%s'\n", from, output)

	var dumpto io.WriteCloser
	if output != "/dev/stdout" {
		dumpto = utils.OpenWriteFile(output)
		defer dumpto.Close()
	} else {
		dumpto = os.Stdout
	}

	master, nsize := cmd.SendCmd(from, conf.Options.SourceAuthType, conf.Options.SourcePasswordRaw)
	defer master.Close()

	log.Infof("rdb file = %d\n", nsize)

	reader := bufio.NewReaderSize(master, utils.ReaderBufferSize)
	writer := bufio.NewWriterSize(dumpto, utils.WriterBufferSize)

	cmd.DumpRDBFile(reader, writer, nsize)

	if !conf.Options.ExtraInfo {
		return
	}

	cmd.DumpCommand(reader, writer, nsize)
}

func (cmd *CmdDump) SendCmd(master, auth_type, passwd string) (net.Conn, int64) {
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

func (cmd *CmdDump) DumpRDBFile(reader *bufio.Reader, writer *bufio.Writer, nsize int64) {
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

func (cmd *CmdDump) DumpCommand(reader *bufio.Reader, writer *bufio.Writer, nsize int64) {
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
