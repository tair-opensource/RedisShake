package reader

import (
	"os"
	"path"
	"path/filepath"

	"RedisShake/internal/aof"
	"RedisShake/internal/entry"
	"RedisShake/internal/log"
)

type AOFReaderOptions struct { // TODO：修改
	Filepath     string `mapstructure:"filepath" default:""`
	AOFTimestamp string
}

type aofReader struct {
	path string
	ch   chan *entry.Entry
}

// TODO:需要实现
func (r *aofReader) Status() interface{} {
	//TODO implement me
	panic("implement me")
}

func (r *aofReader) StatusString() string {
	//TODO implement me
	panic("implement me")
}

func (r *aofReader) StatusConsistent() bool {
	//TODO implement me
	panic("implement me")
}

func NewAOFReader(opts *AOFReaderOptions) Reader {
	log.Infof("NewAOFReader: path=[%s]", opts.Filepath)
	absolutePath, err := filepath.Abs(opts.Filepath)
	if err != nil {
		log.Panicf("NewAOFReader: filepath.Abs error: %s", err.Error())
	}
	log.Infof("NewAOFReader: absolute path=[%s]", absolutePath)
	r := &aofReader{
		path: absolutePath,
		ch:   make(chan *entry.Entry),
	}
	return r
}

func (r *aofReader) StartRead() chan *entry.Entry {
	r.ch = make(chan *entry.Entry, 1024)

	go func() {
		aof.AOFFileInfo = *(aof.NewAOFFileInfo(r.path))

		aof.AOFLoadManifestFromDisk()
		am := aof.AOFFileInfo.GetAOFManifest()

		if am == nil {
			paths := path.Join(aof.AOFFileInfo.GetAOFDirName(), aof.AOFFileInfo.GetAOFFileName())
			log.Infof("start send AOF path=[%s]", r.path)
			fi, err := os.Stat(r.path)
			if err != nil {
				log.Panicf("NewAOFReader: os.Stat error：%s", err.Error())
			}
			log.Infof("the file stat:%v", fi)
			aofLoader := aof.NewLoader(r.path, r.ch)
			_ = aofLoader.LoadSingleAppendOnlyFile(paths, r.ch, true)
			log.Infof("Send AOF finished. path=[%s]", r.path)
			close(r.ch)
		} else {
			log.Infof("start send AOF。path=[%s]", r.path)
			fi, err := os.Stat(r.path)
			if err != nil {
				log.Panicf("NewAOFReader: os.Stat error：%s", err.Error())
			}
			log.Infof("the file stat:%v", fi)
			aofLoader := aof.NewLoader(r.path, r.ch)
			_ = aofLoader.LoadAppendOnlyFile(aof.AOFFileInfo.GetAOFManifest(), r.ch)
			log.Infof("Send AOF finished. path=[%s]", r.path)
			close(r.ch)
		}

	}()

	return r.ch
}
