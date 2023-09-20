package reader

import (
	"os"
	"path"
	"path/filepath"

	"RedisShake/internal/entry"
	"RedisShake/internal/log"
	"RedisShake/internal/utils"

	"github.com/dustin/go-humanize"
)

type AOFReaderOptions struct {
	Filepath     string `mapstructure:"aoffilepath" default:""`
	AOFTimestamp int64  `mapstructure:"aoftimestamp" default:"0"`
}

type aofReader struct {
	path string
	ch   chan *entry.Entry

	stat struct {
		AOFName          string `json:"aof_name"`
		AOFStatus        string `json:"aof_status"`
		AOFFilepath      string `json:"aof_file_path"`
		AOFFileSizeBytes int64  `json:"aof_file_size_bytes"`
		AOFFileSizeHuman string `json:"aof_file_size_human"`
		AOFFileSentBytes int64  `json:"aof_file_sent_bytes"`
		AOFFileSentHuman string `json:"aof_file_sent_human"`
		AOFPercent       string `json:"aof_percent"`
		AOFTimestamp     int64  `json:"aof_time_stamp"`
	}
}

func (r *aofReader) Status() interface{} {
	return r.stat
}

func (r *aofReader) StatusString() string {
	return r.stat.AOFStatus
}

func (r *aofReader) StatusConsistent() bool {
	return r.stat.AOFFileSentBytes == r.stat.AOFFileSizeBytes
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
	r.stat.AOFName = "aof_reader"
	r.stat.AOFStatus = "init"
	r.stat.AOFFilepath = absolutePath
	r.stat.AOFFileSizeBytes = int64(utils.GetFileSize(absolutePath))
	r.stat.AOFFileSizeHuman = humanize.Bytes(uint64(r.stat.AOFFileSizeBytes))
	r.stat.AOFTimestamp = opts.AOFTimestamp
	return r
}

func (r *aofReader) StartRead() chan *entry.Entry {
	r.ch = make(chan *entry.Entry, 1024)

	go func() {
		AOFFileInfo = *(NewAOFFileInfo(r.path))
		AOFLoadManifestFromDisk()
		AM := AOFFileInfo.GetAOFManifest()
		if AM == nil {
			paths := path.Join(AOFFileInfo.GetAOFDirName(), AOFFileInfo.GetAOFFileName())
			log.Infof("start send AOF path=[%s]", r.path)
			fi, err := os.Stat(r.path)
			if err != nil {
				log.Panicf("NewAOFReader: os.Stat error：%s", err.Error())
			}
			log.Infof("the file stat:%v", fi)
			aofLoader := NewLoader(r.path, r.ch)
			_ = aofLoader.ParsingSingleAppendOnlyFile(paths, r.ch, true, r.stat.AOFTimestamp)
			log.Infof("Send AOF finished. path=[%s]", r.path)
			close(r.ch)
		} else {
			log.Infof("start send AOF。path=[%s]", r.path)
			fi, err := os.Stat(r.path)
			if err != nil {
				log.Panicf("NewAOFReader: os.Stat error：%s", err.Error())
			}
			log.Infof("the file stat:%v", fi)
			aofLoader := NewLoader(r.path, r.ch)
			ret := aofLoader.LoadAppendOnlyFile(AM, r.ch, r.stat.AOFTimestamp)
			if ret == AOFEmpty {
				log.Panicf("The AOF file is empty.")
			}
			if ret == AOFNotExist {
				log.Panicf("The AOF file does not exist.")
			}
			if ret == AOFFailed {
				log.Panicf("An error occurred while reading the AOF file.")
			}
			log.Infof("Send AOF finished. path=[%s]", r.path)
			close(r.ch)
		}

	}()

	return r.ch
}
