package reader

import (
	"RedisShake/internal/aof"
	"path/filepath"

	"RedisShake/internal/entry"
	"RedisShake/internal/log"
	"RedisShake/internal/utils"

	"github.com/dustin/go-humanize"
)

type AOFReaderOptions struct {
	Filepath     string `mapstructure:"filepath" default:""`
	AOFTimestamp int64  `mapstructure:"timestamp" default:"0"`
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
	//init entry
	r.ch = make(chan *entry.Entry, 1024)

	// start read aof
	go func() {
		aofFileInfo := NewAOFFileInfo(r.path, r.ch)
		// try load manifest file
		aofFileInfo.AOFLoadManifestFromDisk()
		manifestInfo := aofFileInfo.AOFManifest
		if manifestInfo == nil { // load single aof file
			log.Infof("start send single AOF path=[%s]", r.path)
			aofLoader := aof.NewLoader(r.path, r.ch)
			ret := aofLoader.LoadSingleAppendOnlyFile(r.stat.AOFTimestamp)
			if ret == AOFOk || ret == AOFTruncated {
				log.Infof("The AOF File was successfully loaded")
			} else {
				log.Infof("There was an error opening the AOF File.")
			}
			log.Infof("Send single AOF finished. path=[%s]", r.path)
			close(r.ch)
		} else {
			aofLoader := NewAOFFileInfo(r.path, r.ch)
			ret := aofLoader.LoadAppendOnlyFile(manifestInfo, r.stat.AOFTimestamp)
			if ret == AOFOk || ret == AOFTruncated {
				log.Infof("The AOF File was successfully loaded")
			} else {
				log.Infof("There was an error opening the AOF File.")
			}
			log.Infof("Send multi-part AOF finished. path=[%s]", r.path)
			close(r.ch)
		}

	}()

	return r.ch
}