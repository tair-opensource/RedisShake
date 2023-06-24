package aof

type AofFileType string

const (
	aofResp        AofFileType = "AOF_RESP"
	aofRdbPreamble AofFileType = "AOF_RDB_PREAMBLE"
	aofMultiPart   AofFileType = "AOF_MULTI_PART"
)

// check 里面的主函数
func CheckAofMain(aofFilePath string) (checkResult bool, fileType AofFileType, err error) {
	/*
		getAofType := getInputAofFileTye(filePath) // 获取aof文件的类型
			switch getAOFType {
			case AOF_MULTI_PART:
				checkResult, err = checkMultiPartAof(dirpath, filepath, fix)
				return checkResult, aofMultiPart, nil
			case AOF_RESP:
				checkResult, err := checkOldStyleAof(filepath)
				return checkResult, aofResp, nil
				case AOF_RDB_PREAMBLE:
				checkResult, err := checkOldStyleAof(filepath)
				return checkResult, aofRdbPreamble, nil
				}
			return result, err
	*/
	//TODO: mock result
	return true, aofMultiPart, nil
}
