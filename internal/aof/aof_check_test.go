package aof

import (
	"testing"
)

func TestCheckAofMain(t *testing.T) {

	aofFilePath := "D:/BaiduNetdiskDownload/sa/appendonly.aof.manifest"

	checkResult, fileType, err := CheckAofMain(aofFilePath)

	if err != nil {
		t.Errorf("Unexpected error: %v", err)
	}

	expectedCheckResult := true
	if checkResult != expectedCheckResult {
		t.Errorf("Unexpected check result. Got: %v, Expected: %v", checkResult, expectedCheckResult)
	}

	expectedFileType := "AOF_MULTI_PART"
	if string(fileType) != expectedFileType {
		t.Errorf("Unexpected file type. Got: %v, Expected: %v", fileType, expectedFileType)
	}

}
