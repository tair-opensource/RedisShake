package filter

import (
	"fmt"
	"regexp"
	"strings"
)

// keys pattern
type KeysPattern struct {
	regList []*regexp.Regexp
}

// new keys patterns
func NewKeysPattern(patternList01 []string) (ret *KeysPattern, err error) {
	ret = &KeysPattern{
		regList: []*regexp.Regexp{},
	}
	for _, k01 := range patternList01 {
		k01 = strings.TrimSpace(k01)
		if k01 == "" {
			continue
		}
		regItem, err := regexp.Compile(k01)
		if err != nil {
			err = fmt.Errorf("%s regexp.Compile fail,err:%v", regItem, err)
			return nil, err
		}
		ret.regList = append(ret.regList, regItem)
	}
	return ret, nil
}

// match Key
func (f *KeysPattern) MatchKey(k01 string) bool {
	if len(f.regList) == 0 {
		return false
	}
	for _, reg01 := range f.regList {
		regItem := reg01
		if regItem.MatchString(k01) == true {
			return true
		}
	}
	return false
}
