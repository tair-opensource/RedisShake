package filter

import (
	"RedisShake/internal/config"
	"RedisShake/internal/entry"
	"log"
	"slices"
	"strings"
	"sync"
)

// Filter returns:
// - true if the entry should be processed
// - false if it should be filtered out
func Filter(e *entry.Entry) bool {
	keyResults := make([]bool, len(e.Keys))
	for i := range keyResults {
		keyResults[i] = true
	}

	for inx, key := range e.Keys {
		// Check if the key matches any of the blocked patterns
		if blockKeyFilter(key) {
			keyResults[inx] = false
			continue
		}
		if !allowKeyFilter(key) {
			keyResults[inx] = false
		}
	}

	allTrue := true
	allFalse := true
	var passedKeys, filteredKeys []string
	for i, result := range keyResults {
		if result {
			allFalse = false
			passedKeys = append(passedKeys, e.Keys[i])
		} else {
			allTrue = false
			filteredKeys = append(filteredKeys, e.Keys[i])
		}
	}
	if allTrue {
		// All keys are allowed, continue checking
	} else if allFalse {
		return false
	} else {
		// If we reach here, it means some keys are true and some are false
		log.Printf("Error: Inconsistent filter results for entry with %d keys", len(e.Keys))
		log.Printf("Passed keys: %v", passedKeys)
		log.Printf("Filtered keys: %v", filteredKeys)
		return false
	}

	// Check if the database matches any of the allowed databases
	if len(config.Opt.Filter.AllowDB) > 0 {
		if !slices.Contains(config.Opt.Filter.AllowDB, e.DbId) {
			return false
		}
	}
	// Check if the database matches any of the blocked databases
	if len(config.Opt.Filter.BlockDB) > 0 {
		if slices.Contains(config.Opt.Filter.BlockDB, e.DbId) {
			return false
		}
	}

	// Check if the command matches any of the allowed commands
	if len(config.Opt.Filter.AllowCommand) > 0 {
		if !slices.Contains(config.Opt.Filter.AllowCommand, e.CmdName) {
			return false
		}
	}
	// Check if the command matches any of the blocked commands
	if len(config.Opt.Filter.BlockCommand) > 0 {
		if slices.Contains(config.Opt.Filter.BlockCommand, e.CmdName) {
			return false
		}
	}

	// Check if the command group matches any of the allowed command groups
	if len(config.Opt.Filter.AllowCommandGroup) > 0 {
		if !slices.Contains(config.Opt.Filter.AllowCommandGroup, e.Group) {
			return false
		}
	}
	// Check if the command group matches any of the blocked command groups
	if len(config.Opt.Filter.BlockCommandGroup) > 0 {
		if slices.Contains(config.Opt.Filter.BlockCommandGroup, e.Group) {
			return false
		}
	}

	return true
}

// blockKeyFilter is block key? default false
func blockKeyFilter(key string) bool {
	if len(config.Opt.Filter.BlockKeyRegex) == 0 && len(config.Opt.Filter.BlockKeyPrefix) == 0 &&
		len(config.Opt.Filter.BlockKeySuffix) == 0 {
		return false
	}
	if blockKeyMatch(config.Opt.Filter.BlockKeyRegex, key) {
		return true
	}
	for _, prefix := range config.Opt.Filter.BlockKeyPrefix {
		if strings.HasPrefix(key, prefix) {
			return true
		}
	}
	for _, suffix := range config.Opt.Filter.BlockKeySuffix {
		if strings.HasSuffix(key, suffix) {
			return true
		}
	}

	return false
}

// allowKeyFilter is allow key? default true
func allowKeyFilter(key string) bool {
	// if all allow filter is empty. default is true
	if len(config.Opt.Filter.AllowKeyRegex) == 0 && len(config.Opt.Filter.AllowKeyPrefix) == 0 &&
		len(config.Opt.Filter.AllowKeySuffix) == 0 {
		return true
	}
	// If the RE matches, there is no need to iterate over the others
	if allowKeyMatch(config.Opt.Filter.AllowKeyRegex, key) {
		return true
	}

	for _, prefix := range config.Opt.Filter.AllowKeyPrefix {
		if strings.HasPrefix(key, prefix) {
			return true
		}
	}
	for _, suffix := range config.Opt.Filter.AllowKeySuffix {
		if strings.HasSuffix(key, suffix) {
			return true
		}
	}
	return false
}

var (
	blockListOnce        sync.Once
	blockListKeyPatterns *KeysPattern
)

// blockKeyMatch
func blockKeyMatch(regList []string, key string) bool {
	blockListOnce.Do(func() {
		var err error
		blockListKeyPatterns, err = NewKeysPattern(regList)
		if err != nil {
			log.Panicf("%s,conf.Options.BlockKeyRegex[%+v]", err, regList)
		}
	})

	return blockListKeyPatterns.MatchKey(key)
}

var (
	allowOnce            sync.Once
	allowListKeyPatterns *KeysPattern
)

// allowKeyMatch
func allowKeyMatch(regList []string, key string) bool {
	if len(regList) == 1 {
		first := regList[0]
		if first == "*" || first == ".*" || first == "^.*$" {
			return true
		}
	}
	allowOnce.Do(func() {
		var err error
		allowListKeyPatterns, err = NewKeysPattern(regList)
		if err != nil {
			log.Panicf("%s,conf.Options.AllowKeyRegex[%+v]", err, regList)
		}
	})

	return allowListKeyPatterns.MatchKey(key)
}
