package filter

import (
	"RedisShake/internal/config"
	"RedisShake/internal/entry"
	"log"
	"slices"
	"strings"
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
		// Check if the key matches any of the allowed patterns
		allow := false
		for _, prefix := range config.Opt.Filter.AllowKeyPrefix {
			if strings.HasPrefix(key, prefix) {
				allow = true
			}
		}
		for _, suffix := range config.Opt.Filter.AllowKeySuffix {
			if strings.HasSuffix(key, suffix) {
				allow = true
			}
		}
		if len(config.Opt.Filter.AllowKeyPrefix) == 0 && len(config.Opt.Filter.AllowKeySuffix) == 0 {
			allow = true
		}
		if !allow {
			keyResults[inx] = false
		}

		// Check if the key matches any of the blocked patterns
		block := false
		for _, prefix := range config.Opt.Filter.BlockKeyPrefix {
			if strings.HasPrefix(key, prefix) {
				block = true
			}
		}
		for _, suffix := range config.Opt.Filter.BlockKeySuffix {
			if strings.HasSuffix(key, suffix) {
				block = true
			}
		}
		if block {
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
