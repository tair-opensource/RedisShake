package filter

import (
	"strings"
	"redis-shake/configure"
	"strconv"
	"redis-shake/common"
)

var (
	innerFilterKeys = map[string]struct{} {
		utils.CheckpointKey: {},
	}
)

// return true means not pass
func FilterCommands(cmd string) bool {
	if strings.EqualFold(cmd, "opinfo") {
		return true
	}

	if conf.Options.FilterLua && (strings.EqualFold(cmd, "eval") || strings.EqualFold(cmd, "script") ||
			strings.EqualFold(cmd, "evalsha")) {
		return true
	}

	return false
}

// return true means not pass
func FilterKey(key string) bool {
	if _, ok := innerFilterKeys[key]; ok {
		return true
	}
	if strings.HasPrefix(key, utils.CheckpointKey) {
		return true
	}

	if len(conf.Options.FilterKeyBlacklist) != 0 {
		if hasAtLeastOnePrefix(key, conf.Options.FilterKeyBlacklist) {
			return true
		}
		return false
	} else if len(conf.Options.FilterKeyWhitelist) != 0 {
		if hasAtLeastOnePrefix(key, conf.Options.FilterKeyWhitelist) {
			return false
		}
		return true
	}
	return false
}

// return true means not pass
func FilterSlot(slot int) bool {
	if len(conf.Options.FilterSlot) == 0 {
		return false
	}

	// the slot in FilterSlot need to be passed
	for _, ele := range conf.Options.FilterSlot {
		slotInt, _ := strconv.Atoi(ele)
		if slot == slotInt {
			return false
		}
	}
	return true
}

// return true means not pass
func FilterDB(db int) bool {
	dbString := strconv.FormatInt(int64(db), 10)
	if len(conf.Options.FilterDBBlacklist) != 0 {
		if matchOne(dbString, conf.Options.FilterDBBlacklist) {
			return true
		}
		return false
	} else if len(conf.Options.FilterDBWhitelist) != 0 {
		if matchOne(dbString, conf.Options.FilterDBWhitelist) {
			return false
		}
		return true
	}
	return false
}

/*
 * judge whether the input command with key should be filter,
 * @return:
 *     [][]byte: the new argv which may be modified after filter.
 *     bool: true means pass
 */
func HandleFilterKeyWithCommand(scmd string, commandArgv [][]byte) ([][]byte, bool) {
	if len(conf.Options.FilterKeyWhitelist) == 0 && len(conf.Options.FilterKeyBlacklist) == 0 {
		// pass if no filter given
		return commandArgv, false
	}

	cmdNode, ok := RedisCommands[scmd]
	if !ok || len(commandArgv) == 0 {
		// pass when command not found or length of argv == 0
		return commandArgv, false
	}

	newArgs, pass := getMatchKeys(cmdNode, commandArgv)
	return newArgs, !pass
}

// hasAtLeastOnePrefix checks whether the key begins with at least one of prefixes.
func hasAtLeastOnePrefix(key string, prefixes []string) bool {
	for _, prefix := range prefixes {
		if strings.HasPrefix(key, prefix) {
			return true
		}
	}
	return false
}

func matchOne(input string, list []string) bool {
	for _, ele := range list {
		if ele == input {
			return true
		}
	}
	return false
}