package utils

import "strings"

// return true means not pass
func FilterCommands(cmd string, luaFilter bool) bool {
	if strings.EqualFold(cmd, "opinfo") {
		return true
	}

	if luaFilter && (strings.EqualFold(cmd, "eval") || strings.EqualFold(cmd, "script")) {
		return true
	}

	return false
}