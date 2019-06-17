package run

import "strings"

// hasAtLeastOnePrefix checks whether the key has begins with at least one of prefixes.
func hasAtLeastOnePrefix(key string, prefixes []string) bool {
	for _, prefix := range prefixes {
		if strings.HasPrefix(key, prefix) {
			return true
		}
	}
	return false
}
