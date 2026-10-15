package filter

import (
	"fmt"
	"strconv"
	"strings"
	"sync"

	"RedisShake/internal/config"
	"RedisShake/internal/entry"
	"RedisShake/internal/log"
)

const clusterSlotCount = 16384

var (
	slotFilterOnce sync.Once
	allowSlots     []bool
	blockSlots     []bool
)

func slotFilter(e *entry.Entry) bool {
	loadSlotFilters()
	if allowSlots == nil && blockSlots == nil {
		return true
	}
	passed, filtered := 0, 0
	for _, slot := range e.Slots {
		if slotAllowed(slot) {
			passed++
		} else {
			filtered++
		}
	}
	if passed > 0 && filtered > 0 {
		log.Infof("Error: Inconsistent slot filter results for entry with %d keys", len(e.Keys))
		log.Infof("Keys: %v, slots: %v", e.Keys, e.Slots)
	}
	return filtered == 0
}

func loadSlotFilters() {
	slotFilterOnce.Do(func() {
		var err error
		allowSlots, err = parseSlotRanges(config.Opt.Filter.AllowSlot)
		if err != nil {
			log.Panicf("filter.allow_slot: %v", err)
		}
		blockSlots, err = parseSlotRanges(config.Opt.Filter.BlockSlot)
		if err != nil {
			log.Panicf("filter.block_slot: %v", err)
		}
	})
}

func parseSlotRanges(ranges []string) ([]bool, error) {
	if len(ranges) == 0 {
		return nil, nil
	}
	slots := make([]bool, clusterSlotCount)
	for _, r := range ranges {
		startStr, endStr, isRange := strings.Cut(strings.TrimSpace(r), "-")
		start, err := strconv.Atoi(strings.TrimSpace(startStr))
		if err != nil {
			return nil, fmt.Errorf("invalid slot range %q: %w", r, err)
		}
		end := start
		if isRange {
			end, err = strconv.Atoi(strings.TrimSpace(endStr))
			if err != nil {
				return nil, fmt.Errorf("invalid slot range %q: %w", r, err)
			}
		}
		if start < 0 || end >= clusterSlotCount || start > end {
			return nil, fmt.Errorf("invalid slot range %q: slots must be within 0-%d and start <= end", r, clusterSlotCount-1)
		}
		for slot := start; slot <= end; slot++ {
			slots[slot] = true
		}
	}
	return slots, nil
}

func slotAllowed(slot int) bool {
	if slot < 0 || slot >= clusterSlotCount {
		return false
	}
	if allowSlots != nil && !allowSlots[slot] {
		return false
	}
	if blockSlots != nil && blockSlots[slot] {
		return false
	}
	return true
}
