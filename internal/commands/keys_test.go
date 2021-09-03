package commands

import (
	"testing"
)

func testEq(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func TestCalcKeys(t *testing.T) {
	// SET
	cmd, group, keys := CalcKeys([]string{"SET", "key", "value"})
	if cmd != "SET" || group != "STRING" || !testEq(keys, []string{"key"}) {
		t.Errorf("CalcKeys(SET key value) failed. cmd=%s, group=%s, keys=%v", cmd, group, keys)
	}

	// MSET
	cmd, group, keys = CalcKeys([]string{"MSET", "key1", "value1", "key2", "value2"})
	if cmd != "MSET" || group != "STRING" || !testEq(keys, []string{"key1", "key2"}) {
		t.Errorf("CalcKeys(MSET key1 value1 key2 value2) failed. cmd=%s, group=%s, keys=%v", cmd, group, keys)
	}

	// XADD
	cmd, group, keys = CalcKeys([]string{"XADD", "key", "*", "field1", "value1", "field2", "value2"})
	if cmd != "XADD" || group != "STREAM" || !testEq(keys, []string{"key"}) {
		t.Errorf("CalcKeys(XADD key * field1 value1 field2 value2) failed. cmd=%s, group=%s, keys=%v", cmd, group, keys)
	}

	// ZUNIONSTORE
	cmd, group, keys = CalcKeys([]string{"ZUNIONSTORE", "key", "2", "key1", "key2"})
	if cmd != "ZUNIONSTORE" || group != "SORTED_SET" || !testEq(keys, []string{"key", "key1", "key2"}) {
		t.Errorf("CalcKeys(ZUNIONSTORE key 2 key1 key2) failed. cmd=%s, group=%s, keys=%v", cmd, group, keys)
	}

}
