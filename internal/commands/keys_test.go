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
	cmd, group, keys, _ := CalcKeys([]string{"SET", "key", "value"})
	if cmd != "SET" || group != "STRING" || !testEq(keys, []string{"key"}) {
		t.Errorf("CalcKeys(SET key value) failed. cmd=%s, group=%s, keys=%v", cmd, group, keys)
	}

	// MSET
	cmd, group, keys, _ = CalcKeys([]string{"MSET", "key1", "value1", "key2", "value2"})
	if cmd != "MSET" || group != "STRING" || !testEq(keys, []string{"key1", "key2"}) {
		t.Errorf("CalcKeys(MSET key1 value1 key2 value2) failed. cmd=%s, group=%s, keys=%v", cmd, group, keys)
	}

	// XADD
	cmd, group, keys, _ = CalcKeys([]string{"XADD", "key", "*", "field1", "value1", "field2", "value2"})
	if cmd != "XADD" || group != "STREAM" || !testEq(keys, []string{"key"}) {
		t.Errorf("CalcKeys(XADD key * field1 value1 field2 value2) failed. cmd=%s, group=%s, keys=%v", cmd, group, keys)
	}

	// ZUNIONSTORE
	cmd, group, keys, _ = CalcKeys([]string{"ZUNIONSTORE", "key", "2", "key1", "key2"})
	if cmd != "ZUNIONSTORE" || group != "SORTED_SET" || !testEq(keys, []string{"key", "key1", "key2"}) {
		t.Errorf("CalcKeys(ZUNIONSTORE key 2 key1 key2) failed. cmd=%s, group=%s, keys=%v", cmd, group, keys)
	}

	// COMMAND
	cmd, group, keys, _ = CalcKeys([]string{"COMMAND"})
	if cmd != "COMMAND" || group != "SERVER" || !testEq(keys, []string{}) {
		t.Errorf("CalcKeys(COMMAND) failed. cmd=%s, group=%s, keys=%v", cmd, group, keys)
	}

	// GEORADIUS with STORE
	cmd, group, keys, _ = CalcKeys([]string{"GEORADIUS", "Sicily", "15", "37", "200", "km", "STORE", "dest"})
	if cmd != "GEORADIUS" || group != "GEO" || !testEq(keys, []string{"Sicily", "dest"}) {
		t.Errorf("CalcKeys(GEORADIUS with STORE) failed. cmd=%s, group=%s, keys=%v", cmd, group, keys)
	}

	// GEORADIUS with STOREDIST
	cmd, group, keys, _ = CalcKeys([]string{"GEORADIUS", "Sicily", "15", "37", "200", "km", "STOREDIST", "dest"})
	if cmd != "GEORADIUS" || group != "GEO" || !testEq(keys, []string{"Sicily", "dest"}) {
		t.Errorf("CalcKeys(GEORADIUS with STOREDIST) failed. cmd=%s, group=%s, keys=%v", cmd, group, keys)
	}

	// GEORADIUS without STORE or STOREDIST
	cmd, group, keys, _ = CalcKeys([]string{"GEORADIUS", "Sicily", "15", "37", "200", "km"})
	if cmd != "GEORADIUS" || group != "GEO" || !testEq(keys, []string{"Sicily"}) {
		t.Errorf("CalcKeys(GEORADIUS without STORE) failed. cmd=%s, group=%s, keys=%v", cmd, group, keys)
	}

	// GEORADIUSBYMEMBER with STORE
	cmd, group, keys, _ = CalcKeys([]string{"GEORADIUSBYMEMBER", "Sicily", "Palermo", "200", "km", "STORE", "dest"})
	if cmd != "GEORADIUSBYMEMBER" || group != "GEO" || !testEq(keys, []string{"Sicily", "dest"}) {
		t.Errorf("CalcKeys(GEORADIUSBYMEMBER with STORE) failed. cmd=%s, group=%s, keys=%v", cmd, group, keys)
	}

	// GEORADIUSBYMEMBER without STORE or STOREDIST
	cmd, group, keys, _ = CalcKeys([]string{"GEORADIUSBYMEMBER", "Sicily", "Palermo", "200", "km"})
	if cmd != "GEORADIUSBYMEMBER" || group != "GEO" || !testEq(keys, []string{"Sicily"}) {
		t.Errorf("CalcKeys(GEORADIUSBYMEMBER without STORE) failed. cmd=%s, group=%s, keys=%v", cmd, group, keys)
	}
}

func TestKeyHash(t *testing.T) {
	ret := keyHash("abcde")
	if ret != 16097 {
		t.Errorf("keyHash(abcde) = %x", ret)
	}
	ret = keyHash("abcde{")
	if ret != 14689 {
		t.Errorf("keyHash(abcde{) = %x", ret)
	}
	ret = keyHash("abcde}")
	if ret != 6567 {
		t.Errorf("keyHash(abcde}) = %x", ret)
	}
	ret = keyHash("{abcde}")
	if ret != 16097 {
		t.Errorf("keyHash({abcde}) = %x", ret)
	}
	ret = keyHash("same")
	if ret != 13447 {
		t.Errorf("keyHash(same) = %x", ret)
	}
	ret = keyHash("abcdefghi{same}abcdefghi")
	if ret != 13447 {
		t.Errorf("keyHash(abcdefghi{same}abcdefghi) = %x", ret)
	}
	ret = keyHash("123456789{same}123456789")
	if ret != 13447 {
		t.Errorf("keyHash(123456789{same}123456789) = %x", ret)
	}
	ret = keyHash("1234我是你89{same}12我就456789")
	if ret != 13447 {
		t.Errorf("keyHash(1234我是你89{same}12我就456789) = %x", ret)
	}
	ret = keyHash("你你你{你你你}你你你")
	if ret != 15023 {
		t.Errorf("keyHash(1234我是你89{same}12我就456789) = %x", ret)
	}
	b := make([]byte, 0)
	for i := 0; i < 256; i++ {
		b = append(b, byte(i))
	}
	ret = keyHash(string(b))
	if ret != 16155 {
		t.Errorf("keyHash(%s) = %x", string(b), ret)
	}
}
