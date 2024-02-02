package types

import (
	"bytes"
	"sort"
	"testing"
)

// rdbTypeSet 0
// rdbTypeSetIntset 11
// rdbTypeSetListpack 20

func testOne(t *testing.T, typeByte byte, setData string, values []string) {
	if typeByte != setData[0] {
		t.Errorf("typeByte not match. typeByte=[%d]", typeByte)
	}
	o := new(SetObject)
	o.LoadFromBuffer(bytes.NewReader([]byte(setData[1:])), "key", typeByte)
	cmdC := o.Rewrite()
	var elements []string
	for cmd := range cmdC {
		elements = append(elements, cmd[2])
	}
	if len(elements) != len(values) {
		t.Errorf("elements not match. len(o.elements)=[%d], len(values)=[%d]", len(elements), len(values))
	}
	count := len(elements)
	sort.Strings(elements)
	sort.Strings(values)
	// check set
	for i := 0; i < count; i++ {
		if elements[i] != values[i] {
			t.Errorf("elements not match. o.elements[i]=[%s], values[i]=[%s]", elements[i], values[i])
		}
	}
}

// sadd key 1 2 3 4 q w e r
func TestSetListpack(t *testing.T) {
	data := "\x14\x1b\x1b\x00\x00\x00\b\x00\x81w\x02\x81r\x02\x04\x01\x81e\x02\x01\x01\x02\x01\x81q\x02\x03\x01\xff\x0b\x00T\xe9)\xf7*\xe0\xe3\xf9"
	values := []string{"1", "2", "3", "4", "q", "w", "e", "r"}
	testOne(t, rdbTypeSetListpack, data, values)
}

// sadd key 1 2 3 4 5 6 7 8
func TestSetIntset(t *testing.T) {
	data := "\x0b\x18\x02\x00\x00\x00\b\x00\x00\x00\x01\x00\x02\x00\x03\x00\x04\x00\x05\x00\x06\x00\a\x00\b\x00\x0b\x00\xd7\x03\xf0nIZV\x8d"
	values := []string{"1", "2", "3", "4", "5", "6", "7", "8"}
	testOne(t, rdbTypeSetIntset, data, values)
}

// sadd key 1 2 3 4 q w e r
func TestSet(t *testing.T) {
	data := "\x02\b\xc0\x04\x01r\x01w\xc0\x02\xc0\x01\xc0\x03\x01q\x01e\t\x00r\x99O\xba\x8c\x8f\x00\xcb"
	values := []string{"1", "2", "3", "4", "q", "w", "e", "r"}
	testOne(t, rdbTypeSet, data, values)
}
