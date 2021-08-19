// +build linux darwin windows
// +build integration

package filter

import (
	"fmt"
	"testing"

	"redis-shake/configure"

	"github.com/stretchr/testify/assert"
)

func TestFilterCommands(t *testing.T) {
	// test FilterCommands

	var nr int
	{
		fmt.Printf("TestFilterCommands case %d.\n", nr)
		nr++

		assert.Equal(t, false, FilterCommands("unknown-cmd"), "should be equal")
		assert.Equal(t, true, FilterCommands("opinfo"), "should be equal")
		assert.Equal(t, false, FilterCommands("eval"), "should be equal")
		conf.Options.FilterLua = true
		assert.Equal(t, false, FilterCommands("unknown-cmd"), "should be equal")
		assert.Equal(t, true, FilterCommands("eval"), "should be equal")
		assert.Equal(t, true, FilterCommands("evalsha"), "should be equal")
		assert.Equal(t, true, FilterCommands("script"), "should be equal")

	}
}

func TestFilterKey(t *testing.T) {
	// test FilterKey

	var nr int
	{
		fmt.Printf("TestFilterKey case %d.\n", nr)
		nr++

		assert.Equal(t, false, FilterKey("unknown-key"), "should be equal")
	}

	{
		fmt.Printf("TestFilterKey case %d.\n", nr)
		nr++

		conf.Options.FilterKeyBlacklist = []string{"abc", "xyz", "a"}
		conf.Options.FilterKeyWhitelist = []string{}
		assert.Equal(t, false, FilterKey("unknown-key"), "should be equal")
		assert.Equal(t, true, FilterKey("abc"), "should be equal")
		assert.Equal(t, true, FilterKey("abc111"), "should be equal")
		assert.Equal(t, true, FilterKey("abcxyz"), "should be equal")
		assert.Equal(t, true, FilterKey("xyz"), "should be equal")
		assert.Equal(t, false, FilterKey("xy"), "should be equal")
		assert.Equal(t, true, FilterKey("a"), "should be equal")
		assert.Equal(t, true, FilterKey("ab"), "should be equal")
	}

	{
		fmt.Printf("TestFilterKey case %d.\n", nr)
		nr++

		conf.Options.FilterKeyBlacklist = []string{}
		conf.Options.FilterKeyWhitelist = []string{"abc", "xyz", "a"}
		assert.Equal(t, true, FilterKey("unknown-key"), "should be equal")
		assert.Equal(t, false, FilterKey("abc"), "should be equal")
		assert.Equal(t, false, FilterKey("abc111"), "should be equal")
		assert.Equal(t, false, FilterKey("abcxyz"), "should be equal")
		assert.Equal(t, false, FilterKey("xyz"), "should be equal")
		assert.Equal(t, true, FilterKey("xy"), "should be equal")
		assert.Equal(t, false, FilterKey("a"), "should be equal")
		assert.Equal(t, false, FilterKey("ab"), "should be equal")
	}
}

func TestFilterSlot(t *testing.T) {
	// test FilterSlot

	var nr int
	{
		fmt.Printf("TestFilterSlot case %d.\n", nr)
		nr++

		conf.Options.FilterSlot = []string{}
		assert.Equal(t, false, FilterSlot(2), "should be equal")
		assert.Equal(t, false, FilterSlot(0), "should be equal")
	}

	{
		fmt.Printf("TestFilterSlot case %d.\n", nr)
		nr++

		conf.Options.FilterSlot = []string{"1", "3", "5"}
		assert.Equal(t, false, FilterSlot(1), "should be equal")
		assert.Equal(t, true, FilterSlot(0), "should be equal")
		assert.Equal(t, false, FilterSlot(5), "should be equal")
	}
}

func TestFilterDB(t *testing.T) {
	// test FilterDB

	var nr int
	{
		fmt.Printf("TestFilterDB case %d.\n", nr)
		nr++

		conf.Options.FilterDBWhitelist = []string{}
		conf.Options.FilterDBBlacklist = []string{}
		assert.Equal(t, false, FilterDB(2), "should be equal")
		assert.Equal(t, false, FilterDB(0), "should be equal")
	}

	{
		fmt.Printf("TestFilterDB case %d.\n", nr)
		nr++

		conf.Options.FilterDBWhitelist = []string{"0", "1", "5"}
		conf.Options.FilterDBBlacklist = []string{}
		assert.Equal(t, true, FilterDB(2), "should be equal")
		assert.Equal(t, false, FilterDB(0), "should be equal")
		assert.Equal(t, false, FilterDB(5), "should be equal")

	}

	{
		fmt.Printf("TestFilterDB case %d.\n", nr)
		nr++

		conf.Options.FilterDBWhitelist = []string{}
		conf.Options.FilterDBBlacklist = []string{"0", "1", "5"}
		assert.Equal(t, false, FilterDB(2), "should be equal")
		assert.Equal(t, true, FilterDB(0), "should be equal")
		assert.Equal(t, true, FilterDB(5), "should be equal")

	}
}

func TestHandleFilterKeyWithCommand(t *testing.T) {
	// test HandleFilterKeyWithCommand

	var nr int
	var cmd string
	var args, expectArgs, ret [][]byte
	var filter bool
	{
		fmt.Printf("TestHandleFilterKeyWithCommand case %d.\n", nr)
		nr++

		cmd = "set"
		args = convertToByte("xyz", "1")
		conf.Options.FilterKeyBlacklist = []string{}
		conf.Options.FilterKeyWhitelist = []string{}
		ret, filter = HandleFilterKeyWithCommand(cmd, args)
		assert.Equal(t, false, filter, "should be equal")
		assert.Equal(t, args, ret, "should be equal")

		conf.Options.FilterKeyBlacklist = []string{"x", "y"}
		conf.Options.FilterKeyWhitelist = []string{}
		ret, filter = HandleFilterKeyWithCommand(cmd, args)
		assert.Equal(t, true, filter, "should be equal")

		conf.Options.FilterKeyBlacklist = []string{}
		conf.Options.FilterKeyWhitelist = []string{"x"}
		ret, filter = HandleFilterKeyWithCommand(cmd, args)
		assert.Equal(t, false, filter, "should be equal")
		assert.Equal(t, args, ret, "should be equal")
	}

	{
		fmt.Printf("TestHandleFilterKeyWithCommand case %d.\n", nr)
		nr++

		cmd = "mset"
		args = convertToByte("xyz", "1", "abc", "2", "ab", "3", "zzz", "1111111111111", "ffffffffff", "90")
		conf.Options.FilterKeyBlacklist = []string{}
		conf.Options.FilterKeyWhitelist = []string{}
		ret, filter = HandleFilterKeyWithCommand(cmd, args)
		assert.Equal(t, false, filter, "should be equal")
		assert.Equal(t, args, ret, "should be equal")

		conf.Options.FilterKeyBlacklist = []string{"x"}
		conf.Options.FilterKeyWhitelist = []string{}
		ret, filter = HandleFilterKeyWithCommand(cmd, args)
		expectArgs = convertToByte("abc", "2", "ab", "3", "zzz", "1111111111111", "ffffffffff", "90")
		assert.Equal(t, false, filter, "should be equal")
		assert.Equal(t, expectArgs, ret, "should be equal")

		conf.Options.FilterKeyBlacklist = []string{}
		conf.Options.FilterKeyWhitelist = []string{"x"}
		ret, filter = HandleFilterKeyWithCommand(cmd, args)
		expectArgs = convertToByte("xyz", "1")
		assert.Equal(t, false, filter, "should be equal")
		assert.Equal(t, expectArgs, ret, "should be equal")
	}

	{
		fmt.Printf("TestHandleFilterKeyWithCommand case %d.\n", nr)
		nr++

		cmd = "msetnx"
		args = convertToByte("xyz", "1", "abc", "2", "ab", "3", "zzz", "1111111111111", "ffffffffff", "90")
		conf.Options.FilterKeyBlacklist = []string{}
		conf.Options.FilterKeyWhitelist = []string{}
		ret, filter = HandleFilterKeyWithCommand(cmd, args)
		assert.Equal(t, false, filter, "should be equal")
		assert.Equal(t, args, ret, "should be equal")

		conf.Options.FilterKeyBlacklist = []string{"x"}
		conf.Options.FilterKeyWhitelist = []string{}
		ret, filter = HandleFilterKeyWithCommand(cmd, args)
		expectArgs = convertToByte("abc", "2", "ab", "3", "zzz", "1111111111111", "ffffffffff", "90")
		assert.Equal(t, false, filter, "should be equal")
		assert.Equal(t, expectArgs, ret, "should be equal")

		conf.Options.FilterKeyBlacklist = []string{}
		conf.Options.FilterKeyWhitelist = []string{"x"}
		ret, filter = HandleFilterKeyWithCommand(cmd, args)
		expectArgs = convertToByte("xyz", "1")
		assert.Equal(t, false, filter, "should be equal")
		assert.Equal(t, expectArgs, ret, "should be equal")
	}

	// unknown command, should pass
	{
		fmt.Printf("TestHandleFilterKeyWithCommand case %d.\n", nr)
		nr++

		cmd = "unknownCmd"
		args = convertToByte("xyz", "1")
		conf.Options.FilterKeyBlacklist = []string{}
		conf.Options.FilterKeyWhitelist = []string{}
		ret, filter = HandleFilterKeyWithCommand(cmd, args)
		assert.Equal(t, false, filter, "should be equal")
		assert.Equal(t, args, ret, "should be equal")
	}

	// length == 0
	{
		fmt.Printf("TestHandleFilterKeyWithCommand case %d.\n", nr)
		nr++

		cmd = "unknownCmd"
		args = convertToByte("xyz")
		conf.Options.FilterKeyBlacklist = []string{}
		conf.Options.FilterKeyWhitelist = []string{}
		ret, filter = HandleFilterKeyWithCommand(cmd, args)
		assert.Equal(t, false, filter, "should be equal")
		assert.Equal(t, args, ret, "should be equal")
	}

	// del
	{
		fmt.Printf("TestHandleFilterKeyWithCommand case %d.\n", nr)
		nr++

		cmd = "del"
		args = convertToByte("xyz", "abc", "ab", "zzz", "ffffffffff")
		conf.Options.FilterKeyBlacklist = []string{}
		conf.Options.FilterKeyWhitelist = []string{}
		ret, filter = HandleFilterKeyWithCommand(cmd, args)
		assert.Equal(t, false, filter, "should be equal")
		assert.Equal(t, args, ret, "should be equal")

		conf.Options.FilterKeyBlacklist = []string{"x"}
		conf.Options.FilterKeyWhitelist = []string{}
		ret, filter = HandleFilterKeyWithCommand(cmd, args)
		expectArgs = convertToByte("abc", "ab", "zzz", "ffffffffff")
		assert.Equal(t, false, filter, "should be equal")
		assert.Equal(t, expectArgs, ret, "should be equal")

		conf.Options.FilterKeyBlacklist = []string{}
		conf.Options.FilterKeyWhitelist = []string{"x"}
		ret, filter = HandleFilterKeyWithCommand(cmd, args)
		expectArgs = convertToByte("xyz")
		assert.Equal(t, false, filter, "should be equal")
		assert.Equal(t, expectArgs, ret, "should be equal")
	}
}

func TestHasAtLeastOnePrefix(t *testing.T) {
	cases := []struct {
		key          string
		prefixes     []string
		expectResult bool
	}{
		{
			// no prefix provided
			"a",
			[]string{},
			false,
		},
		{
			// has prefix
			"abc",
			[]string{"ab"},
			true,
		},
		{
			// does NOT have prefix
			"abc",
			[]string{"edf", "wab"},
			false,
		},
	}

	for _, c := range cases {
		result := hasAtLeastOnePrefix(c.key, c.prefixes)
		assert.Equal(t, c.expectResult, result)
	}
}

func convertToByte(args ...string) [][]byte {
	ret := make([][]byte, 0)
	for _, arg := range args {
		ret = append(ret, []byte(arg))
	}
	return ret
}
