package function

import (
	"RedisShake/internal/config"
	"RedisShake/internal/entry"
	"RedisShake/internal/log"
	lua "github.com/yuin/gopher-lua"
	"strings"
)

var luaString string

func Init() {
	luaString = config.Opt.Function
	luaString = strings.TrimSpace(luaString)
	if len(luaString) == 0 {
		log.Infof("no function script")
		return
	}
}

// DB
// GROUP
// CMD
// KEYS
// KEY_INDEXES
// SLOTS
// ARGV

// shake.call(DB, ARGV)
// shake.log()

func RunFunction(e *entry.Entry) []*entry.Entry {
	entries := make([]*entry.Entry, 0)
	if len(luaString) == 0 {
		entries = append(entries, e)
		return entries
	}

	L := lua.NewState()
	L.SetGlobal("DB", lua.LNumber(e.DbId))
	L.SetGlobal("GROUP", lua.LString(e.Group))
	L.SetGlobal("CMD", lua.LString(e.CmdName))
	keys := L.NewTable()
	for _, key := range e.Keys {
		keys.Append(lua.LString(key))
	}
	L.SetGlobal("KEYS", keys)
	slots := L.NewTable()
	for _, slot := range e.Slots {
		slots.Append(lua.LNumber(slot))
	}
	keyIndexes := L.NewTable()
	for _, keyIndex := range e.KeyIndexes {
		keyIndexes.Append(lua.LNumber(keyIndex))
	}
	L.SetGlobal("KEY_INDEXES", keyIndexes)
	L.SetGlobal("SLOTS", slots)
	argv := L.NewTable()
	for _, arg := range e.Argv {
		argv.Append(lua.LString(arg))
	}
	L.SetGlobal("ARGV", argv)
	shake := L.NewTypeMetatable("shake")
	L.SetGlobal("shake", shake)

	L.SetField(shake, "call", L.NewFunction(func(ls *lua.LState) int {
		db := ls.ToInt(1)
		argv := ls.ToTable(2)
		var argvStrings []string
		for i := 1; i <= argv.Len(); i++ {
			argvStrings = append(argvStrings, argv.RawGetInt(i).String())
		}
		entries = append(entries, &entry.Entry{
			DbId: db,
			Argv: argvStrings,
		})
		return 0
	}))
	L.SetField(shake, "log", L.NewFunction(func(ls *lua.LState) int {
		log.Infof("lua log: %v", ls.ToString(1))
		return 0
	}))
	err := L.DoString(luaString)
	if err != nil {
		log.Panicf("load function script failed: %v", err)
	}

	return entries
}
