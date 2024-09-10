package filter

import (
	"strings"
	"sync"

	"RedisShake/internal/entry"
	"RedisShake/internal/log"

	lua "github.com/yuin/gopher-lua"
	"github.com/yuin/gopher-lua/parse"
)

type Runtime struct {
	luaVMPool        *sync.Pool
	compiledFunction *lua.FunctionProto
}

func NewFunctionFilter(luaCode string) *Runtime {
	if len(luaCode) == 0 {
		return nil
	}
	luaCode = strings.TrimSpace(luaCode)
	chunk, err := parse.Parse(strings.NewReader(luaCode), "<string>")
	if err != nil {
		log.Panicf("parse lua code failed: %v", err)
	}
	codeObject, err := lua.Compile(chunk, "<string>")
	if err != nil {
		log.Panicf("compile lua code failed: %v", err)
	}
	return &Runtime{
		luaVMPool: &sync.Pool{
			New: func() interface{} {
				return lua.NewState()
			},
		},
		compiledFunction: codeObject,
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

func (runtime *Runtime) RunFunction(e *entry.Entry) []*entry.Entry {
	if runtime == nil {
		return []*entry.Entry{e}
	}
	entries := make([]*entry.Entry, 0)
	luaState := runtime.luaVMPool.Get().(*lua.LState)
	defer runtime.luaVMPool.Put(luaState)
	luaState.SetGlobal("DB", lua.LNumber(e.DbId))
	luaState.SetGlobal("GROUP", lua.LString(e.Group))
	luaState.SetGlobal("CMD", lua.LString(e.CmdName))
	keys := luaState.NewTable()
	for _, key := range e.Keys {
		keys.Append(lua.LString(key))
	}
	luaState.SetGlobal("KEYS", keys)
	slots := luaState.NewTable()
	for _, slot := range e.Slots {
		slots.Append(lua.LNumber(slot))
	}
	keyIndexes := luaState.NewTable()
	for _, keyIndex := range e.KeyIndexes {
		keyIndexes.Append(lua.LNumber(keyIndex))
	}
	luaState.SetGlobal("KEY_INDEXES", keyIndexes)
	luaState.SetGlobal("SLOTS", slots)
	argv := luaState.NewTable()
	for _, arg := range e.Argv {
		argv.Append(lua.LString(arg))
	}
	luaState.SetGlobal("ARGV", argv)
	shake := luaState.NewTypeMetatable("shake")
	luaState.SetGlobal("shake", shake)

	luaState.SetField(shake, "call", luaState.NewFunction(func(ls *lua.LState) int {
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
	luaState.SetField(shake, "log", luaState.NewFunction(func(ls *lua.LState) int {
		log.Infof("lua log: %v", ls.ToString(1))
		return 0
	}))
	luaState.Push(luaState.NewFunctionFromProto(runtime.compiledFunction))
	if err := luaState.PCall(0, lua.MultRet, nil); err != nil {
		log.Panicf("load function script failed: %v", err)
	}

	return entries
}
