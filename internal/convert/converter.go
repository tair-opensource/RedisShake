package converter

import (
	lua "github.com/yuin/gopher-lua"
)

var luaInstance *lua.LState

func LoadFromFile(luaFile string) {
	luaInstance = lua.NewState()
	err := luaInstance.DoFile(luaFile)
	if err != nil {
		panic(err)
	}
}

func Convert(key string) string {
	if luaInstance == nil {
		return key
	}

	f := luaInstance.GetGlobal("converter")
	luaInstance.Push(f)
	luaInstance.Push(lua.LString(key)) // keys

	luaInstance.Call(1, 1)

	ret := luaInstance.Get(1).(lua.LString)
	luaInstance.Pop(1)
	return ret.String()
}
