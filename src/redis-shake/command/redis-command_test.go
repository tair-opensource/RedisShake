package command

import (
	"testing"
)


func Test_Get_Match_Keys_Mset_Cmd(t *testing.T) {
	mset_cmd := RedisCommands["mset"]
	/*filterkey: x
	 *cmd: mset kk 1
	 */
	args := make([][]byte, 2)
	args[0] = []byte("kk")
	args[1] = []byte("1")
	filterkey := make([]string, 1)
	filterkey[0] = "x"
	new_args, ret := GetMatchKeys(mset_cmd, args, filterkey)

	if len(new_args) != 0 || ret != false {
		t.Error("mset test fail")
	}

	/*filterkey: k
	 *cmd: mset kk 1
	 */
	args = make([][]byte, 2)
	args[0] = []byte("kk")
	args[1] = []byte("1")
	filterkey = make([]string, 1)
	filterkey[0] = "k"
	new_args, ret = GetMatchKeys(mset_cmd, args, filterkey)

	if len(new_args) != 2 || ret != true {
		t.Error("mset test fail")
	}

	/*filterkey: k
	 *cmd: mset kk 1 gg ll zz nn k ll
	 */
	args = make([][]byte, 8)
	args[0] = []byte("kk")
	args[1] = []byte("1")
	args[2] = []byte("gg")
	args[3] = []byte("ll")
	args[4] = []byte("zz")
	args[5] = []byte("nn")
	args[6] = []byte("k")
	args[7] = []byte("ll")
	filterkey = make([]string, 1)
	filterkey[0] = "k"
	new_args, ret = GetMatchKeys(mset_cmd, args, filterkey)

	if len(new_args) != 4 || ret != true ||
		string(new_args[0]) != "kk" || string(new_args[1]) != "1" ||
		string(new_args[2]) != "k" || string(new_args[3]) != "ll" {
		t.Error("mset test fail")
	}
}

func Test_Get_Match_Keys_SetXX_Cmd(t *testing.T) {
	set_cmd := RedisCommands["set"]
	/*filterkey: x
	 *cmd: set kk 1
	 */
	args := make([][]byte, 2)
	args[0] = []byte("kk")
	args[1] = []byte("1")
	filterkey := make([]string, 1)
	filterkey[0] = "x"
	new_args, ret := GetMatchKeys(set_cmd, args, filterkey)

	if ret != false {
		t.Error("set test fail", ret, len(new_args))
	}

	/*filterkey: k
	 *cmd: set kk 1
	 */
	args = make([][]byte, 2)
	args[0] = []byte("kk")
	args[1] = []byte("1")
	filterkey = make([]string, 1)
	filterkey[0] = "k"
	new_args, ret = GetMatchKeys(set_cmd, args, filterkey)

	if len(new_args) != 2 || ret != true {
		t.Error("set test fail")
	}

	/*filterkey: k
	 *cmd: setex kk 3000 lll
	 */
	set_cmd = RedisCommands["setex"]
	args = make([][]byte, 3)
	args[0] = []byte("kk")
	args[1] = []byte("3000")
	args[2] = []byte("lll")
	filterkey = make([]string, 1)
	filterkey[0] = "k"
	new_args, ret = GetMatchKeys(set_cmd, args, filterkey)

	if len(new_args) != 3 || ret != true ||
		string(new_args[0]) != "kk" || string(new_args[1]) != "3000" ||
		string(new_args[2]) != "lll" {
		t.Error("setex test fail")
	}

	/*filterkey: k
	 *cmd: setrange kk 3000 lll
	 */
	set_cmd = RedisCommands["setrange"]
	args = make([][]byte, 3)
	args[0] = []byte("kk")
	args[1] = []byte("3000")
	args[2] = []byte("lll")
	filterkey = make([]string, 1)
	filterkey[0] = "k"
	new_args, ret = GetMatchKeys(set_cmd, args, filterkey)

	if len(new_args) != 3 || ret != true ||
		string(new_args[0]) != "kk" || string(new_args[1]) != "3000" ||
		string(new_args[2]) != "lll" {
		t.Error("setrange test fail")
	}
}
