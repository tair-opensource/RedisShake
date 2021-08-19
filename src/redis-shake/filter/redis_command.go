// redis command struct.
package filter

type getkeys_proc func(args []string) []int
type redisCommand struct {
	getkey_proc                getkeys_proc
	firstkey, lastkey, keystep int
}

var RedisCommands = map[string]redisCommand{
	"set":              {nil, 1, 1, 1},
	"setnx":            {nil, 1, 1, 1},
	"setex":            {nil, 1, 1, 1},
	"psetex":           {nil, 1, 1, 1},
	"append":           {nil, 1, 1, 1},
	"del":              {nil, 1, 0, 1},
	"unlink":           {nil, 1, -1, 1},
	"setbit":           {nil, 1, 1, 1},
	"bitfield":         {nil, 1, 1, 1},
	"setrange":         {nil, 1, 1, 1},
	"incr":             {nil, 1, 1, 1},
	"decr":             {nil, 1, 1, 1},
	"rpush":            {nil, 1, 1, 1},
	"lpush":            {nil, 1, 1, 1},
	"rpushx":           {nil, 1, 1, 1},
	"lpushx":           {nil, 1, 1, 1},
	"linsert":          {nil, 1, 1, 1},
	"rpop":             {nil, 1, 1, 1},
	"lpop":             {nil, 1, 1, 1},
	"brpop":            {nil, 1, -2, 1},
	"brpoplpush":       {nil, 1, 2, 1},
	"blpop":            {nil, 1, -2, 1},
	"lset":             {nil, 1, 1, 1},
	"ltrim":            {nil, 1, 1, 1},
	"lrem":             {nil, 1, 1, 1},
	"rpoplpush":        {nil, 1, 2, 1},
	"sadd":             {nil, 1, 1, 1},
	"srem":             {nil, 1, 1, 1},
	"smove":            {nil, 1, 2, 1},
	"spop":             {nil, 1, 1, 1},
	"sinterstore":      {nil, 1, -1, 1},
	"sunionstore":      {nil, 1, -1, 1},
	"sdiffstore":       {nil, 1, -1, 1},
	"zadd":             {nil, 1, 1, 1},
	"zincrby":          {nil, 1, 1, 1},
	"zrem":             {nil, 1, 1, 1},
	"zremrangebyscore": {nil, 1, 1, 1},
	"zremrangebyrank":  {nil, 1, 1, 1},
	"zremrangebylex":   {nil, 1, 1, 1},
	//"zunionstore", {zunionInterGetKeys, 0, 0, 0},
	//"zinterstore", {zunionInterGetKeys, 0, 0, 0},
	"hset":         {nil, 1, 1, 1},
	"hsetnx":       {nil, 1, 1, 1},
	"hmset":        {nil, 1, 1, 1},
	"hincrby":      {nil, 1, 1, 1},
	"hincrbyfloat": {nil, 1, 1, 1},
	"hdel":         {nil, 1, 1, 1},
	"incrby":       {nil, 1, 1, 1},
	"decrby":       {nil, 1, 1, 1},
	"incrbyfloat":  {nil, 1, 1, 1},
	"getset":       {nil, 1, 1, 1},
	"mset":         {nil, 1, -1, 2},
	"msetnx":       {nil, 1, -1, 2},
	"move":         {nil, 1, 1, 1},
	"rename":       {nil, 1, 2, 1},
	"renamenx":     {nil, 1, 2, 1},
	"expire":       {nil, 1, 1, 1},
	"expireat":     {nil, 1, 1, 1},
	"pexpire":      {nil, 1, 1, 1},
	"pexpireat":    {nil, 1, 1, 1},
	//"sort", {sortGetKeys, 1, 1, 1},
	"persist":        {nil, 1, 1, 1},
	"restore":        {nil, 1, 1, 1},
	"restore-asking": {nil, 1, 1, 1},
	//"eval", {evalGetKeys, 0, 0, 0},
	//"evalsha", {evalGetKeys, 0, 0, 0},
	"bitop":  {nil, 2, -1, 1},
	"geoadd": {nil, 1, 1, 1},
	//"georadius", {georadiusGetKeys, 1, 1, 1},
	//"georadiusbymember", {georadiusGetKeys, 1, 1, 1},
	"pfadd":   {nil, 1, 1, 1},
	"pfmerge": {nil, 1, -1, 1},
}

func getMatchKeys(redis_cmd redisCommand, args [][]byte) (new_args [][]byte, pass bool) {
	lastkey := redis_cmd.lastkey - 1
	keystep := redis_cmd.keystep

	if lastkey < 0 {
		lastkey = lastkey + len(args)
	}

	array := make([]int, len(args)) // store all positions of the pass key
	number := 0                     // matching key number
	for firstkey := redis_cmd.firstkey - 1; firstkey <= lastkey; firstkey += keystep {
		key := string(args[firstkey])
		if FilterKey(key) == false {
			// pass
			array[number] = firstkey
			number++
		}
	}

	pass = false
	new_args = make([][]byte, number*redis_cmd.keystep+len(args)-lastkey-redis_cmd.keystep)
	if number > 0 {
		pass = true
		for i := 0; i < number; i++ {
			for j := 0; j < redis_cmd.keystep; j++ {
				new_args[i*redis_cmd.keystep+j] = args[array[i]+j]
			}
		}
	}

	// add alias parameters
	j := 0
	for i := lastkey + redis_cmd.keystep; i < len(args); i++ {
		new_args[number*redis_cmd.keystep+j] = args[i]
		j = j + 1
	}

	return
}
