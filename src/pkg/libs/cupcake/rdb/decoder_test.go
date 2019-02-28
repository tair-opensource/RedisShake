package rdb_test

import (
	"fmt"
	"os"
	"strings"
	"testing"

	"github.com/cupcake/rdb"
	. "gopkg.in/check.v1"
)

// Hook gocheck into the gotest runner.
func Test(t *testing.T) { TestingT(t) }

type DecoderSuite struct{}

var _ = Suite(&DecoderSuite{})

func (s *DecoderSuite) TestEmptyRDB(c *C) {
	r := decodeRDB("empty_database")
	c.Assert(r.started, Equals, 1)
	c.Assert(r.ended, Equals, 1)
	c.Assert(len(r.dbs), Equals, 0)
}

func (s *DecoderSuite) TestMultipleDatabases(c *C) {
	r := decodeRDB("multiple_databases")
	c.Assert(len(r.dbs), Equals, 2)
	_, ok := r.dbs[1]
	c.Assert(ok, Equals, false)
	c.Assert(r.dbs[0]["key_in_zeroth_database"], Equals, "zero")
	c.Assert(r.dbs[2]["key_in_second_database"], Equals, "second")
}

func (s *DecoderSuite) TestExpiry(c *C) {
	r := decodeRDB("keys_with_expiry")
	c.Assert(r.expiries[0]["expires_ms_precision"], Equals, int64(1671963072573))
}

func (s *DecoderSuite) TestMixedExpiry(c *C) {
	r := decodeRDB("keys_with_mixed_expiry")
	c.Assert(r.expiries[0]["key01"], Not(Equals), int64(0))
	c.Assert(r.expiries[0]["key04"], Not(Equals), int64(0))

	c.Assert(r.expiries[0]["key02"], Equals, int64(0))
	c.Assert(r.expiries[0]["key03"], Equals, int64(0))
}

func (s *DecoderSuite) TestIntegerKeys(c *C) {
	r := decodeRDB("integer_keys")
	c.Assert(r.dbs[0]["125"], Equals, "Positive 8 bit integer")
	c.Assert(r.dbs[0]["43947"], Equals, "Positive 16 bit integer")
	c.Assert(r.dbs[0]["183358245"], Equals, "Positive 32 bit integer")
	c.Assert(r.dbs[0]["-123"], Equals, "Negative 8 bit integer")
	c.Assert(r.dbs[0]["-29477"], Equals, "Negative 16 bit integer")
	c.Assert(r.dbs[0]["-183358245"], Equals, "Negative 32 bit integer")
}

func (s *DecoderSuite) TestStringKeyWithCompression(c *C) {
	r := decodeRDB("easily_compressible_string_key")
	c.Assert(r.dbs[0][strings.Repeat("a", 200)], Equals, "Key that redis should compress easily")
}

func (s *DecoderSuite) TestZipmapWithCompression(c *C) {
	r := decodeRDB("zipmap_that_compresses_easily")
	zm := r.dbs[0]["zipmap_compresses_easily"].(map[string]string)
	c.Assert(zm["a"], Equals, "aa")
	c.Assert(zm["aa"], Equals, "aaaa")
	c.Assert(zm["aaaaa"], Equals, "aaaaaaaaaaaaaa")
}

func (s *DecoderSuite) TestZipmap(c *C) {
	r := decodeRDB("zipmap_that_doesnt_compress")
	zm := r.dbs[0]["zimap_doesnt_compress"].(map[string]string)
	c.Assert(zm["MKD1G6"], Equals, "2")
	c.Assert(zm["YNNXK"], Equals, "F7TI")
}

func (s *DecoderSuite) TestZipmapWitBigValues(c *C) {
	r := decodeRDB("zipmap_with_big_values")
	zm := r.dbs[0]["zipmap_with_big_values"].(map[string]string)
	c.Assert(len(zm["253bytes"]), Equals, 253)
	c.Assert(len(zm["254bytes"]), Equals, 254)
	c.Assert(len(zm["255bytes"]), Equals, 255)
	c.Assert(len(zm["300bytes"]), Equals, 300)
	c.Assert(len(zm["20kbytes"]), Equals, 20000)
}

func (s *DecoderSuite) TestHashZiplist(c *C) {
	r := decodeRDB("hash_as_ziplist")
	zm := r.dbs[0]["zipmap_compresses_easily"].(map[string]string)
	c.Assert(zm["a"], Equals, "aa")
	c.Assert(zm["aa"], Equals, "aaaa")
	c.Assert(zm["aaaaa"], Equals, "aaaaaaaaaaaaaa")
}

func (s *DecoderSuite) TestDictionary(c *C) {
	r := decodeRDB("dictionary")
	d := r.dbs[0]["force_dictionary"].(map[string]string)
	c.Assert(len(d), Equals, 1000)
	c.Assert(d["ZMU5WEJDG7KU89AOG5LJT6K7HMNB3DEI43M6EYTJ83VRJ6XNXQ"], Equals, "T63SOS8DQJF0Q0VJEZ0D1IQFCYTIPSBOUIAI9SB0OV57MQR1FI")
	c.Assert(d["UHS5ESW4HLK8XOGTM39IK1SJEUGVV9WOPK6JYA5QBZSJU84491"], Equals, "6VULTCV52FXJ8MGVSFTZVAGK2JXZMGQ5F8OVJI0X6GEDDR27RZ")
}

func (s *DecoderSuite) TestZiplistWithCompression(c *C) {
	r := decodeRDB("ziplist_that_compresses_easily")
	for i, length := range []int{6, 12, 18, 24, 30, 36} {
		c.Assert(r.dbs[0]["ziplist_compresses_easily"].([]string)[i], Equals, strings.Repeat("a", length))
	}
}

func (s *DecoderSuite) TestZiplist(c *C) {
	r := decodeRDB("ziplist_that_doesnt_compress")
	l := r.dbs[0]["ziplist_doesnt_compress"].([]string)
	c.Assert(l[0], Equals, "aj2410")
	c.Assert(l[1], Equals, "cc953a17a8e096e76a44169ad3f9ac87c5f8248a403274416179aa9fbd852344")
}

func (s *DecoderSuite) TestZiplistWithInts(c *C) {
	r := decodeRDB("ziplist_with_integers")
	expected := []string{"0", "1", "2", "3", "4", "5", "6", "7", "8", "9", "10", "11", "12", "-2", "13", "25", "-61", "63", "16380", "-16000", "65535", "-65523", "4194304", "9223372036854775807"}
	for i, x := range expected {
		c.Assert(r.dbs[0]["ziplist_with_integers"].([]string)[i], Equals, x)
	}
}

func (s *DecoderSuite) TestIntSet16(c *C) {
	r := decodeRDB("intset_16")
	for i, x := range []string{"32764", "32765", "32766"} {
		c.Assert(r.dbs[0]["intset_16"].([]string)[i], Equals, x)
	}
}

func (s *DecoderSuite) TestIntSet32(c *C) {
	r := decodeRDB("intset_32")
	for i, x := range []string{"2147418108", "2147418109", "2147418110"} {
		c.Assert(r.dbs[0]["intset_32"].([]string)[i], Equals, x)
	}
}

func (s *DecoderSuite) TestIntSet64(c *C) {
	r := decodeRDB("intset_64")
	for i, x := range []string{"9223090557583032316", "9223090557583032317", "9223090557583032318"} {
		c.Assert(r.dbs[0]["intset_64"].([]string)[i], Equals, x)
	}
}

func (s *DecoderSuite) TestSet(c *C) {
	r := decodeRDB("regular_set")
	for i, x := range []string{"beta", "delta", "alpha", "phi", "gamma", "kappa"} {
		c.Assert(r.dbs[0]["regular_set"].([]string)[i], Equals, x)
	}
}

func (s *DecoderSuite) TestZSetZiplist(c *C) {
	r := decodeRDB("sorted_set_as_ziplist")
	z := r.dbs[0]["sorted_set_as_ziplist"].(map[string]float64)
	c.Assert(z["8b6ba6718a786daefa69438148361901"], Equals, float64(1))
	c.Assert(z["cb7a24bb7528f934b841b34c3a73e0c7"], Equals, float64(2.37))
	c.Assert(z["523af537946b79c4f8369ed39ba78605"], Equals, float64(3.423))
}

func (s *DecoderSuite) TestRDBv5(c *C) {
	r := decodeRDB("rdb_version_5_with_checksum")
	c.Assert(r.dbs[0]["abcd"], Equals, "efgh")
	c.Assert(r.dbs[0]["foo"], Equals, "bar")
	c.Assert(r.dbs[0]["bar"], Equals, "baz")
	c.Assert(r.dbs[0]["abcdef"], Equals, "abcdef")
	c.Assert(r.dbs[0]["longerstring"], Equals, "thisisalongerstring.idontknowwhatitmeans")
}

func (s *DecoderSuite) TestRDBv7(c *C) {
	r := decodeRDB("rdb_v7_list_quicklist")
	c.Assert(r.aux["redis-ver"], Equals, "3.2.0")
	c.Assert(r.dbSize[0], Equals, uint32(1))
	c.Assert(r.expiresSize[0], Equals, uint32(0))
	z := r.dbs[0]["foo"].([]string)
	c.Assert(z[0], Equals, "bar")
	c.Assert(z[1], Equals, "baz")
	c.Assert(z[2], Equals, "boo")
}

func (s *DecoderSuite) TestDumpDecoder(c *C) {
	r := &FakeRedis{}
	err := rdb.DecodeDump([]byte("\u0000\xC0\n\u0006\u0000\xF8r?\xC5\xFB\xFB_("), 1, []byte("test"), 123, r)
	if err != nil {
		c.Error(err)
	}
	c.Assert(r.dbs[1]["test"], Equals, "10")
}

func decodeRDB(name string) *FakeRedis {
	r := &FakeRedis{}
	f, err := os.Open("fixtures/" + name + ".rdb")
	if err != nil {
		panic(err)
	}
	err = rdb.Decode(f, r)
	if err != nil {
		panic(err)
	}
	return r
}

type FakeRedis struct {
	dbs         map[int]map[string]interface{}
	lengths     map[int]map[string]int
	expiries    map[int]map[string]int64
	dbSize      map[int]uint32
	expiresSize map[int]uint32

	cdb     int
	started int
	ended   int

	aux map[string]string
}

func (r *FakeRedis) setExpiry(key []byte, expiry int64) {
	r.expiries[r.cdb][string(key)] = expiry
}

func (r *FakeRedis) setLength(key []byte, length int64) {
	r.lengths[r.cdb][string(key)] = int(length)
}

func (r *FakeRedis) getLength(key []byte) int {
	return int(r.lengths[r.cdb][string(key)])
}

func (r *FakeRedis) db() map[string]interface{} {
	return r.dbs[r.cdb]
}

func (r *FakeRedis) StartRDB() {
	r.started++
	r.dbs = make(map[int]map[string]interface{})
	r.expiries = make(map[int]map[string]int64)
	r.lengths = make(map[int]map[string]int)
	r.aux = make(map[string]string)
	r.dbSize = make(map[int]uint32)
	r.expiresSize = make(map[int]uint32)
}

func (r *FakeRedis) StartDatabase(n int) {
	r.dbs[n] = make(map[string]interface{})
	r.expiries[n] = make(map[string]int64)
	r.lengths[n] = make(map[string]int)
	r.cdb = n
}

func (r *FakeRedis) Set(key, value []byte, expiry int64) {
	r.setExpiry(key, expiry)
	r.db()[string(key)] = string(value)
}

func (r *FakeRedis) StartHash(key []byte, length, expiry int64) {
	r.setExpiry(key, expiry)
	r.setLength(key, length)
	r.db()[string(key)] = make(map[string]string)
}

func (r *FakeRedis) Hset(key, field, value []byte) {
	r.db()[string(key)].(map[string]string)[string(field)] = string(value)
}

func (r *FakeRedis) EndHash(key []byte) {
	actual := len(r.db()[string(key)].(map[string]string))
	if actual != r.getLength(key) {
		panic(fmt.Sprintf("wrong length for key %s got %d, expected %d", key, actual, r.getLength(key)))
	}
}

func (r *FakeRedis) StartSet(key []byte, cardinality, expiry int64) {
	r.setExpiry(key, expiry)
	r.setLength(key, cardinality)
	r.db()[string(key)] = make([]string, 0, cardinality)
}

func (r *FakeRedis) Sadd(key, member []byte) {
	r.db()[string(key)] = append(r.db()[string(key)].([]string), string(member))
}

func (r *FakeRedis) EndSet(key []byte) {
	actual := len(r.db()[string(key)].([]string))
	if actual != r.getLength(key) {
		panic(fmt.Sprintf("wrong length for key %s got %d, expected %d", key, actual, r.getLength(key)))
	}
}

func (r *FakeRedis) StartList(key []byte, length, expiry int64) {
	r.setExpiry(key, expiry)
	r.setLength(key, length)
	cap := length
	if length < 0 {
		cap = 1
	}
	r.db()[string(key)] = make([]string, 0, cap)
}

func (r *FakeRedis) Rpush(key, value []byte) {
	r.db()[string(key)] = append(r.db()[string(key)].([]string), string(value))
}

func (r *FakeRedis) EndList(key []byte) {
	actual := len(r.db()[string(key)].([]string))
	if actual != r.getLength(key) && r.getLength(key) >= 0 {
		panic(fmt.Sprintf("wrong length for key %s got %d, expected %d", key, actual, r.getLength(key)))
	}
}

func (r *FakeRedis) StartZSet(key []byte, cardinality, expiry int64) {
	r.setExpiry(key, expiry)
	r.setLength(key, cardinality)
	r.db()[string(key)] = make(map[string]float64)
}

func (r *FakeRedis) Zadd(key []byte, score float64, member []byte) {
	r.db()[string(key)].(map[string]float64)[string(member)] = score
}

func (r *FakeRedis) EndZSet(key []byte) {
	actual := len(r.db()[string(key)].(map[string]float64))
	if actual != r.getLength(key) {
		panic(fmt.Sprintf("wrong length for key %s got %d, expected %d", key, actual, r.getLength(key)))
	}
}

func (r *FakeRedis) EndDatabase(n int) {
	if n != r.cdb {
		panic(fmt.Sprintf("database end called with %d, expected %d", n, r.cdb))
	}
}

func (r *FakeRedis) EndRDB() {
	r.ended++
}

func (r *FakeRedis) Aux(key, value []byte) {
	r.aux[string(key)] = string(value)
}

func (r *FakeRedis) ResizeDatabase(dbSize, expiresSize uint32) {
	r.dbSize[r.cdb] = dbSize
	r.expiresSize[r.cdb] = expiresSize
}
