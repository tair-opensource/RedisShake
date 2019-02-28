package rdb_test

import (
	"bytes"
	"encoding/base64"

	"github.com/cupcake/rdb"
	. "gopkg.in/check.v1"
)

type EncoderSuite struct{}

var _ = Suite(&EncoderSuite{})

var stringEncodingTests = []struct {
	str string
	res string
}{
	{"0", "AMAABgAOrc/4DQU/mw=="},
	{"127", "AMB/BgCbWIOxpwH5hw=="},
	{"-128", "AMCABgAPi1rt2llnSg=="},
	{"128", "AMGAAAYAfZfbNeWad/Y="},
	{"-129", "AMF//wYAgY3qqKHVuBM="},
	{"32767", "AMH/fwYA37dfWuKh6bg="},
	{"-32768", "AMEAgAYAI61ux6buJl0="},
	{"-32768", "AMEAgAYAI61ux6buJl0="},
	{"2147483647", "AML///9/BgC6mY0eFXuRMg=="},
	{"-2147483648", "AMIAAACABgBRou++xgC9FA=="},
	{"a", "AAFhBgApE4cbemNBJw=="},
}

func (e *EncoderSuite) TestStringEncoding(c *C) {
	buf := &bytes.Buffer{}
	for _, t := range stringEncodingTests {
		e := rdb.NewEncoder(buf)
		e.EncodeType(rdb.TypeString)
		e.EncodeString([]byte(t.str))
		e.EncodeDumpFooter()
		expected, _ := base64.StdEncoding.DecodeString(t.res)
		c.Assert(buf.Bytes(), DeepEquals, expected, Commentf("%s - expected: %x, actual: %x", t.str, expected, buf.Bytes()))
		buf.Reset()
	}
}
