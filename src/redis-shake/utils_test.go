package run

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

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
