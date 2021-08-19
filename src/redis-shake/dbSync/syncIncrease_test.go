package dbSync

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestBarrierStatus(t *testing.T) {
	// test barrierStatus

	var nr int

	// test barrierStatus
	{
		fmt.Printf("TestBarrierStatus case %d.\n", nr)
		nr++

		bs, flushStatus := barrierStatus("set", barrierStatusNo)
		assert.Equal(t, barrierStatusNo, bs, "should be equal")
		assert.Equal(t, flushStatusNo, flushStatus, "should be equal")

		bs, flushStatus = barrierStatus("select", bs)
		assert.Equal(t, barrierStatusAdd, bs, "should be equal")
		assert.Equal(t, flushStatusYes, flushStatus, "should be equal")
	}

	{
		fmt.Printf("TestBarrierStatus case %d.\n", nr)
		nr++

		bs, flushStatus := barrierStatus("multi", barrierStatusNo)
		assert.Equal(t, barrierStatusHoldStart, bs, "should be equal")
		assert.Equal(t, flushStatusYes, flushStatus, "should be equal")

		bs, flushStatus = barrierStatus("set", bs)
		assert.Equal(t, barrierStatusHolding, bs, "should be equal")
		assert.Equal(t, flushStatusNo, flushStatus, "should be equal")

		bs, flushStatus = barrierStatus("exec", bs)
		assert.Equal(t, barrierStatusHoldEnd, bs, "should be equal")
		assert.Equal(t, flushStatusYes, flushStatus, "should be equal")

		bs, flushStatus = barrierStatus("set", bs)
		assert.Equal(t, barrierStatusNo, bs, "should be equal")
		assert.Equal(t, flushStatusNo, flushStatus, "should be equal")
	}
}
