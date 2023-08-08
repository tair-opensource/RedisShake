package main

import (
	//"fmt"
	//"io"
	//"math"
	"os"
	"testing"
)

func TestReadString(t *testing.T) {
	// Create a temporary file and write some data into it
	tempFile, err := os.CreateTemp("", "testfile")
	if err != nil {
		t.Fatal(err)
	}
	defer os.Remove(tempFile.Name())
	data := []byte("$5\r\nHello\r\n")
	_, err = tempFile.Write(data)
	if err != nil {
		t.Fatal(err)
	}

	// Open the temporary file for reading
	file, err := os.Open(tempFile.Name())
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()

	// Test reading a valid string
	var target string
	expectedResult := 1
	result := readString(file, &target)
	if result != expectedResult {
		t.Errorf("1Expected %d, but got %d", expectedResult, result)
	}
	expectedValue := "Hello"
	if target != expectedValue {
		t.Errorf("2Expected value '%s', but got '%s'", expectedValue, target)
	}

}
