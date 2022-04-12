// Tests for SquarerImpl. Students should write their code in this file.

package p0partB

import (
	"fmt"
	"math"
	"testing"
	"time"
)

const (
	timeoutMillis = 5000
)

type example struct {
	name    string
	value   int
	squared int
}

var examples = []example{
	{name: "positive", value: 2, squared: 4},
	{name: "negative", value: -2, squared: 4},
	{name: "zero", value: 0, squared: 0},
	{name: "max", value: math.MaxInt, squared: 1},
	{name: "min", value: math.MinInt, squared: 0},
}

func reverseSlice(s []example) []example {
	r := []example{}
	for i := len(s) - 1; i >= 0; i-- {
		r = append(r, s[i])
	}
	return r
}

func setupSquarer() (chan int, <-chan int, Squarer) {
	input := make(chan int)
	squarer := &SquarerImpl{}
	output := squarer.Initialize(input)
	return input, output, squarer
}

func TestBasicCorrectness(t *testing.T) {
	fmt.Println("Running TestBasicCorrectness.")
	input, squares, _ := setupSquarer()
	go func() {
		input <- 2
	}()
	timeoutChan := time.After(time.Duration(timeoutMillis) * time.Millisecond)
	select {
	case <-timeoutChan:
		t.Error("Test timed out.")
	case result := <-squares:
		if result != 4 {
			t.Error("Error, got result", result, ", expected 4 (=2^2).")
		}
	}
}

func TestValues(t *testing.T) {
	fmt.Println("Running TestValues.")
	input, squares, _ := setupSquarer()

	for _, s := range examples {
		t.Run(s.name, func(t *testing.T) {
			input <- s.value
			got := <-squares
			if got != s.squared {
				t.Errorf("Error. Testing %d^2: got %d but expected %d.", s.value, got, s.squared)
			}
		})
	}
}

func TestMultAccess(t *testing.T) {
	fmt.Println("Running TestMultAccess.")
	input, squares, _ := setupSquarer()
	close := make(chan int)
	count := 0
	samples := []([]example){
		examples,
		reverseSlice(examples),
		examples,
		reverseSlice(examples),
		examples,
		reverseSlice(examples),
	}

	for _, sample := range samples {
		count += len(sample)
		go func(sample []example) {
			for _, s := range sample {
				input <- s.value
				got := <-squares
				if got != s.squared {
					t.Errorf("Error. Testing %d^2: got %d but expected %d.", s.value, got, s.squared)
				}
				close <- 1
			}
		}(sample)
	}

	for i := 0; i < count; i++ {
		<-close
	}
}

// Squarer objects take a chan and return a chan that passes squares of the elements on the input chan.
// Squarer's should not have factory methods: they should be initialized by doing eg
// squarer := squarer{}
// mySquaredChan := squarer.Initialize(myChan)

// Test different types
// int, float, string, bool, nil, max/min num?

// Send multiple in before reading?
// Channels with buffer

// Initialize should be called first and once. Should return a chan on which squares will be passed.
// What happens when you call multiple times?

// Close closes the Squarer: after Close returns, all resources associated with the Squarer, including all goroutines.
// should be gone
// Check if all goroutines are deleted.
