package config

import (
	"sync"
	"testing"
)

func TestConfigSingleton(t *testing.T) {
	c1 := Get()
	c2 := Get()

	if c1 != c2 {
		t.Fatal("Get() returned different instances, expected same pointer")
	}

	if c1 == nil || c2 == nil {
		t.Fatal("Get() returned nil")
	}
}

func TestConfigSingletonConcurrent(t *testing.T) {
	const goroutines = 10

	var wg sync.WaitGroup
	results := make([]*Config, goroutines)

	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			results[idx] = Get()
		}(i)
	}

	wg.Wait()

	first := results[0]
	for i := 1; i < goroutines; i++ {
		if results[i] != first {
			t.Fatalf("goroutine %d got different instance", i)
		}
	}
}
