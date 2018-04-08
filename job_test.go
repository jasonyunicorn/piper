package batch

import (
	"fmt"
	"math/rand"
	"testing"
)

func TestJob_NewJob(t *testing.T) {
	j := newJob(newTestData(1))
	if j == nil {
		t.Fatal("newJob returned nil")
	}
}

func TestJob_IncrementRetry(t *testing.T) {
	j := newJob(newTestData(2))
	count := rand.Intn(10) + 1
	for i := 0; i < count; i++ {
		j.incrementRetry()
	}
	if j.retries != count {
		t.Fatalf("invalid retry count: wanted [%d], got [%d]", count, j.retries)
	}
}

func TestJob_AddError(t *testing.T) {
	j := newJob(newTestData(3))
	count := rand.Intn(10) + 1
	for i := 0; i < count; i++ {
		j.addError(fmt.Errorf("Error#%d", i))
	}
	if len(j.errors) != count {
		t.Fatalf("invalid errors count: wanted [%d], got [%d]", count, len(j.errors))
	}
}
