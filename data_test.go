package batch

import "strconv"

type testData struct {
	id    string
	value int
}

func newTestData(value int) *testData {
	return &testData{
		id:    strconv.Itoa(value),
		value: value,
	}
}

func (d *testData) GetID() string {
	return d.id
}
