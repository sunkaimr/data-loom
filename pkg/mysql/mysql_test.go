package mysql

import (
	"fmt"
	"testing"
)

func TestBuildSelectSQL(t *testing.T) {
	tests := []struct {
		name       string
		table      string
		columns    []string
		conditions string
	}{
		{"test001", "student", []string{}, " id > 100 "},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			o, err := BuildSelectSQL(test.table, test.columns, test.conditions)
			if err != nil {
				t.Fatalf("generateSelectSQL failed, got error: %s", err)
			}
			fmt.Println(o)
		})
	}
}
