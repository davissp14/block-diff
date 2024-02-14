package main

import "testing"

func TestFormatFileSize(t *testing.T) {
	tests := []struct {
		size     float64
		expected string
	}{
		{size: 0, expected: "0B"},
		{size: 1024, expected: "1.0KiB"},
		{size: 1024 * 1024, expected: "1.0MiB"},
		{size: 1024 * 1024 * 555.52, expected: "555.52MiB"},
		{size: 1024 * 1024 * 1024, expected: "1.0GiB"},
		{size: 1024 * 1024 * 1024 * 1024, expected: "1.0TiB"},
	}

	for _, test := range tests {
		if got := formatFileSize(float64(test.size)); got != test.expected {
			t.Errorf("expected %s, got %s", test.expected, got)
		}
	}
}
