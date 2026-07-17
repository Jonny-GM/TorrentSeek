package main

import "testing"

func TestRequiresToken(t *testing.T) {
	open := []string{"127.0.0.1:3480", "127.0.0.5:80", "localhost:3480", "[::1]:3480"}
	for _, addr := range open {
		if requiresToken(addr) {
			t.Errorf("requiresToken(%q) = true, want false (loopback)", addr)
		}
	}
	protected := []string{"0.0.0.0:3480", ":3480", "[::]:3480", "192.168.1.5:3480", "10.0.0.1:80", "example.com:3480", "garbage"}
	for _, addr := range protected {
		if !requiresToken(addr) {
			t.Errorf("requiresToken(%q) = false, want true", addr)
		}
	}
}
