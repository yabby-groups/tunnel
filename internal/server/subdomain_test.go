package server

import "testing"

func TestValidSubdomainRequiresAtLeastFiveCharacters(t *testing.T) {
	for _, value := range []string{"a", "abcd", "-abcde", "abcde-", "abc_de"} {
		if validSubdomain(value) {
			t.Fatalf("validSubdomain(%q) = true, want false", value)
		}
	}
	if !validSubdomain("abcde") {
		t.Fatal("validSubdomain(\"abcde\") = false, want true")
	}
}
