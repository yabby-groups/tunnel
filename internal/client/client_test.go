package client

import "testing"

func TestNormalizeLocalURL(t *testing.T) {
	tests := []struct {
		name    string
		raw     string
		want    string
		wantErr bool
	}{
		{name: "port", raw: "3000", want: "http://127.0.0.1:3000"},
		{name: "http URL", raw: "http://localhost:8080/app", want: "http://localhost:8080/app"},
		{name: "https URL", raw: "https://localhost:8443", want: "https://localhost:8443"},
		{name: "unsupported scheme", raw: "ftp://localhost:21", wantErr: true},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got, err := normalizeLocalURL(test.raw)
			if test.wantErr {
				if err == nil {
					t.Fatal("normalizeLocalURL() error = nil, want error")
				}
				return
			}
			if err != nil {
				t.Fatalf("normalizeLocalURL() error = %v", err)
			}
			if got.String() != test.want {
				t.Fatalf("normalizeLocalURL() = %q, want %q", got, test.want)
			}
		})
	}
}
