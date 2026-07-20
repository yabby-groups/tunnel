package main

import (
	"flag"
	"log"
	"net/http"
	"time"

	"github.com/yabby-groups/tunnel/internal/server"
)

func main() {
	listen := flag.String("listen", ":8080", "HTTP listen address")
	baseDomain := flag.String("base-domain", "", "public tunnel base domain, e.g. tunnel.example.com")
	controlURL := flag.String("control-url", "", "myna credential validation endpoint")
	devToken := flag.String("dev-token", "", "development-only static bearer token")
	flag.Parse()

	var auth server.Authenticator
	if *devToken != "" {
		auth = server.StaticAuthenticator{Token: *devToken}
	} else if *controlURL != "" {
		auth = server.HTTPAuthenticator{URL: *controlURL}
	} else {
		log.Fatal("set -control-url for production or -dev-token for local development")
	}
	s, err := server.New(server.Config{
		BaseDomain: *baseDomain, RequestTimeout: 60 * time.Second,
	}, auth)
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("tunnel server listening on %s for *.%s", *listen, *baseDomain)
	log.Fatal(http.ListenAndServe(*listen, s.Handler()))
}
