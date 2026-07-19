package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	"github.com/myna-server/tunnel/internal/client"
)

type credentials struct {
	Token string `json:"token"`
}

func main() {
	if len(os.Args) < 2 {
		usage()
		os.Exit(2)
	}
	switch os.Args[1] {
	case "login":
		login(os.Args[2:])
	case "http":
		runHTTP(os.Args[2:])
	default:
		usage()
		os.Exit(2)
	}
}

func login(args []string) {
	flags := flag.NewFlagSet("login", flag.ExitOnError)
	controlURL := flags.String("control-url", "", "myna control-plane base URL")
	flags.Parse(args)
	if *controlURL == "" {
		fmt.Fprintln(os.Stderr, "-control-url is required")
		os.Exit(2)
	}
	token, err := client.Login(context.Background(), *controlURL, func(device client.DeviceAuthorization) {
		fmt.Printf("Open %s and enter code %s\n", device.VerificationURI, device.UserCode)
	})
	if err != nil {
		fmt.Fprintln(os.Stderr, "login failed:", err)
		os.Exit(1)
	}
	if err := saveCredentials(credentials{Token: token}); err != nil {
		fmt.Fprintln(os.Stderr, "save credentials:", err)
		os.Exit(1)
	}
	fmt.Println("Tunnel login complete.")
}

func runHTTP(args []string) {
	flags := flag.NewFlagSet("http", flag.ExitOnError)
	serverURL := flags.String("server", "", "wss:// tunnel server /connect URL")
	token := flags.String("token", os.Getenv("TUNNEL_TOKEN"), "tunnel credential")
	flags.Parse(args)
	if *serverURL == "" || flags.NArg() != 1 {
		fmt.Fprintln(os.Stderr, "usage: tunnel http -server wss://tunnel.example.com/connect <port|url>")
		os.Exit(2)
	}
	if *token == "" {
		saved, err := loadCredentials()
		if err != nil {
			fmt.Fprintln(os.Stderr, "read credentials:", err)
			os.Exit(1)
		}
		*token = saved.Token
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	tunnel := client.Tunnel{
		ServerURL: *serverURL, Token: *token, LocalURL: flags.Arg(0),
		OnURL: func(url string) { fmt.Println("Forwarding", url) },
		OnRequest: func(method, path string, status int, elapsed time.Duration) {
			fmt.Printf("%s %s -> %d (%s)\n", method, path, status, elapsed.Round(time.Millisecond))
		},
	}
	if err := tunnel.Run(ctx); err != nil {
		fmt.Fprintln(os.Stderr, "tunnel:", err)
		os.Exit(1)
	}
}

func credentialsPath() (string, error) {
	dir, err := os.UserConfigDir()
	if err != nil {
		return "", err
	}
	return filepath.Join(dir, "myna-tunnel", "credentials.json"), nil
}

func saveCredentials(value credentials) error {
	path, err := credentialsPath()
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
		return err
	}
	raw, err := json.Marshal(value)
	if err != nil {
		return err
	}
	return os.WriteFile(path, raw, 0600)
}

func loadCredentials() (credentials, error) {
	path, err := credentialsPath()
	if err != nil {
		return credentials{}, err
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		return credentials{}, err
	}
	var value credentials
	if err := json.Unmarshal(raw, &value); err != nil {
		return credentials{}, err
	}
	if value.Token == "" {
		return credentials{}, fmt.Errorf("saved credential is empty")
	}
	return value, nil
}

func usage() {
	fmt.Fprintln(os.Stderr, "usage: tunnel login|http")
}
