package main

import (
	"os"

	"github.com/ticketing-platform/notification-service/internal/app"
)

func main() {
	os.Exit(run(os.Args[1:]))
}

func run(args []string) int {
	return app.Run(args, os.Stdout, os.Stderr)
}
