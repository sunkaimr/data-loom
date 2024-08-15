package main

import (
	"github.com/sunkaimr/data-loom/cmd"
)

// @title           data-loom API
// @version         v1.0
// @description     data-loom.
// @schemes http https
// @host localhost:8080
// @BasePath /data-loom/api/v1
func main() {
	cmd.Execute()
}
