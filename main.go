package main

import (
	"fmt"
	"os"
)

func main() {
    if len(os.Args) < 2 {
      fmt.Println("Please provide an argument.")
      return
    }
    command := os.Args[1]
    options := os.Args[2:]

    switch command {
    case "daemon":
      fmt.Println("Daemon")
    case "start":
    	fmt.Println("Start")
     fmt.Println(options)
    case "stop":
     	fmt.Println("Stop")
    case "restart":
      fmt.Println("Restart")
    case "status":
    	fmt.Println("Status")
    case "help":
    	fmt.Println("Help")
    default:
      fmt.Println("Sorrry, but '" + command + "' is not valid. Type 'help' to get a list")
    }
}
