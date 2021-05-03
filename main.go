package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/pilebones/go-udev/crawler"
	"github.com/pilebones/go-udev/netlink"

	"github.com/kr/pretty"
)

var (
	verbose         bool
	matcherFilePath string = "./data/usb-matcher.json"
)

func init() {
	flag.BoolVar(&verboseMode, "verbose", false, "verbose output")
}

func main() {
	flag.Parse()
	stream, err := ioutil.ReadFile(*filePath)
	if err != nil {
		log.Fatalf("error: %s\n", err)
	}
	var rules netlink.RuleDefinition
	if err := json.Unmarshal(stream, &rules); err != nil {
		return nil, fmt.Errorf("Wrong rule syntax, err: %w", err)
	}
	monitor(rules)
}

// monitor run monitor mode
func monitor(matcher netlink.Matcher) {
	log.Println("Monitoring UEvent kernel message to user-space...")

	conn := new(netlink.UEventConn)
	if err := conn.Connect(netlink.UdevEvent); err != nil {
		log.Fatalln("Unable to connect to Netlink Kobject UEvent socket")
	}
	defer conn.Close()

	queue := make(chan netlink.UEvent)
	errors := make(chan error)
	quit := conn.Monitor(queue, errors, matcher)

	// Signal handler to quit properly monitor mode
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT)
	go func() {
		<-signals
		log.Println("Exiting monitor mode...")
		close(quit)
		os.Exit(0)
	}()

	// Handling message from queue
	for {
		select {
		case uevent := <-queue:
			log.Println("Handle", pretty.Sprint(uevent))
		case err := <-errors:
			log.Println("ERROR:", err)
		}
	}

}
