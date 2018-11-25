package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
	"os"
	"strings"

	termutil "github.com/andrew-d/go-termutil"
	"github.com/stellar/go/keypair"

	"boscoin.io/sebak/lib/transaction"
)

var (
	flagNetworkID string = "sebak-test-network"
	flagSilent    bool   = false
)

func init() {
	flag.StringVar(&flagNetworkID, "network-id", flagNetworkID, "network-id")
	flag.BoolVar(&flagSilent, "s", flagSilent, "silence!")
	flag.Parse()
}

func printError(s string, err error) {
	if flagSilent {
		os.Exit(1)
	}

	var errString string
	if err != nil {
		errString = err.Error()
	}

	if len(s) > 0 {
		fmt.Println("error:", s, "", errString)
	}
	fmt.Fprintf(os.Stderr, "Usage: %s <secret seed>\n", os.Args[0])

	flag.PrintDefaults()
	os.Exit(2)
}

func main() {
	var err error
	var kp keypair.KP
	if kp, err = keypair.Parse(flag.Arg(0)); err != nil {
		printError("<sender's secret seed>:", err)
	}

	var senderKP *keypair.Full
	var ok bool
	if senderKP, ok = kp.(*keypair.Full); !ok {
		printError("<sender's secret seed>: is not secret seed", nil)
	}

	var message []byte
	if flag.NArg() > 1 {
		message = []byte(strings.Join(flag.Args()[1:], " "))
	} else {
		if !termutil.Isatty(os.Stdin.Fd()) {
			message, _ = ioutil.ReadAll(os.Stdin)
		} else {
			printError("empty input", nil)
		}
	}

	fmt.Println("< original ======================================================================")
	fmt.Println(strings.TrimSpace(string(message)))
	fmt.Println("> signed ========================================================================")

	var tx transaction.Transaction
	if err = json.Unmarshal(message, &tx); err != nil {
		printError("<message>", err)
		return
	}

	tx.B.Source = senderKP.Address()
	tx.Sign(senderKP, []byte(flagNetworkID))

	fmt.Println(tx)

	networkID := []byte(flagNetworkID)
	networkID = append(networkID, []byte(tx.B.MakeHashString())...)
	fmt.Println("network-id", []byte(flagNetworkID))
	fmt.Println("sig data", networkID)
	fmt.Println("hash", tx.B.MakeHashString())

	os.Exit(0)
}
