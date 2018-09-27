package main

import (
	"flag"
	"fmt"
	"io/ioutil"
	"os"
	"strings"

	termutil "github.com/andrew-d/go-termutil"
	"github.com/btcsuite/btcutil/base58"
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

func printError(s string) {
	if flagSilent {
		os.Exit(1)
	}

	if len(s) > 0 {
		fmt.Println("error:", s)
	}
	fmt.Fprintf(os.Stderr, "Usage: %s <message> \n", os.Args[0])

	flag.PrintDefaults()
	os.Exit(2)
}

func main() {
	var message []byte
	if flag.NArg() > 0 {
		message = []byte(strings.Join(flag.Args(), " "))
	} else {
		if !termutil.Isatty(os.Stdin.Fd()) {
			message, _ = ioutil.ReadAll(os.Stdin)
		} else {
			printError("empty input")
		}
	}

	var err error
	var tx transaction.Transaction
	if tx, err = transaction.NewTransactionFromJSON(message); err != nil {
		printError(err.Error())
	}

	var kp keypair.KP
	if kp, err = keypair.Parse(tx.B.Source); err != nil {
		printError(err.Error())
	}

	err = kp.Verify(
		append([]byte(flagNetworkID), []byte(tx.H.Hash)...),
		base58.Decode(tx.H.Signature),
	)
	if err != nil {
		if flagSilent {
			os.Exit(1)
		}

		fmt.Println("failed:", err)
		os.Exit(1)
	}

	if flagSilent {
		os.Exit(0)
	}

	fmt.Println("verified")
	os.Exit(0)
}
