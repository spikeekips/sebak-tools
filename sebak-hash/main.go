package main

import (
	"flag"
	"fmt"
	"io/ioutil"
	"os"

	"boscoin.io/sebak/lib/common"
	"github.com/btcsuite/btcutil/base58"
)

func init() {
	flag.Parse()
}

func printError(s string, err error) {
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
	if flag.NArg() < 1 {
		printError("failed to open file", nil)
	}

	message, err := ioutil.ReadFile(flag.Arg(0))
	if err != nil {
		printError("failed to open file", err)
	}

	fmt.Println(base58.Encode((common.MustMakeObjectHash(message))))

	os.Exit(0)
}
