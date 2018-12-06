package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
	"os"
	"strings"

	termutil "github.com/andrew-d/go-termutil"
	"github.com/ethereum/go-ethereum/rlp"

	"boscoin.io/sebak/lib/common"
	"boscoin.io/sebak/lib/transaction"
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
	var err error
	var message []byte
	if flag.NArg() > 0 {
		message = []byte(strings.Join(flag.Args(), " "))
	} else {
		if !termutil.Isatty(os.Stdin.Fd()) {
			message, _ = ioutil.ReadAll(os.Stdin)
		} else {
			printError("empty input", nil)
		}
	}

	var tx transaction.Transaction
	if err = json.Unmarshal(message, &tx); err != nil {
		printError("<message>", err)
		return
	}

	e, _ := rlp.EncodeToBytes(tx.B)
	fmt.Println("   rlp:", e)
	c := common.MakeHash(e)
	fmt.Println("sha256:", c)

	fmt.Println("base58:", tx.B.MakeHashString())

	os.Exit(0)
}
