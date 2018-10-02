package main

import (
	"flag"
	"fmt"
	"os"

	"github.com/stellar/go/keypair"

	"boscoin.io/sebak/lib/common"
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
	fmt.Fprintf(os.Stderr, "Usage: %s <sender's secret seed> <receiver's public address> <amount>\n", os.Args[0])

	flag.PrintDefaults()
	os.Exit(2)
}

func main() {
	var senderKP *keypair.Full
	if flag.NArg() != 3 {
		printError("empty input", nil)
	}

	var err error
	var kp keypair.KP
	var ok bool
	if kp, err = keypair.Parse(flag.Arg(0)); err != nil {
		printError("<sender's secret seed>:", err)
	} else if senderKP, ok = kp.(*keypair.Full); !ok {
		printError("<sender's secret seed>: is not secret seed", nil)
	}

	var receiverKP keypair.KP
	if receiverKP, err = keypair.Parse(flag.Arg(1)); err != nil {
		printError("<receiver's public address>:", err)
	}

	var amount common.Amount
	if amount, err = common.AmountFromString(flag.Arg(2)); err != nil {
		printError("<amount>:", err)
	}

	opb := transaction.NewOperationBodyCreateAccount(receiverKP.Address(), amount, "")
	op := transaction.Operation{
		H: transaction.OperationHeader{Type: transaction.OperationCreateAccount},
		B: opb,
	}

	ops := []transaction.Operation{op}

	var sequenceID uint64
	txBody := transaction.TransactionBody{
		Source:     senderKP.Address(),
		Fee:        common.BaseFee.MustMult(len(ops)),
		SequenceID: sequenceID,
		Operations: ops,
	}

	tx := transaction.Transaction{
		T: "transaction",
		H: transaction.TransactionHeader{
			Created: common.NowISO8601(),
			Hash:    txBody.MakeHashString(),
		},
		B: txBody,
	}
	tx.Sign(senderKP, []byte(flagNetworkID))

	fmt.Println(tx)

	os.Exit(0)
}
