package main

import (
	"flag"
	"fmt"
	"os"

	"github.com/ethereum/go-ethereum/rlp"
	"github.com/stellar/go/keypair"

	"boscoin.io/sebak/lib/common"
	"boscoin.io/sebak/lib/transaction"
	"boscoin.io/sebak/lib/transaction/operation"
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
	fmt.Fprintf(os.Stderr, "Usage: %s <sender's secret seed> <receiver's public address> [<amount>]\n", os.Args[0])

	flag.PrintDefaults()
	os.Exit(2)
}

func main() {
	var senderKP *keypair.Full
	if flag.NArg() < 2 {
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

	amount := common.BaseReserve
	if flag.NArg() == 3 {
		if amount, err = common.AmountFromString(flag.Arg(2)); err != nil {
			printError("<amount>:", err)
		}
	}

	opb := operation.NewCreateAccount(receiverKP.Address(), amount, "")
	op := operation.Operation{
		H: operation.Header{Type: operation.TypeCreateAccount},
		B: opb,
	}

	tx, err := transaction.NewTransaction(senderKP.Address(), 0, op)

	if err != nil {
		printError("", err)
	}
	tx.Sign(senderKP, []byte(flagNetworkID))

	networkID := []byte(flagNetworkID)
	networkID = append(networkID, []byte(tx.B.MakeHashString())...)
	fmt.Println("network-id", []byte(flagNetworkID))
	e, _ := rlp.EncodeToBytes(tx.B)
	fmt.Println("   rlp:", e)
	fmt.Println("sig data", networkID)
	fmt.Println("hash", tx.B.MakeHashString())
	fmt.Println(tx)

	os.Exit(0)
}
