package main

import (
	"bufio"
	"errors"
	"flag"
	"fmt"
	"os"
	"strings"
	"time"

	"boscoin.io/sebak/lib/common"
	"boscoin.io/sebak/lib/common/keypair"
	"boscoin.io/sebak/lib/network"
	"boscoin.io/sebak/lib/node"
	"boscoin.io/sebak/lib/node/runner/api"
	"boscoin.io/sebak/lib/transaction"
	"boscoin.io/sebak/lib/transaction/operation"
	logging "github.com/inconshreveable/log15"
)

const (
	defaultLogLevel        logging.Lvl = logging.LvlInfo
	defaultLogFormat       string      = "terminal"
	defaultRequestTimeout  string      = "30s"
	defaultConfirmDuration string      = "60s"
	defaultOperationsLimit int         = 800
)

var (
	flagSEBAKEndpoint   string = "http://127.0.0.1:12345"
	flagLogLevel        string = defaultLogLevel.String()
	flagLogFormat       string = defaultLogFormat
	flagLog             string
	flagRequestTimeout  string = defaultRequestTimeout
	flagConfirmDuration string = defaultConfirmDuration
)

var (
	networkID       []byte
	endpoint        *common.Endpoint
	logLevel        logging.Lvl
	log             logging.Logger = logging.New("module", "sebak-create-accounts")
	kp              *keypair.Full
	requestTimeout  time.Duration
	confirmDuration time.Duration
	accounts        map[string]Account
	totalBalance    common.Amount
	client          *network.HTTP2NetworkClient
	operationsLimit int = defaultOperationsLimit
)

type Account struct {
	address string
	balance common.Amount
}

type BlockAccount struct {
	Address    string        `json:"address"`
	Balance    common.Amount `json:"balance"`
	SequenceID uint64        `json:"sequence_id"`
}

func printError(s string, err error) {
	var errString string
	if err != nil {
		errString = err.Error()
	}

	if len(s) > 0 {
		fmt.Println("error:", s, "", errString)
	}
	fmt.Fprintf(os.Stderr, "Usage: %s <secret seed> <accounts>\n", os.Args[0])

	flag.PrintDefaults()
	os.Exit(2)
}

func init() {
	flag.StringVar(&flagSEBAKEndpoint, "sebak", flagSEBAKEndpoint, "sebak endpoint")
	flag.StringVar(&flagLogLevel, "log-level", flagLogLevel, "log level, {crit, error, warn, info, debug}")
	flag.StringVar(&flagLogFormat, "log-format", flagLogFormat, "log format, {terminal, json}")
	flag.StringVar(&flagLog, "log", flagLog, "set log file")
	flag.StringVar(&flagRequestTimeout, "request-timeout", flagRequestTimeout, "timeout for requests")
	flag.StringVar(&flagConfirmDuration, "confirm-duration", flagConfirmDuration, "duration for checking transaction confirmed")
	flag.IntVar(&operationsLimit, "limit", operationsLimit, "operations in one transaction")

	flag.Parse()
	if flag.NArg() < 2 {
		printError("empty input", nil)
	}

	{
		kpKP, err := keypair.Parse(flag.Arg(0))
		if err != nil {
			printError("invalid <secret seed>", err)
		}
		var ok bool
		if kp, ok = kpKP.(*keypair.Full); !ok {
			printError("invalid <secret seed>", err)
		}
	}

	{
		var err error

		accounts = map[string]Account{}
		f, err := os.Open(flag.Arg(1))
		if err != nil {
			printError("failed to read file", err)
		}
		defer f.Close()

		var address string
		var balance common.Amount

		scanner := bufio.NewScanner(f)
		for scanner.Scan() {
			s := scanner.Text()
			if strings.HasPrefix(s, "accountid,balance,seqnum,numsubentries,") {
				continue
			}
			sl := strings.SplitN(s, ",", 3)
			address = sl[0]

			if len(sl) < 2 {
				printError("invalid line found", fmt.Errorf(s))
			} else if !strings.HasPrefix(address, "G") {
				printError("invalid line found", fmt.Errorf("'<public address>,<balance>', but %s", s))
			} else if _, err := keypair.Parse(address); err != nil {
				printError("invalid public address found", err)
			} else if _, ok := accounts[address]; ok {
				printError("duplicated public address found", fmt.Errorf(address))
			} else if address == kp.Address() {
				printError("duplicated public address found with secret seed", fmt.Errorf(address))
			} else if balance, err = common.AmountFromString(sl[1]); err != nil {
				printError("invalid balance found", err)
			}

			accounts[address] = Account{address: address, balance: balance}
			totalBalance = totalBalance.MustAdd(balance)
		}

		if err := scanner.Err(); err != nil {
			printError("failed to read file", err)
		}
	}

	{
		var err error
		if endpoint, err = common.ParseEndpoint(flagSEBAKEndpoint); err != nil {
			printError("--sebak", err)
		}
	}

	{
		var err error
		if len(flagRequestTimeout) < 1 {
			printError("--request-timeout", errors.New("must be given"))
		} else if requestTimeout, err = time.ParseDuration(flagRequestTimeout); err != nil {
			printError("--request-timeout", err)
		}
	}

	{
		var err error
		if len(flagConfirmDuration) < 1 {
			printError("--confirm-duration", errors.New("must be given"))
		} else if confirmDuration, err = time.ParseDuration(flagConfirmDuration); err != nil {
			printError("--confirm-duration", err)
		}
	}

	if operationsLimit < 1 {
		printError("--limit should be over 1", nil)
	}

	{
		var err error
		var connection *common.HTTP2Client

		// Keep-alive ignores timeout/idle timeout
		if connection, err = common.NewHTTP2Client(0, 0, true); err != nil {
			printError("Error while creating network client", err)
		}
		client = network.NewHTTP2NetworkClient(endpoint, connection)

		resp, err := client.Get("/")
		if err != nil {
			printError("failed to connect sebak", err)
		}

		var nodeInfo node.NodeInfo
		if nodeInfo, err = node.NewNodeInfoFromJSON(resp); err != nil {
			printError("failed to parse node info response", err)
		}
		networkID = []byte(nodeInfo.Policy.NetworkID)
	}

	parsedFlags := []interface{}{}
	parsedFlags = append(parsedFlags, "\n\tsebak", endpoint)
	parsedFlags = append(parsedFlags, "\n\tlog-level", flagLogLevel)
	parsedFlags = append(parsedFlags, "\n\tlog-format", flagLogFormat)
	parsedFlags = append(parsedFlags, "\n\tlog", flagLog)
	parsedFlags = append(parsedFlags, "\n\trequest-timeout", flagRequestTimeout)
	parsedFlags = append(parsedFlags, "\n\tconfirm-duration", flagConfirmDuration)
	parsedFlags = append(parsedFlags, "\n\taccounts", len(accounts))
	parsedFlags = append(parsedFlags, "\n\ttotal-balance", totalBalance)
	parsedFlags = append(parsedFlags, "\n\tnetwork-id", string(networkID))
	parsedFlags = append(parsedFlags, "\n\tops", operationsLimit)
	parsedFlags = append(parsedFlags, "\n", "")

	log.Debug("parsed flags:", parsedFlags...)

}

func getAccount(address string) (ac BlockAccount, err error) {
	url := fmt.Sprintf("%s/%s/accounts/%s", network.UrlPathPrefixAPI, api.APIVersionV1, address)
	log_ := log.New(logging.Ctx{"m": "get-account", "uid": common.GenerateUUID()})

	log_.Debug("starting", "url", url)

	var b []byte
	for i := 0; i < 3; i++ {
		if b, err = client.Get(url); err != nil {
			log_.Error("failed", "error", err)
			continue
		}
		err = nil
		break
	}
	if err != nil {
		log_.Error("failed to get account", "error", err)
		return
	}

	if err = common.DecodeJSONValue(b, &ac); err != nil {
		log_.Error("failed parse", "error", err)
		return
	}

	log_.Debug("success", "account", ac)
	return
}

func sendTransaction(tx transaction.Transaction) (err error) {
	log_ := log.New(logging.Ctx{"m": "sendTransaction", "uid": common.GenerateUUID()})

	if _, err = tx.Serialize(); err != nil {
		return
	}

	var b []byte
	retries := 3
	for i := 0; i < 3; i++ { // retry
		if b, err = client.SendTransaction(tx); err != nil {
			log_.Error("failed to send transaction", err)
			if i == retries-1 {
				break
			}
		}
		break
	}

	if err != nil {
		log_.Error("response", "body", string(b), "error", err, "error-type", fmt.Sprintf("%T", err))
	} else {
		log_.Debug("response", "body", string(b))
	}

	return
}

func checkTransaction(hash string) (err error) {
	var log_ logging.Logger
	log_ = log.New(logging.Ctx{"m": "checkTransaction", "uid": common.GenerateUUID(), "hash": hash})

	url := fmt.Sprintf("%s/%s/transactions/%s", network.UrlPathPrefixAPI, api.APIVersionV1, hash)
	log_.Debug("starting", "url", url)

	if _, err = client.Get(url); err != nil {
		log_.Error("failed", "error", err)
		return
	}

	log_.Debug("success")
	return
}

func createAccount(targets []Account) (err error) {
	log_ := log.New(logging.Ctx{"m": "create-accounts", "uid": common.GenerateUUID()})

	// create accounts
	defer func(l logging.Logger) {
		log_.Debug(
			"done",
			"error", err,
		)
	}(log_)

	log_.Debug(
		"starting",
		"source", kp.Address(),
		"target", targets,
	)

	var ac BlockAccount
	if ac, err = getAccount(kp.Address()); err != nil {
		log_.Error(err.Error())
		return
	}
	sequenceID := ac.SequenceID

	var ops []operation.Operation
	for _, account := range targets {
		op, _ := operation.NewOperation(operation.CreateAccount{
			Target: account.address,
			Amount: account.balance,
		})
		ops = append(ops, op)
	}

	var tx transaction.Transaction
	if tx, err = transaction.NewTransaction(kp.Address(), sequenceID, ops...); err != nil {
		log_.Error(err.Error())
		return
	}

	tx.Sign(kp, networkID)
	log_.Debug("transaction created", "transaction", tx.GetHash())

	if err = sendTransaction(tx); err != nil {
		log_.Error("failed to send transaction", "error", err)
		return
	}

	// check transaction is stored in block
	for {
		if err = checkTransaction(tx.GetHash()); err == nil {
			break
		}
		err = nil
		time.Sleep(time.Duration(600) * time.Millisecond)
	}

	log_.Debug(
		"transaction confirmed",
		"confirmed transaction", tx.GetHash(),
	)

	return
}

func main() {
	var targets []Account
	var i int
	for _, account := range accounts {
		targets = append(targets, account)
		if len(targets) == operationsLimit || i == len(accounts)-1 {
			if err := createAccount(targets); err != nil {
				printError("failed to create accounts", err)
			}
			targets = []Account{}
		}
		i++
	}

	// check accounts
	for _, account := range accounts {
		ac, err := getAccount(account.address)
		if err != nil {
			log.Error("account was not created", "address", account.address)
			continue
		}
		if ac.Balance != account.balance {
			log.Error(
				"balance is different",
				"address", account.address,
				"expected", account.balance,
				"created", ac.Balance,
			)
			continue
		}
		log.Info("checked", "address", account.address, "balance", ac.Balance)
	}

	log.Info("accounts created", "count", len(accounts))
}
