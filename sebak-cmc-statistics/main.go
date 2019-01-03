package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"

	"boscoin.io/sebak/lib/block"
	"boscoin.io/sebak/lib/common"
	"boscoin.io/sebak/lib/network"
	"boscoin.io/sebak/lib/node"
	"boscoin.io/sebak/lib/node/runner"
	"boscoin.io/sebak/lib/transaction"
	"boscoin.io/sebak/lib/transaction/operation"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	jsonrpc "github.com/gorilla/rpc/json"
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
	flagSEBAKJSONRPC    string = "http://127.0.0.1:54321/jsonrpc"
	flagLogLevel        string = defaultLogLevel.String()
	flagLogFormat       string = defaultLogFormat
	flagLog             string
	flagS3Region        string = "ap-northeast-2"
	flagS3Bucket        string
	flagS3Path          string
	flagS3ACL           string = "public-read"
	flagAWSAccessKeyID  string
	flagAWSSecretKey    string
	flagTopHoldersLimit int = 3000
)

var (
	flags           *flag.FlagSet
	networkID       []byte
	endpoint        *common.Endpoint
	jsonrpcEndpoint *common.Endpoint
	logLevel        logging.Lvl
	log             logging.Logger = logging.New("module", "sebak-cmc-statistics")
	client          *network.HTTP2NetworkClient
	awsSession      *session.Session
	nodeInfo        node.NodeInfo

	latestBlockFile    string
	totalInflationFile string
	totalSupplyFile    string
	totalHoldersFile   string
)

func printError(s string, err error) {
	var errString string
	if err != nil {
		errString = err.Error()
	}

	if len(s) > 0 {
		fmt.Println("error:", s, "", errString)
	}

	os.Exit(1)
}

func printFlagError(s string, err error) {
	var errString string
	if err != nil {
		errString = err.Error()
	}

	if len(s) > 0 {
		fmt.Println("error:", s, "", errString)
	}
	fmt.Fprintf(os.Stderr, "Usage: %s <secret seed> <accounts>\n", os.Args[0])

	flag.PrintDefaults()
	os.Exit(1)
}

func init() {
	flags = flag.NewFlagSet(os.Args[0], flag.ExitOnError)
	flags.StringVar(&flagSEBAKEndpoint, "sebak", flagSEBAKEndpoint, "sebak endpoint")
	flags.StringVar(&flagSEBAKJSONRPC, "sebak-jsonrpc", flagSEBAKJSONRPC, "sebak jsonrpc")
	flags.StringVar(&flagLogLevel, "log-level", flagLogLevel, "log level, {crit, error, warn, info, debug}")
	flags.StringVar(&flagLogFormat, "log-format", flagLogFormat, "log format, {terminal, json}")
	flags.StringVar(&flagLog, "log", flagLog, "set log file")
	flags.IntVar(&flagTopHoldersLimit, "top-holders-limit", flagTopHoldersLimit, "limit for number of top holders")
	flags.StringVar(&flagS3Region, "region", flagS3Region, "s3 region")
	flags.StringVar(&flagAWSAccessKeyID, "aws-access-key", flagAWSAccessKeyID, "aws access key")
	flags.StringVar(&flagAWSSecretKey, "aws-secret-key", flagAWSSecretKey, "aws secret key")
	flags.StringVar(&flagS3Bucket, "s3-bucket", flagS3Bucket, "s3 bucket name")
	flags.StringVar(&flagS3Path, "s3-path", flagS3Path, "s3 file path")
	flags.StringVar(&flagS3ACL, "s3-acl", flagS3ACL, "s3 acl; {public-read}")

	flags.Parse(os.Args[1:])

	if len(flagS3Bucket) < 1 {
		printFlagError("--s3-bucket", fmt.Errorf("must be given"))
	}

	{
		os.Setenv("AWS_ACCESS_KEY_ID", flagAWSAccessKeyID)
		os.Setenv("AWS_SECRET_ACCESS_KEY", flagAWSSecretKey)

		var err error
		if awsSession, err = session.NewSession(&aws.Config{Region: aws.String(flagS3Region)}); err != nil {
			printFlagError("failed to access aws s3", err)
		}
	}

	{
		var err error
		if endpoint, err = common.ParseEndpoint(flagSEBAKEndpoint); err != nil {
			printFlagError("--sebak", err)
		}
		if jsonrpcEndpoint, err = common.ParseEndpoint(flagSEBAKJSONRPC); err != nil {
			printFlagError("--sebak-jsonrpc", err)
		}
	}

	{
		var err error
		var connection *common.HTTP2Client

		// Keep-alive ignores timeout/idle timeout
		if connection, err = common.NewHTTP2Client(0, 0, true); err != nil {
			printFlagError("Error while creating network client", err)
		}
		client = network.NewHTTP2NetworkClient(endpoint, connection)

		resp, err := client.Get("/")
		if err != nil {
			printFlagError("failed to connect sebak", err)
		}

		if nodeInfo, err = node.NewNodeInfoFromJSON(resp); err != nil {
			printFlagError("failed to parse node info response", err)
		}
		networkID = []byte(nodeInfo.Policy.NetworkID)
	}

	parsedFlags := []interface{}{}
	parsedFlags = append(parsedFlags, "\n\tsebak", endpoint)
	parsedFlags = append(parsedFlags, "\n\tsebak-jsonrpc", jsonrpcEndpoint)
	parsedFlags = append(parsedFlags, "\n\tlog-level", flagLogLevel)
	parsedFlags = append(parsedFlags, "\n\tlog-format", flagLogFormat)
	parsedFlags = append(parsedFlags, "\n\tlog", flagLog)
	parsedFlags = append(parsedFlags, "\n\tnetwork-id", string(networkID))
	parsedFlags = append(parsedFlags, "\n\ttop-holders-limit", flagTopHoldersLimit)
	parsedFlags = append(parsedFlags, "\n\ts3Bucket", flagS3Bucket)
	parsedFlags = append(parsedFlags, "\n\ts3-path", flagS3Path)
	parsedFlags = append(parsedFlags, "\n\ts3-path", flagS3ACL)
	parsedFlags = append(parsedFlags, "\n\ts3-region", flagS3Region)

	log.Debug("parsed flags:", parsedFlags...)

	latestBlockFile = filepath.Join(flagS3Path, "latest-block.txt")
	totalInflationFile = filepath.Join(flagS3Path, "total-inflation.txt")
	totalSupplyFile = filepath.Join(flagS3Path, "total-supply.txt")
	totalHoldersFile = filepath.Join(flagS3Path, "top-holders%s.txt")
}

func getAccounts(cursor []byte) (result runner.DBGetIteratorResult, err error) {
	args := runner.DBGetIteratorArgs{
		Prefix: common.BlockAccountPrefixAddress,
		Options: runner.GetIteratorOptions{
			Limit:  runner.MaxLimitListOptions,
			Cursor: cursor,
		},
	}

	var message []byte
	if message, err = jsonrpc.EncodeClientRequest("DB.GetIterator", &args); err != nil {
		return
	}

	var req *http.Request
	if req, err = http.NewRequest("POST", jsonrpcEndpoint.String(), bytes.NewBuffer(message)); err != nil {
		return
	}

	req.Header.Set("Content-Type", "application/json")
	client := new(http.Client)

	var resp *http.Response
	if resp, err = client.Do(req); err != nil {
		return
	}
	defer resp.Body.Close()

	if err = jsonrpc.DecodeClientResponse(resp.Body, &result); err != nil {
		return
	}

	return
}

func getBlocks(cursor []byte) (result runner.DBGetIteratorResult, err error) {
	args := runner.DBGetIteratorArgs{
		Prefix: common.BlockPrefixHeight,
		Options: runner.GetIteratorOptions{
			Limit:   runner.MaxLimitListOptions,
			Cursor:  cursor,
			Reverse: false,
		},
	}

	var message []byte
	if message, err = jsonrpc.EncodeClientRequest("DB.GetIterator", &args); err != nil {
		return
	}

	var req *http.Request
	if req, err = http.NewRequest("POST", jsonrpcEndpoint.String(), bytes.NewBuffer(message)); err != nil {
		return
	}

	req.Header.Set("Content-Type", "application/json")
	client := new(http.Client)

	var resp *http.Response
	if resp, err = client.Do(req); err != nil {
		return
	}
	defer resp.Body.Close()

	if err = jsonrpc.DecodeClientResponse(resp.Body, &result); err != nil {
		return
	}

	return
}

func getDB(key string) (result runner.DBGetResult, err error) {
	args := runner.DBGetArgs(key)
	var message []byte
	if message, err = jsonrpc.EncodeClientRequest("DB.Get", &args); err != nil {
		return
	}

	var req *http.Request
	if req, err = http.NewRequest("POST", jsonrpcEndpoint.String(), bytes.NewBuffer(message)); err != nil {
		return
	}

	req.Header.Set("Content-Type", "application/json")
	client := new(http.Client)

	var resp *http.Response
	if resp, err = client.Do(req); err != nil {
		return
	}
	defer resp.Body.Close()

	if err = jsonrpc.DecodeClientResponse(resp.Body, &result); err != nil {
		return
	}

	return
}

func getBlock(hash string) (blk block.Block, err error) {
	var result runner.DBGetResult
	if result, err = getDB(fmt.Sprintf("%s%s", common.BlockPrefixHash, hash)); err != nil {
		return
	}

	err = json.Unmarshal(result.Value, &blk)
	return
}

func getTransaction(hash string) (tx transaction.Transaction, err error) {
	var result runner.DBGetResult
	if result, err = getDB(fmt.Sprintf("%s%s", common.TransactionPoolPrefix, hash)); err != nil {
		return
	}

	var tp block.TransactionPool
	if err = json.Unmarshal(result.Value, &tp); err != nil {
		return
	}

	if err = json.Unmarshal(tp.Message, &tx); err != nil {
		return
	}
	return
}

func downloadS3(key string) ([]byte, error) {
	downloader := s3manager.NewDownloader(awsSession)

	w := aws.NewWriteAtBuffer([]byte{})

	_, err := downloader.Download(
		w,
		&s3.GetObjectInput{
			Bucket: aws.String(flagS3Bucket),
			Key:    aws.String(key),
		},
	)
	if err != nil {
		return nil, err
	}

	return w.Bytes(), nil
}

func uploadS3(path string, body []byte) (*s3manager.UploadOutput, error) {
	uploadInput := &s3manager.UploadInput{
		Bucket: aws.String(flagS3Bucket),
		Key:    aws.String(path),
		Body:   bytes.NewReader(body),
	}
	if len(flagS3ACL) > 0 {
		uploadInput.ACL = aws.String(flagS3ACL)
	}

	svc := s3manager.NewUploader(awsSession)
	return svc.Upload(uploadInput)
}

type SortByBalance []block.BlockAccount

func (a SortByBalance) Len() int           { return len(a) }
func (a SortByBalance) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a SortByBalance) Less(i, j int) bool { return a[i].Balance > a[j].Balance }

func getInflation() (height uint64, inflation map[operation.OperationType]common.Amount, err error) {
	{ // latest block from s3
		var b []byte
		if b, err = downloadS3(latestBlockFile); err != nil {
			log.Error("failed to download `.latest-block.txt` from s3", "error", err)
		} else {
			log.Debug("downloaded `.latest-block.txt` from s3", "content", string(b))
			if height, err = strconv.ParseUint(string(b), 10, 64); err != nil {
				log.Error("failed parse `.latest-block.txt`", "error", err)
				height = 0
			}
		}

		log.Debug("start height", "height", height)
	}

	inflation = map[operation.OperationType]common.Amount{}

	if height > 0 { // latest inflation data
		var b []byte
		if b, err = downloadS3(totalInflationFile); err != nil {
			log.Error("failed to download latest inflation data from s3", "error", err)
		} else {
			log.Debug("downloaded latest inflation data from s3", "content", string(b))

			var l string
			for _, s := range strings.Split(string(b), "\n") {
				if strings.Contains(s, "#") {
					continue
				} else if len(strings.TrimSpace(s)) < 1 {
					continue
				}

				l = s
				break
			}

			s := strings.SplitN(l, ",", 3)
			if len(s) == 3 {
				var i uint64
				if i, err = strconv.ParseUint(s[1], 10, 64); err != nil {
					log.Error("failed to parse TypeInflation", "content", s[1])
					goto end
				}
				inflation[operation.TypeInflation] = common.Amount(i)

				if i, err = strconv.ParseUint(s[2], 10, 64); err != nil {
					log.Error("failed to parse TypeInflationPF", "content", s[1])
					goto end
				}
				inflation[operation.TypeInflationPF] = common.Amount(i)
			}
		}

	end:
		//

		log.Debug("latest inflation data loaded", "data", inflation)
	}

	var cursor []byte
	if height > 0 {
		cursor = []byte(fmt.Sprintf("%s%020d", common.BlockPrefixHeight, height))
	}

	var txInflation map[operation.OperationType]common.Amount
	var result runner.DBGetIteratorResult
	for {
		if result, err = getBlocks(cursor); err != nil {
			log.Error("failed to get block", "error", err)
			return
		}

		for _, item := range result.Items {
			var hash string
			if err = json.Unmarshal(item.Value, &hash); err != nil {
				log.Error("invalid value", "error", err)
				return
			}

			var blk block.Block
			if blk, err = getBlock(hash); err != nil {
				log.Error("failed to get block", "hash", hash, "error", err)
				return
			}
			log.Debug("check block", "height", blk.Height)

			if txInflation, err = getInflationFromTransaction(blk.ProposerTransaction); err != nil {
				return
			}
			for t, amount := range txInflation {
				inflation[t] = inflation[t].MustAdd(amount)
			}

			for _, h := range blk.Transactions {
				if txInflation, err = getInflationFromTransaction(h); err != nil {
					return
				}
				for t, amount := range txInflation {
					inflation[t] = inflation[t].MustAdd(amount)
				}
			}
			height = blk.Height
		}

		if uint64(len(result.Items)) < result.Limit {
			break
		}
		cursor = result.Items[len(result.Items)-1].Key
	}

	return
}

func getInflationFromTransaction(hash string) (inflation map[operation.OperationType]common.Amount, err error) {
	inflation = map[operation.OperationType]common.Amount{}

	var tx transaction.Transaction
	if tx, err = getTransaction(hash); err != nil {
		log.Error("failed to get transaction", "hash", hash, "error", err)
		return
	}

	var amount common.Amount
	for _, op := range tx.B.Operations {
		if amount, err = amount.Add(getInflationFromOperation(op)); err != nil {
			log.Error("invalid operation found", "operation", op)
			return
		}
		if amount < 1 {
			continue
		}
		inflation[op.H.Type] = inflation[op.H.Type].MustAdd(amount)
	}

	return
}

func getInflationFromOperation(op operation.Operation) common.Amount {
	switch op.H.Type {
	case operation.TypeInflation:
		return op.B.(operation.Payable).GetAmount()
	case operation.TypeInflationPF:
		return op.B.(operation.Payable).GetAmount()
	default:
		return common.Amount(0)
	}
}

var inflationTemplate = `
# initial balance, block inflation, pf inflation
%s,%s,%s
`

func main() {
	{ // inflation
		lastHeight, inflation, err := getInflation()
		if err != nil {
			printError("failed to get inflation", err)
		}
		log.Debug("inflation amount", "inflation", inflation, "block", lastHeight)

		t := fmt.Sprintf(
			inflationTemplate,
			nodeInfo.Policy.InitialBalance,
			inflation[operation.TypeInflation],
			inflation[operation.TypeInflationPF],
		)

		{
			output, err := uploadS3(
				totalInflationFile,
				[]byte(t),
			)
			if err != nil {
				printError("failed to total inflation upload to s3", err)
			}
			log.Debug("total inflation uploaded", "location", output.Location)
		}

		{ // save latest block
			output, err := uploadS3(
				latestBlockFile,
				[]byte(strconv.FormatInt(int64(lastHeight), 10)),
			)
			if err != nil {
				printError("failed to latest block upload to s3", err)
			}
			log.Debug("latest block uploaded", "location", output.Location)
		}
	}

	accountsMap := map[string]block.BlockAccount{}
	var accountsByBalance []block.BlockAccount
	{
		log.Debug("getting all the accounts")

		var cursor []byte
		for {
			result, err := getAccounts(cursor)
			if err != nil {
				printError("failed to get accounts", err)
			}
			for _, item := range result.Items {
				var account block.BlockAccount
				if err := json.Unmarshal(item.Value, &account); err != nil {
					printError("invalid value", err)
				}
				if _, found := accountsMap[string(account.Address)]; found {
					printError("duplicated key found", nil)
				}

				accountsMap[account.Address] = account
				if account.Balance > common.Amount(0) {
					accountsByBalance = append(accountsByBalance, account)
				}
			}

			if uint64(len(result.Items)) < result.Limit {
				break
			}
			cursor = result.Items[len(result.Items)-1].Key
		}

		log.Debug("all accounts", "accounts", len(accountsMap), "over-1", len(accountsByBalance))
	}

	{
		log.Debug("calculating total supply")
		var total common.Amount
		for _, account := range accountsByBalance {
			total = total.MustAdd(account.Balance)
		}

		log.Debug("total balance", "supply", total)
		output, err := uploadS3(totalSupplyFile, []byte(strconv.FormatInt(int64(total), 10)))
		if err != nil {
			printError("failed to total supply upload to s3", err)
		}
		log.Debug("total supply uploaded", "location", output.Location)
	}

	{
		log.Debug("sorting top holders")
		sort.Sort(SortByBalance(accountsByBalance))

		var csv []string
		var csvAll []string
		for i, account := range accountsByBalance {
			if i < flagTopHoldersLimit {
				csv = append(csv, fmt.Sprintf("%s,%s", account.Address, account.Balance))
			}
			csvAll = append(csvAll, fmt.Sprintf("%s,%s", account.Address, account.Balance))
		}

		{
			output, err := uploadS3(
				fmt.Sprintf(
					totalHoldersFile,
					fmt.Sprintf("-%d", flagTopHoldersLimit),
				),
				[]byte(strings.Join(csv, "\n")),
			)
			if err != nil {
				printError("failed to top holders upload to s3", err)
			}
			log.Debug("top holders uploaded", "location", output.Location, "limit", flagTopHoldersLimit)
		}

		{
			output, err := uploadS3(
				fmt.Sprintf(totalHoldersFile, ""),
				[]byte(strings.Join(csvAll, "\n")),
			)
			if err != nil {
				printError("failed to top holders upload to s3", err)
			}
			log.Debug("top holders uploaded", "location", output.Location)
		}
	}

	os.Exit(0)
}
