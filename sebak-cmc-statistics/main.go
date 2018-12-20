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
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
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
	flagSEBAKJSONRPC    string = "http://127.0.0.1:54321"
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
	networkID       []byte
	endpoint        *common.Endpoint
	jsonrpcEndpoint *common.Endpoint
	logLevel        logging.Lvl
	log             logging.Logger = logging.New("module", "sebak-create-accounts")
	client          *network.HTTP2NetworkClient
	awsSession      *session.Session
)

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
	os.Exit(1)
}

func init() {
	flag.StringVar(&flagSEBAKEndpoint, "sebak", flagSEBAKEndpoint, "sebak endpoint")
	flag.StringVar(&flagSEBAKJSONRPC, "sebak-jsonrpc", flagSEBAKJSONRPC, "sebak jsonrpc")
	flag.StringVar(&flagLogLevel, "log-level", flagLogLevel, "log level, {crit, error, warn, info, debug}")
	flag.StringVar(&flagLogFormat, "log-format", flagLogFormat, "log format, {terminal, json}")
	flag.StringVar(&flagLog, "log", flagLog, "set log file")
	flag.IntVar(&flagTopHoldersLimit, "top-holders-limit", flagTopHoldersLimit, "limit for number of top holders")
	flag.StringVar(&flagS3Region, "region", flagS3Region, "s3 region")
	flag.StringVar(&flagAWSAccessKeyID, "aws-access-key", flagAWSAccessKeyID, "aws access key")
	flag.StringVar(&flagAWSSecretKey, "aws-secret-key", flagAWSSecretKey, "aws secret key")
	flag.StringVar(&flagS3Bucket, "s3-bucket", flagS3Bucket, "s3 bucket name")
	flag.StringVar(&flagS3Path, "s3-path", flagS3Path, "s3 file path")
	flag.StringVar(&flagS3ACL, "s3-acl", flagS3ACL, "s3 acl; {public-read}")

	flag.Parse()

	if len(flagS3Bucket) < 1 {
		printError("--s3-bucket", fmt.Errorf("must be given"))
	}

	{
		os.Setenv("AWS_ACCESS_KEY_ID", flagAWSAccessKeyID)
		os.Setenv("AWS_SECRET_ACCESS_KEY", flagAWSSecretKey)

		var err error
		if awsSession, err = session.NewSession(&aws.Config{Region: aws.String(flagS3Region)}); err != nil {
			printError("failed to access aws s3", err)
		}
	}

	{
		var err error
		if endpoint, err = common.ParseEndpoint(flagSEBAKEndpoint); err != nil {
			printError("--sebak", err)
		}
		if jsonrpcEndpoint, err = common.ParseEndpoint(flagSEBAKJSONRPC); err != nil {
			printError("--sebak-jsonrpc", err)
		}
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

func main() {
	accountsMap := map[string]block.BlockAccount{}
	var accountsByBalance []block.BlockAccount
	{
		log.Info("getting all the accounts")

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

			cursor = result.Items[len(result.Items)-1].Key
			if uint64(len(result.Items)) < result.Limit {
				break
			}
		}

		log.Info("all accounts", "accounts", len(accountsMap), "over-1", len(accountsByBalance))
	}

	{
		log.Info("calculating total supply")
		var total common.Amount
		for _, account := range accountsByBalance {
			total = total.MustAdd(account.Balance)
		}

		log.Info("total balance", "supply", total)
		output, err := uploadS3(
			filepath.Join(flagS3Path, "total-supply.txt"),
			[]byte(strconv.FormatInt(int64(total), 10)),
		)
		if err != nil {
			printError("failed to total supply upload to s3", err)
		}
		log.Info("total supply uploaded", "location", output.Location)
	}

	{
		log.Info("sorting top holders")
		sort.Sort(SortByBalance(accountsByBalance))

		var csv []string
		for i, account := range accountsByBalance {
			if i == flagTopHoldersLimit {
				break
			}
			csv = append(csv, fmt.Sprintf("%s,%s", account.Address, account.Balance))
		}

		output, err := uploadS3(
			filepath.Join(flagS3Path, "top-holders.txt"),
			[]byte(strings.Join(csv, "\n")),
		)
		if err != nil {
			printError("failed to top holders upload to s3", err)
		}
		log.Info("top holders uploaded", "location", output.Location)
	}
}
