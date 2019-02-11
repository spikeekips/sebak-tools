package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"syscall"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	jsonrpc "github.com/gorilla/rpc/json"
	logging "github.com/inconshreveable/log15"
	isatty "github.com/mattn/go-isatty"

	"boscoin.io/sebak/lib/block"
	"boscoin.io/sebak/lib/common"
	"boscoin.io/sebak/lib/network"
	"boscoin.io/sebak/lib/node"
	"boscoin.io/sebak/lib/node/runner"
	"boscoin.io/sebak/lib/transaction"
	"boscoin.io/sebak/lib/transaction/operation"
)

const (
	defaultLogLevel        logging.Lvl = logging.LvlInfo
	defaultLogFormat       string      = "terminal"
	defaultRequestTimeout  string      = "30s"
	defaultConfirmDuration string      = "60s"
	defaultOperationsLimit int         = 800
)

var (
	flagInit            bool
	flagDryrun          bool
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
	log             logging.Logger = logging.New("module", "sebak-stats")
	client          *network.HTTP2NetworkClient
	awsSession      *session.Session
	nodeInfo        node.NodeInfo

	snapshot string

	latestBlockFile        string
	totalInflationFile     string
	totalSupplyFile        string
	totalSupplyDetailsFile string
	totalHoldersFile       string
	frozenAccountFile      string
	dryrunDirectory        string
)

var chanStop = make(chan os.Signal, 1)

func printError(s string, err error, args ...interface{}) {
	var errString string
	if err != nil {
		errString = err.Error()
	}

	if len(s) > 0 {
		fmt.Println("error:", fmt.Sprintf(s, args...), ":", errString)
	}

	exit(1)
}

func printFlagsError(s string, err error) {
	var errString string
	if err != nil {
		errString = err.Error()
	}

	if len(s) > 0 {
		fmt.Println("error:", s, "", errString)
	}
	fmt.Fprintf(os.Stderr, "Usage: %s <secret seed> <accounts>\n", os.Args[0])

	flag.PrintDefaults()
	exit(1)
}

func exit(s int) {
	if len(snapshot) > 0 {
		releaseSnapshot()
	}

	os.Exit(s)
}

func parseCSV(b string) (l [][]string) {
	for _, s := range strings.Split(b, "\n") {
		if strings.Contains(s, "#") {
			continue
		} else if len(strings.TrimSpace(s)) < 1 {
			continue
		}

		n := strings.Split(s, ",")
		l = append(l, n)
	}

	return
}

// gonToBOS prints with dot for GON.
func gonToBOS(i interface{}) string {
	var s string
	switch i.(type) {
	case uint64:
		s = strconv.FormatUint(i.(uint64), 10)
	case common.Amount:
		s = i.(common.Amount).String()
	}

	if len(s) < 7 {
		return fmt.Sprintf(
			"0.%s%s",
			strings.Repeat("0", 7-len(s)),
			s,
		)
	}

	if len(s) == 7 {
		return fmt.Sprintf("0.%s", s)
	}

	return fmt.Sprintf("%s.%s", s[:len(s)-7], s[len(s)-7:])
}

func bosToString(s string) common.Amount {
	return common.MustAmountFromString(strings.Replace(s, ".", "", 1))
}

func init() {
	flags = flag.NewFlagSet(os.Args[0], flag.ExitOnError)
	flags.SetOutput(os.Stderr)

	flags.Usage = func() {
		fmt.Fprintf(flags.Output(), "Usage: %s <secret seed> <accounts>\n", os.Args[0])
		flags.PrintDefaults()
	}

	flags.BoolVar(&flagInit, "init", flagInit, "initialize")
	flags.BoolVar(&flagDryrun, "dry-run", flagDryrun, "dry-run")
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

	{
		var err error

		if logLevel, err = logging.LvlFromString(flagLogLevel); err != nil {
			printFlagsError("--log-level", err)
		}

		var logFormatter logging.Format
		switch flagLogFormat {
		case "terminal":
			if isatty.IsTerminal(os.Stdout.Fd()) && len(flagLog) < 1 {
				logFormatter = logging.TerminalFormat()
			} else {
				logFormatter = logging.LogfmtFormat()
			}
		case "json":
			logFormatter = common.JsonFormatEx(false, true)
		default:
			printFlagsError("--log-format", fmt.Errorf("'%s'", flagLogFormat))
		}

		logHandler := logging.StreamHandler(os.Stdout, logFormatter)
		if len(flagLog) > 0 {
			if logHandler, err = logging.FileHandler(flagLog, logFormatter); err != nil {
				printFlagsError("--log", err)
			}
		}

		if logLevel == logging.LvlDebug { // only debug produces `caller` data
			logHandler = logging.CallerFileHandler(logHandler)
		}
		logHandler = logging.LvlFilterHandler(logLevel, logHandler)
		log.SetHandler(logHandler)
	}

	if len(flagS3Bucket) < 1 {
		printFlagsError("--s3-bucket", fmt.Errorf("must be given"))
	}

	{
		os.Setenv("AWS_ACCESS_KEY_ID", flagAWSAccessKeyID)
		os.Setenv("AWS_SECRET_ACCESS_KEY", flagAWSSecretKey)

		var err error
		if awsSession, err = session.NewSession(&aws.Config{Region: aws.String(flagS3Region)}); err != nil {
			printFlagsError("failed to access aws s3", err)
		}
	}

	{
		var err error
		if endpoint, err = common.ParseEndpoint(flagSEBAKEndpoint); err != nil {
			printFlagsError("--sebak", err)
		}
		if jsonrpcEndpoint, err = common.ParseEndpoint(flagSEBAKJSONRPC); err != nil {
			printFlagsError("--sebak-jsonrpc", err)
		}
	}

	{
		var err error
		var connection *common.HTTP2Client

		// Keep-alive ignores timeout/idle timeout
		if connection, err = common.NewHTTP2Client(0, 0, true); err != nil {
			printFlagsError("Error while creating network client", err)
		}
		client = network.NewHTTP2NetworkClient(endpoint, connection)

		resp, err := client.Get("/")
		if err != nil {
			printFlagsError("failed to connect sebak", err)
		}

		if nodeInfo, err = node.NewNodeInfoFromJSON(resp); err != nil {
			printFlagsError("failed to parse node info response", err)
		}
		networkID = []byte(nodeInfo.Policy.NetworkID)
	}

	if flagDryrun {
		f, err := ioutil.TempDir("/tmp", "sebak-stat")
		if err != nil {
			printError("failed to create temp directory", err)
		}
		dryrunDirectory = f
		flagS3Path = ""
		log.Info("output files will be saved in", "directory", dryrunDirectory)
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
	parsedFlags = append(parsedFlags, "\n\tdryrun-directory", dryrunDirectory)

	log.Debug("parsed flags:", parsedFlags...)

	latestBlockFile = filepath.Join(flagS3Path, "latest-block.txt")
	totalInflationFile = filepath.Join(flagS3Path, "total-inflation.txt")
	totalSupplyFile = filepath.Join(flagS3Path, "total-supply.txt")
	totalSupplyDetailsFile = filepath.Join(flagS3Path, "total-supply-details.txt")
	totalHoldersFile = filepath.Join(flagS3Path, "top-holders%s.txt")
	frozenAccountFile = filepath.Join(flagS3Path, "frozen-accounts.txt")

	{
		var err error
		if snapshot, err = openSnapshot(); err != nil {
			printError("failed to open snapshot", err)
		}

		signal.Notify(chanStop, syscall.SIGTERM)
		signal.Notify(chanStop, syscall.SIGINT)
		signal.Notify(chanStop, syscall.SIGKILL)

		go func() {
			<-chanStop

			if len(snapshot) > 0 {
				releaseSnapshot()
			}
			exit(0)
		}()
	}
}

func openSnapshot() (snapshot string, err error) {
	log.Debug("trying to open snapshot")

	var message []byte
	if message, err = jsonrpc.EncodeClientRequest("DB.OpenSnapshot", &runner.DBOpenSnapshotResult{}); err != nil {
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

	var result runner.DBOpenSnapshotResult
	if err = jsonrpc.DecodeClientResponse(resp.Body, &result); err != nil {
		return
	}

	snapshot = result.Snapshot

	log.Debug("snapshot opened")
	return
}

func releaseSnapshot() (err error) {
	log.Debug("trying to release snapshot", "snapshot", snapshot)

	var message []byte
	if message, err = jsonrpc.EncodeClientRequest("DB.ReleaseSnapshot", &runner.DBReleaseSnapshot{Snapshot: snapshot}); err != nil {
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

	var result runner.DBReleaseSnapshotResult
	if err = jsonrpc.DecodeClientResponse(resp.Body, &result); err != nil {
		return
	}

	snapshot = ""
	log.Debug("snapshot released")

	return
}

func getAccounts(cursor []byte) (result runner.DBGetIteratorResult, err error) {
	args := runner.DBGetIteratorArgs{
		Snapshot: snapshot,
		Prefix:   common.BlockAccountPrefixAddress,
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
		Snapshot: snapshot,
		Prefix:   common.BlockPrefixHeight,
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
	args := runner.DBGetArgs{
		Snapshot: snapshot,
		Key:      key,
	}
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

func uploadS3(path string, body []byte) {
	if flagDryrun {
		err := ioutil.WriteFile(filepath.Join(dryrunDirectory, path), body, 0644)
		if err != nil {
			printError("failed to save file in dryrun directory", err)
		}

		return
	}

	uploadInput := &s3manager.UploadInput{
		Bucket: aws.String(flagS3Bucket),
		Key:    aws.String(path),
		Body:   bytes.NewReader(body),
	}
	if len(flagS3ACL) > 0 {
		uploadInput.ACL = aws.String(flagS3ACL)
	}

	svc := s3manager.NewUploader(awsSession)
	output, err := svc.Upload(uploadInput)
	if err != nil {
		printError(fmt.Sprintf("failed to upload %s to s3", path), err)
	}
	log.Debug("uploaded", "location", output.Location, "path", path)
}

type SortByBalance []block.BlockAccount

func (a SortByBalance) Len() int           { return len(a) }
func (a SortByBalance) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a SortByBalance) Less(i, j int) bool { return a[i].Balance > a[j].Balance }

func getInflation() (height uint64, inflation map[operation.OperationType]common.Amount, err error) {
	inflation = map[operation.OperationType]common.Amount{}

	if !flagInit {
		{
			// latest block from s3
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

		if height > 0 { // latest inflation data
			var b []byte
			if b, err = downloadS3(totalInflationFile); err != nil {
				log.Error("failed to download latest inflation data from s3", "error", err)
			} else {
				log.Debug("downloaded latest inflation data from s3", "content", string(b))

				l := parseCSV(string(b))
				if len(l) < 1 {
					log.Error("failed to parse downloaded latest inflation data from s3")
				} else if len(l[0]) < 3 {
					log.Error("invalid downloaded latest inflation data from s3", "data", l)
				} else {
					inflation[operation.TypeInflation] = bosToString(l[0][1])
					inflation[operation.TypeInflationPF] = bosToString(l[0][2])
				}
			}

			log.Debug("latest inflation data loaded", "data", inflation)
		}
	}

	var cursor []byte
	if height > 0 {
		cursor = []byte(fmt.Sprintf("%s%020d", common.BlockPrefixHeight, height))
	}

	var txInflation map[operation.OperationType]common.Amount
	var result runner.DBGetIteratorResult
	var blk block.Block
	var previousBlk block.Block
	for {
		if result, err = getBlocks(cursor); err != nil {
			log.Error("failed to get block", "error", err, "cursor", string(cursor))
			return
		}

		for _, item := range result.Items {
			var hash string
			if err = json.Unmarshal(item.Value, &hash); err != nil {
				log.Error("invalid value", "error", err)
				return
			}

			previousBlk = blk
			if blk, err = getBlock(hash); err != nil {
				log.Error("failed to get block", "hash", hash, "error", err, "cursor", string(cursor), "previousBlock", previousBlk)
				return
			}
			if blk.Height%1000 == 0 {
				log.Debug("check block", "height", blk.Height)
			}

			if blk.Height != common.GenesisBlockHeight {
				if txInflation, err = getInflationFromTransaction(blk.ProposerTransaction); err != nil {
					return
				}
				for t, amount := range txInflation {
					inflation[t] = inflation[t].MustAdd(amount)
				}
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

	if blk.Height%1000 != 0 {
		log.Debug("last checked block", "height", blk.Height)
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
		return op.B.(operation.InflationPF).GetAmount()
	default:
		return common.Amount(0)
	}
}

func getBlockOperation(hash string) (bo block.BlockOperation, err error) {
	var result runner.DBGetResult
	if result, err = getDB(fmt.Sprintf("%s%s", common.BlockOperationPrefixHash, hash)); err != nil {
		return
	}

	err = json.Unmarshal(result.Value, &bo)
	return
}

func getLastBlockOperation(address string) (bo block.BlockOperation, err error) {
	args := runner.DBGetIteratorArgs{
		Snapshot: snapshot,
		Prefix:   fmt.Sprintf("%s%s-", common.BlockOperationPrefixPeers, address),
		Options: runner.GetIteratorOptions{
			Limit:   1,
			Reverse: true,
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

	var result runner.DBGetIteratorResult
	if err = jsonrpc.DecodeClientResponse(resp.Body, &result); err != nil {
		return
	}

	if len(result.Items) < 1 {
		err = fmt.Errorf("BlockOperation not found")
		return
	}
	var hash string
	if err = json.Unmarshal(result.Items[0].Value, &hash); err != nil {
		return
	}

	return getBlockOperation(hash)
}

var inflationTemplate = `# initial balance, block inflation, pf inflation
%s,%s,%s
`

var totalSupplyDetailsTemplate = `# block height, total supply
%d,%s
`

var frozenTemplate = `# number of membership, number of frozen, total frozen amount, number of unfrozen, total unfrozen amount
%d,%d,%s,%d,%s
`

func main() {
	var lastBlockHeight uint64

	{ // inflation
		lastHeight, inflation, err := getInflation()
		lastBlockHeight = lastHeight
		if err != nil {
			printError("failed to get inflation", err)
		}
		log.Debug("inflation amount", "inflation", inflation, "block", lastHeight)

		t := fmt.Sprintf(
			inflationTemplate,
			gonToBOS(nodeInfo.Policy.InitialBalance),
			gonToBOS(inflation[operation.TypeInflation]),
			gonToBOS(inflation[operation.TypeInflationPF]),
		)

		uploadS3(
			totalInflationFile,
			[]byte(t),
		)
		uploadS3(
			latestBlockFile,
			[]byte(strconv.FormatUint(lastHeight, 10)),
		)
	}

	membershipCount := map[string]bool{}
	var frozen []string
	var unfrozen []string
	var frozenAmount common.Amount

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
				if len(account.Linked) > 0 {
					frozen = append(frozen, account.Address)
					frozenAmount = frozenAmount.MustAdd(account.Balance)
					membershipCount[account.Linked] = true
				}
			}

			if uint64(len(result.Items)) < result.Limit {
				break
			}
			cursor = result.Items[len(result.Items)-1].Key
		}

		log.Debug("all accounts", "accounts", len(accountsMap), "over-1", len(accountsByBalance))

	}

	{ // frozen account
		var unfrozenAmount common.Amount
		for _, address := range frozen {
			bo, err := getLastBlockOperation(address)
			if err != nil {
				log.Crit("failed to get BlockOperation", "address", address, "error", err)
				printError("failed to get BlockOperation", err)
			}

			if bo.Type != operation.TypeUnfreezingRequest {
				continue
			}
			if lastBlockHeight-bo.Height < common.UnfreezingPeriod {
				continue
			}
			unfrozen = append(unfrozen, address)
			account := accountsMap[address]
			unfrozenAmount = unfrozenAmount.MustAdd(account.Balance)
		}

		log.Debug(
			"all freezing accounts",
			"all", len(frozen),
			"frozen", len(frozen)-len(unfrozen),
			"unfrozen", len(unfrozen),
			"frozen-amount", frozenAmount-unfrozenAmount,
			"unfrozen-amount", unfrozenAmount,
			"membership-count", len(membershipCount),
		)

		t := fmt.Sprintf(
			frozenTemplate,
			len(membershipCount),
			len(frozen)-len(unfrozen),
			gonToBOS(frozenAmount-unfrozenAmount),
			len(unfrozen),
			gonToBOS(unfrozenAmount),
		)

		uploadS3(frozenAccountFile, []byte(t))
	}

	{
		log.Debug("calculating total supply")
		var total uint64
		for _, account := range accountsByBalance {
			total += uint64(account.Balance)
		}

		log.Debug("total balance", "supply", total)

		uploadS3(totalSupplyFile, []byte(gonToBOS(total)))

		t := fmt.Sprintf(
			totalSupplyDetailsTemplate,
			lastBlockHeight,
			gonToBOS(total),
		)
		uploadS3(totalSupplyDetailsFile, []byte(t))
	}

	{
		log.Debug("sorting top holders")
		sort.Sort(SortByBalance(accountsByBalance))

		csv := []string{"# order,address,balance"}
		csvAll := []string{"# order,address,balance"}

		for i, account := range accountsByBalance {
			if i < flagTopHoldersLimit {
				csv = append(csv, fmt.Sprintf(
					"%d,%s,%s",
					i,
					account.Address,
					gonToBOS(account.Balance),
				))
			}
			csvAll = append(csvAll, fmt.Sprintf(
				"%d,%s,%s",
				i,
				account.Address,
				gonToBOS(account.Balance),
			))
		}

		uploadS3(
			fmt.Sprintf(
				totalHoldersFile,
				fmt.Sprintf("-%d", flagTopHoldersLimit),
			),
			[]byte(strings.Join(csv, "\n")),
		)
		uploadS3(
			fmt.Sprintf(totalHoldersFile, ""),
			[]byte(strings.Join(csvAll, "\n")),
		)
	}

	exit(0)
}
