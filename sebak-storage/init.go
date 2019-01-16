package main

import (
	"bytes"
	"flag"
	"os"
	"strings"
	"sync"
	"text/template"

	logging "github.com/inconshreveable/log15"
	"github.com/spf13/cobra"
	"golang.org/x/crypto/ssh/terminal"

	cmdcommon "boscoin.io/sebak/cmd/sebak/common"
	"boscoin.io/sebak/lib/common"
	"boscoin.io/sebak/lib/storage"
)

var (
	defaultLogFormat string      = "terminal"
	defaultLogLevel  logging.Lvl = logging.LvlInfo
	flagLog          string      = common.GetENVValue("SEBAK_LOG", "")
	flagLogLevel     string      = common.GetENVValue("SEBAK_LOG_LEVEL", defaultLogLevel.String())
	flagLogFormat    string      = common.GetENVValue("SEBAK_LOG_FORMAT", defaultLogFormat)
	flagForce        bool

	flagSource       string
	flagOutput       string
	flagPrefix       ListFlags
	flagListPrefix   bool
	flagOutputFormat string = "leveldb" // "json"

	flags    *flag.FlagSet = flag.NewFlagSet(os.Args[0], flag.ExitOnError)
	logLevel logging.Lvl
	log      logging.Logger = logging.New("module", "main")

	jsonRPCEndpoint *common.Endpoint
	jsonrpcSnapshot string
	stSource        *storage.LevelDBBackend
	stOutput        *storage.LevelDBBackend
	jsonFiles       *sync.Map
)

var allPrefixes []string = []string{
	common.BlockPrefixHash,
	common.BlockPrefixConfirmed,
	common.BlockPrefixHeight,
	common.BlockTransactionPrefixHash,
	common.BlockTransactionPrefixSource,
	common.BlockTransactionPrefixConfirmed,
	common.BlockTransactionPrefixAccount,
	common.BlockTransactionPrefixBlock,
	common.BlockOperationPrefixHash,
	common.BlockOperationPrefixTxHash,
	common.BlockOperationPrefixSource,
	common.BlockOperationPrefixTarget,
	common.BlockOperationPrefixPeers,
	common.BlockOperationPrefixTypeSource,
	common.BlockOperationPrefixTypeTarget,
	common.BlockOperationPrefixTypePeers,
	common.BlockOperationPrefixCreateFrozen,
	common.BlockOperationPrefixFrozenLinked,
	common.BlockOperationPrefixBlockHeight,
	common.BlockAccountPrefixAddress,
	common.BlockAccountPrefixCreated,
	common.BlockAccountSequenceIDPrefix,
	common.BlockAccountSequenceIDByAddressPrefix,
	common.TransactionPoolPrefix,
	common.InternalPrefix,
}

var allPrefixesWithName map[string]string = map[string]string{
	common.BlockPrefixHash:                       "block-hash",
	common.BlockPrefixConfirmed:                  "block-confirmed",
	common.BlockPrefixHeight:                     "block-height",
	common.BlockTransactionPrefixHash:            "block-transaction-hash",
	common.BlockTransactionPrefixSource:          "block-transaction-source",
	common.BlockTransactionPrefixConfirmed:       "block-transaction-confirmed",
	common.BlockTransactionPrefixAccount:         "block-transaction-account",
	common.BlockTransactionPrefixBlock:           "block-transaction-block",
	common.BlockOperationPrefixHash:              "block-operation-hash",
	common.BlockOperationPrefixTxHash:            "block-operation-txhash",
	common.BlockOperationPrefixSource:            "block-operation-source",
	common.BlockOperationPrefixTarget:            "block-operation-target",
	common.BlockOperationPrefixPeers:             "block-operation-peers",
	common.BlockOperationPrefixTypeSource:        "block-operation-type-source",
	common.BlockOperationPrefixTypeTarget:        "block-operation-type-target",
	common.BlockOperationPrefixTypePeers:         "block-operation-type-peers",
	common.BlockOperationPrefixCreateFrozen:      "block-operation-createfrozen",
	common.BlockOperationPrefixFrozenLinked:      "block-operation-frozenlinked",
	common.BlockOperationPrefixBlockHeight:       "block-operation-blockheight",
	common.BlockAccountPrefixAddress:             "block-account-address",
	common.BlockAccountPrefixCreated:             "block-account-created",
	common.BlockAccountSequenceIDPrefix:          "block-account-sequenceid",
	common.BlockAccountSequenceIDByAddressPrefix: "block-account-sequenceidbyaddress",
	common.TransactionPoolPrefix:                 "transaction-pool",
	common.InternalPrefix:                        "internal",
}

var allPrefixesByName map[string]string

var cmd *cobra.Command
var dumpCmd *cobra.Command
var importCmd *cobra.Command

var dumpExampleTemplate = `
$ sebak-storage dump http://localhost:54321/jsonrpc /sebak-dumped
{{ index . "line" }}
Dump storage to '/sebak-dumped' thru jsonrpc

$ sebak-storage dump /sebak-db /sebak-dumped
{{ index . "line" }}
Dump local storage, '/sebak-db' to '/sebak-dumped'

$ sebak-storage dump --format leveldb http://localhost:54321/jsonrpc /sebak-dumped
{{ index . "line" }}
Dump storage to '/sebak-dumped' as leveldb; by default '--format' is 'leveldb'.

$ sebak-storage dump --format json http://localhost:54321/jsonrpc /sebak-dumped
{{ index . "line" }}
Dump storage to '/sebak-dumped' as gzipped json files

$ sebak-storage dump --list-prefix
{{ index . "line" }}
Print all prefixes

$ sebak-storage dump --prefix block-hash /sebak-db /sebak-dumped
{{ index . "line" }}
Dump local storage, '/sebak-db' to '/sebak-dumped'; only 'block-hash' prefixed data
`

var importExampleTemplate = `
$ sebak-storage import /sebak-dumped /sebak-new-storage
{{ index . "line" }}
Import dumped directory, '/sebak-dumped' to '/sebak-new-storage'
`

func init() {
	cmd = &cobra.Command{
		Use:   os.Args[0],
		Short: "sebak-storage",
		Run: func(c *cobra.Command, args []string) {
			if len(args) < 1 {
				c.Usage()
			}
		},
	}

	termWidth, _, _ := terminal.GetSize(int(os.Stdout.Fd()))

	{
		t := template.Must(template.New("example-dump").Parse(dumpExampleTemplate))
		var b bytes.Buffer
		if err := t.Execute(&b, map[string]string{"line": strings.Repeat("-", termWidth-1)}); err != nil {
			cmdcommon.PrintError(dumpCmd, err)
		}

		dumpCmd = &cobra.Command{
			Use:     "dump <source> <output directory>",
			Short:   "dump storage",
			Example: b.String(),
			Run: func(c *cobra.Command, args []string) {
				parseFlagsDump(args)

				dump()
			},
		}

		dumpCmd.Flags().StringVar(&flagLogLevel, "log-level", flagLogLevel, "log level, {crit, error, warn, info, debug}")
		dumpCmd.Flags().StringVar(&flagLogFormat, "log-format", flagLogFormat, "log format, {terminal, json}")
		dumpCmd.Flags().StringVar(&flagLog, "log", flagLog, "set log file")
		dumpCmd.Flags().BoolVar(&flagForce, "force", flagForce, "clean up by force")
		dumpCmd.Flags().Var(&flagPrefix, "prefix", "set prefix")
		dumpCmd.Flags().BoolVar(&flagListPrefix, "list-prefix", flagListPrefix, "list all prefixes")
		dumpCmd.Flags().StringVar(&flagOutputFormat, "format", flagOutputFormat, "output format; {'leveldb', 'json'}")

		cmd.AddCommand(dumpCmd)
	}

	{
		t := template.Must(template.New("example-import").Parse(importExampleTemplate))
		var b bytes.Buffer
		if err := t.Execute(&b, map[string]string{"line": strings.Repeat("-", termWidth-1)}); err != nil {
			cmdcommon.PrintError(importCmd, err)
		}

		importCmd = &cobra.Command{
			Use:     "import <json dump directory> <output directory>",
			Short:   "import json dumped directory to leveldb",
			Args:    cobra.ExactArgs(2),
			Example: b.String(),
			Run: func(c *cobra.Command, args []string) {
				parseFlagsImport(args)

				importSource()
			},
		}

		importCmd.Flags().StringVar(&flagLogLevel, "log-level", flagLogLevel, "log level, {crit, error, warn, info, debug}")
		importCmd.Flags().StringVar(&flagLogFormat, "log-format", flagLogFormat, "log format, {terminal, json}")
		importCmd.Flags().StringVar(&flagLog, "log", flagLog, "set log file")
		importCmd.Flags().BoolVar(&flagForce, "force", flagForce, "clean up by force")

		cmd.AddCommand(importCmd)
	}
}
