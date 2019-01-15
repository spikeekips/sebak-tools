package main

import (
	"encoding/base64"
	"fmt"
	"io"
	"os"
	"strings"

	jsonrpc "github.com/gorilla/rpc/json"

	cmdcommon "boscoin.io/sebak/cmd/sebak/common"
	"boscoin.io/sebak/lib/node/runner"
	"boscoin.io/sebak/lib/storage"
)

func parseFlagsDump(args []string) {
	if flagListPrefix {
		var maxLength int
		for _, prefix := range allPrefixes {
			l := len(allPrefixesWithName[prefix])
			if l <= maxLength {
				continue
			}
			maxLength = l
		}

		var lines []string

		l := fmt.Sprintf(fmt.Sprintf("%%%ds : base64 encoded\n", maxLength), "key name")
		maxLineLength := len(l)
		fmt.Printf(l)

		for _, prefix := range allPrefixes {
			l := fmt.Sprintf(
				fmt.Sprintf("%%%ds : '%%s'\n", maxLength),
				allPrefixesWithName[prefix],
				base64.StdEncoding.EncodeToString([]byte(prefix)),
			)
			if len(l) > maxLineLength {
				maxLineLength = len(l)
			}

			lines = append(lines, l)
		}

		fmt.Println(strings.Repeat("-", maxLineLength+2))
		for _, l := range lines {
			fmt.Print(l)
		}
		os.Exit(0)
	}

	if len(args) < 2 {
		cmdcommon.PrintError(dumpCmd, nil)
	}

	{ // make allPrefixesByName
		allPrefixesByName = map[string]string{}
		for prefix, name := range allPrefixesWithName {
			allPrefixesByName[name] = prefix
		}
	}

	{ // prefix
		if len(flagPrefix) < 1 {
			flagPrefix = append(flagPrefix, "all")
		}

		var all bool
		for _, prefix := range flagPrefix {
			if prefix == "all" {
				all = true
				continue
			}

			if _, found := allPrefixesByName[prefix]; !found {
				cmdcommon.PrintFlagsError(dumpCmd, "--prefix", fmt.Errorf("unknown prefix found: %v", prefix))
			}
		}

		if all {
			flagPrefix = ListFlags{}
			for _, name := range allPrefixesWithName {
				flagPrefix = append(flagPrefix, name)
			}
		}
	}

	parseLogging(dumpCmd)

	{ // output format
		switch flagOutputFormat {
		case "leveldb":
		case "json":
		default:
			cmdcommon.PrintFlagsError(dumpCmd, "--format", fmt.Errorf("unknown output format found"))
		}
	}

	{ // check source
		flagSource = args[0]
		if err := checkSource(flagSource); err != nil {
			cmdcommon.PrintFlagsError(dumpCmd, "<db source directory>", err)
		}
	}

	{ // checkout output
		flagOutput = args[1]

		if _, err := os.Stat(flagOutput); !os.IsNotExist(err) {
			d, err := os.Open(flagOutput)
			if err != nil {
				cmdcommon.PrintFlagsError(dumpCmd, "<output directory>", err)
			}
			defer d.Close()

			if _, err = d.Readdirnames(1); err == io.EOF {
				// empty
			} else {
				if flagForce {
					if err := os.RemoveAll(flagOutput); err != nil {
						log.Error("failed to clean up output directory, but failed", "error", err)
						cmdcommon.PrintFlagsError(dumpCmd, "<output directory>", err)
					}
					log.Debug("output directory found, but remote it by force", "directory", flagOutput)
				} else {
					cmdcommon.PrintFlagsError(dumpCmd, "<output directory>", fmt.Errorf("directory, `%s` is not empty", flagOutput))
				}
			}
		}

		if flagOutputFormat == "leveldb" {
			if storageConfig, err := storage.NewConfigFromString("file://" + flagOutput); err != nil {
				cmdcommon.PrintFlagsError(dumpCmd, "<output directory>", err)
			} else {
				if st, err := storage.NewStorage(storageConfig); err != nil {
					cmdcommon.PrintFlagsError(dumpCmd, "<output directory>", err)
				} else {
					stOutput = st
				}
			}
		} else if flagOutputFormat == "json" {
			// create new directory
			if err := os.MkdirAll(flagOutput, 0700); err != nil {
				cmdcommon.PrintFlagsError(dumpCmd, "<output directory>", err)
			}

			jsonFiles = map[string]*GzipWriter{}
		}
	}

	parsedFlags := []interface{}{}
	parsedFlags = append(parsedFlags, "\n\tlog-level", logLevel)
	parsedFlags = append(parsedFlags, "\n\tlog-format", flagLogFormat)
	parsedFlags = append(parsedFlags, "\n\tlog", flagLog)
	parsedFlags = append(parsedFlags, "\n\tforce", flagForce)
	parsedFlags = append(parsedFlags, "\n\tverbose", flagVerbose)
	parsedFlags = append(parsedFlags, "\n\tsource", flagSource)
	parsedFlags = append(parsedFlags, "\n\toutput", flagOutput)
	parsedFlags = append(parsedFlags, "\n\tjsonRPCEndpoint", jsonRPCEndpoint)
	parsedFlags = append(parsedFlags, "\n\tprefix", flagPrefix)
	parsedFlags = append(parsedFlags, "\n\toutput-format", flagOutputFormat)
	parsedFlags = append(parsedFlags, "\n", "")

	log.Debug("parsed flags:", parsedFlags...)
}

func dumpSource(prefix string) {
	limit := runner.MaxLimitListOptions

	var cursor []byte
	var allCount int
end:
	for {
		var count int
		it, closeFunc := stSource.GetIterator(
			prefix,
			storage.NewDefaultListOptions(false, cursor, limit),
		)
		var item storage.IterItem
		for {
			i, hasNext := it()
			if !hasNext {
				break
			}

			item = i.Clone()
			if err := saveItemToOutput(dumpCmd, prefix, item); err != nil {
				log.Error("failed to save item", "error", err, "prefix", prefix, "cursor", cursor)

				closeFunc()

				return
			}
			allCount += 1
			count += 1
		}

		closeFunc()

		if allCount%100000 == 0 {
			log.Debug("got items", "count", allCount, "prefix", allPrefixesWithName[prefix])
		}

		if count < int(limit) {
			break end
		}

		cursor = item.Key
	}

	log.Debug("dump from source finished", "item-count", allCount, "prefix", allPrefixesWithName[prefix])
}

func dumpJsonRPC(prefix string) {
	defer func() { // ReleaseSnapshot
		if len(jsonrpcSnapshot) < 1 {
			return
		}
		resp, err := request("DB.ReleaseSnapshot", &runner.DBReleaseSnapshot{Snapshot: jsonrpcSnapshot})
		if err != nil {
			log.Error("failed to ReleaseSnapshot", "error", err)
			return
		}
		defer resp.Body.Close()

		var result runner.DBReleaseSnapshotResult
		if err := jsonrpc.DecodeClientResponse(resp.Body, &result); err != nil {
			log.Error("failed to ReleaseSnapshot", "error", err)
			return
		}
		log.Debug("snapshot released", "result", result)

		return
	}()

	{ // OpenSnapshot
		resp, err := request("DB.OpenSnapshot", &runner.DBOpenSnapshotResult{})
		if err != nil {
			log.Error("failed to OpenSnapshot", "error", err)
			return
		}
		defer resp.Body.Close()

		var result runner.DBOpenSnapshotResult
		if err := jsonrpc.DecodeClientResponse(resp.Body, &result); err != nil {
			log.Error("failed to OpenSnapshot", "error", err)
			return
		}

		jsonrpcSnapshot = result.Snapshot
	}

	var count int
	var cursor []byte
	for {
		log.Debug(
			"DB.GetIterator",
			"prefix", []byte(prefix),
			"cursor", cursor,
			"limit", runner.MaxLimitListOptions,
		)

		args := runner.DBGetIteratorArgs{
			Snapshot: jsonrpcSnapshot,
			Prefix:   prefix,
			Options: runner.GetIteratorOptions{
				Reverse: false,
				Limit:   runner.MaxLimitListOptions,
				Cursor:  cursor,
			},
		}
		resp, err := request("DB.GetIterator", &args)
		if err != nil {
			log.Error("failed to db.GetIterator", "error", err)
			return
		}
		defer resp.Body.Close()

		var result runner.DBGetIteratorResult
		if err := jsonrpc.DecodeClientResponse(resp.Body, &result); err != nil {
			log.Error("failed to parse result", "error", err)
			return
		}

		count += len(result.Items)
		log.Debug("got result", "items", len(result.Items), "limit", result.Limit)

		for _, item := range result.Items {
			if err := saveItemToOutput(dumpCmd, prefix, item); err != nil {
				log.Error("failed to save item", "error", err)
				return
			}
		}

		if len(result.Items) < int(result.Limit) {
			break
		}
		cursor = result.Items[len(result.Items)-1].Key
	}

	log.Debug("db.GetIterator finished", "item-count", count)
}

func dump() {
	var stSourceOrig *storage.LevelDBBackend
	defer func() {
		if stSourceOrig != nil {
			stSource.Core.(*storage.Snapshot).Release()
			stSource = stSourceOrig
		}

		if stSource != nil {
			stSource.Close()
		}
		if stOutput != nil {
			stOutput.Close()
		}

		if jsonFiles != nil {
			for _, f := range jsonFiles {
				f.Close()
			}
		}
	}()

	if stSource != nil {
		stSourceOrig = stSource
		if st, err := stSource.OpenSnapshot(); err != nil {
			log.Error("failed to OpenSnapshot", "error", err)
			return
		} else {
			stSource = st
		}
	}

	for _, prefix := range flagPrefix {
		p := allPrefixesByName[prefix]
		if jsonRPCEndpoint != nil {
			dumpJsonRPC(p)
		} else if stSource != nil {
			dumpSource(p)
		}
	}

	log.Debug("finished")
}
