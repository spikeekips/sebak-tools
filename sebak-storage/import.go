package main

import (
	"bufio"
	"compress/gzip"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"

	cmdcommon "boscoin.io/sebak/cmd/sebak/common"
	"boscoin.io/sebak/lib/storage"
)

func parseFlagsImport(args []string) {
	parseLogging(importCmd)

	{ // output format
		switch flagOutputFormat {
		case "leveldb":
		case "json":
		default:
			cmdcommon.PrintFlagsError(importCmd, "--format", fmt.Errorf("unknown output format found"))
		}
	}

	{ // check source
		flagSource = args[0]

		if _, err := os.Stat(flagSource); !os.IsNotExist(err) {
			d, err := os.Open(flagSource)
			if err != nil {
				cmdcommon.PrintFlagsError(importCmd, "<json dump directory>", err)
			}
			defer d.Close()

			var gzippedFound bool
			if files, err := d.Readdirnames(0); err != nil {
				if err == io.EOF {
					log.Error("json dump directory found, but empty", "directory", flagSource)
				} else {
					log.Error("failed to read json dump directory", "error", err)
				}
				cmdcommon.PrintFlagsError(importCmd, "<json dump directory>", err)
				return
			} else {
				for _, f := range files {
					if strings.HasSuffix(f, ".json.gz") {
						gzippedFound = true
						break
					}
				}
			}
			if !gzippedFound {
				cmdcommon.PrintFlagsError(importCmd, "<json dump directory>", fmt.Errorf("gzipped json.gz not found", "<json dump directory>"))
			}
		}
	}

	{ // checkout output
		flagOutput = args[1]

		if _, err := os.Stat(flagOutput); !os.IsNotExist(err) {
			d, err := os.Open(flagOutput)
			if err != nil {
				cmdcommon.PrintFlagsError(importCmd, "<output directory>", err)
			}
			defer d.Close()

			if _, err = d.Readdirnames(1); err == io.EOF {
				// empty
			} else {
				if flagForce {
					if err := os.RemoveAll(flagOutput); err != nil {
						log.Error("failed to clean up output directory, but failed", "error", err)
						cmdcommon.PrintFlagsError(importCmd, "<output directory>", err)
					}
					log.Debug("output directory found, but remote it by force", "directory", flagOutput)
				} else {
					cmdcommon.PrintFlagsError(importCmd, "<output directory>", fmt.Errorf("directory, `%s` is not empty", flagOutput))
				}
			}
		}

		if storageConfig, err := storage.NewConfigFromString("file://" + flagOutput); err != nil {
			cmdcommon.PrintFlagsError(importCmd, "<output directory>", err)
		} else {
			if st, err := storage.NewStorage(storageConfig); err != nil {
				cmdcommon.PrintFlagsError(importCmd, "<output directory>", err)
			} else {
				stOutput = st
			}
		}
	}

	parsedFlags := []interface{}{}
	parsedFlags = append(parsedFlags, "\n\tlog-level", logLevel)
	parsedFlags = append(parsedFlags, "\n\tlog-format", flagLogFormat)
	parsedFlags = append(parsedFlags, "\n\tlog", flagLog)
	parsedFlags = append(parsedFlags, "\n\tforce", flagForce)
	parsedFlags = append(parsedFlags, "\n\tjson dump", flagSource)
	parsedFlags = append(parsedFlags, "\n\toutput", flagOutput)
	parsedFlags = append(parsedFlags, "\n", "")

	log.Debug("parsed flags:", parsedFlags...)
}

func importSourceFile(p string) {
	f, err := os.Open(p)
	if err != nil {
		cmdcommon.PrintFlagsError(
			importCmd,
			"<json dump directory>",
			fmt.Errorf("failed to read file: %s: %v", p, err),
		)
	}
	defer f.Close()

	fz, err := gzip.NewReader(f)
	if err != nil {
		f.Close()

		cmdcommon.PrintFlagsError(
			importCmd,
			"<json dump directory>",
			fmt.Errorf("failed to read gzipped file: %s: %v", p, err),
		)
	}
	fz.Close()

	var count int
	var item storage.IterItem
	r := bufio.NewReader(fz)
	for {
		var (
			isPrefix bool = true
			err      error
			l, b     []byte
		)

		for isPrefix && err == nil {
			l, isPrefix, err = r.ReadLine()
			b = append(b, l...)
		}
		if err == io.EOF {
			break
		}

		if err := json.Unmarshal(b, &item); err != nil {
			cmdcommon.PrintFlagsError(
				importCmd,
				"<json dump directory>",
				fmt.Errorf("failed to parse line: %s: `%s`: %v", p, string(b), err),
			)
		}

		if err := saveItemToOutput(dumpCmd, "", item); err != nil {
			cmdcommon.PrintFlagsError(
				importCmd,
				"<json dump directory>",
				fmt.Errorf("failed to save item: %s: `%s`: %v", p, string(b), err),
			)
		}
		if count%100000 == 0 {
			log.Debug("items loaded", "file", filepath.Base(p), "count", count)
		}
		count += 1
	}
}

func importSource() {
	files, err := ioutil.ReadDir(flagSource)
	if err != nil {
		cmdcommon.PrintFlagsError(importCmd, "<json dump directory>", err)
	}
	for _, f := range files {
		log.Debug("trying to load", "file", f.Name())
		importSourceFile(filepath.Join(flagSource, f.Name()))
		log.Debug("loaded", "file", f.Name())
	}

	stOutput.Close()

	log.Debug("finished")
}
