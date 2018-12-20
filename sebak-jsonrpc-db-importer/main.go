package main

import (
	"bufio"
	"compress/gzip"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"
	"time"

	"boscoin.io/sebak/lib/storage"
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
	if flag.NArg() < 1 {
		printError("exported directory is missing", nil)
	}

	if flag.NArg() < 2 {
		printError("restore directory is missing", nil)
	}

	baseDirectory := flag.Arg(0)
	files, err := ioutil.ReadDir(baseDirectory)
	if err != nil {
		printError("failed to read directory", err)
	}

	storagePath, err := filepath.Abs(flag.Arg(1))
	if err != nil {
		printError("failed to get absolute path", err)
	}
	if err := os.MkdirAll(storagePath, os.ModePerm); err != nil {
		printError("failed to create the restore directory", err)
	}

	storageConfig, err := storage.NewConfigFromString("file://" + storagePath)
	if err != nil {
		printError("failed to initialize storage", err)
	}

	st, err := storage.NewStorage(storageConfig)
	if err != nil {
		printError("failed to initialize storage", err)
	}

	defer st.Close()

	started := time.Now()
	defer func() {
		fmt.Printf("done: %v\n", time.Now().Sub(started))
		os.Exit(0)
	}()

	for _, f := range files {
		if strings.HasSuffix(f.Name(), ".txt") {
			continue
		}

		p := filepath.Join(baseDirectory, f.Name())
		if fi, err := os.Stat(p); err != nil {
			printError("failed to access file", err)
		} else if fi.IsDir() {
			continue
		}

		startedFile := time.Now()

		f, err := os.Open(p)
		if err != nil {
			printError("failed to open file", err)
		}

		var zr io.Reader
		if strings.HasSuffix(f.Name(), ".gz") {
			var err error
			zr, err = gzip.NewReader(f)
			if err != nil {
				printError("failed to open gzipped file", err)
			}
		} else {
			zr = f
		}

		r := bufio.NewReader(zr)

		fmt.Printf("< %s", p)
		for {
			var (
				isPrefix bool  = true
				err      error = nil
				line, ln []byte
			)
			for isPrefix && err == nil {
				line, isPrefix, err = r.ReadLine()
				ln = append(ln, line...)
			}
			if err != nil && err != io.EOF {
				printError("failed to read file", err)
			}
			if err == io.EOF {
				break
			}
			if len(ln) < 1 {
				continue
			}

			var item storage.IterItem
			if err := json.Unmarshal(ln, &item); err != nil {
				printError("failed to load data", err)
			}

			if err := st.Core.Put(item.Key, item.Value, nil); err != nil {
				printError("failed to put data", err)
			}
		}
		fmt.Printf(": %v\n", time.Now().Sub(startedFile))
		break
	}
}
