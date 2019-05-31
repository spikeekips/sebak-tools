package main

import (
	"bytes"
	"compress/flate"
	"compress/gzip"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"

	jsonrpc "github.com/gorilla/rpc/json"
	logging "github.com/inconshreveable/log15"
	isatty "github.com/mattn/go-isatty"
	"github.com/spf13/cobra"

	cmdcommon "boscoin.io/sebak/cmd/sebak/common"
	"boscoin.io/sebak/lib/common"
	"boscoin.io/sebak/lib/node/runner"
	"boscoin.io/sebak/lib/storage"
)

func parseLogging(cmd *cobra.Command) {
	var err error
	if logLevel, err = logging.LvlFromString(flagLogLevel); err != nil {
		cmdcommon.PrintFlagsError(cmd, "--log-level", err)
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
		cmdcommon.PrintFlagsError(cmd, "--log-format", fmt.Errorf("'%s'", flagLogFormat))
	}

	logHandler := logging.StreamHandler(os.Stdout, logFormatter)
	if len(flagLog) > 0 {
		if logHandler, err = logging.FileHandler(flagLog, logFormatter); err != nil {
			cmdcommon.PrintFlagsError(cmd, "--log", err)
		}
	}

	logHandler = logging.CallerFileHandler(logHandler)
	logHandler = logging.LvlFilterHandler(logLevel, logHandler)
	log.SetHandler(logHandler)
}

func checkSource(s string) error {
	log.Debug("checking source", "source", s)

	u, err := url.Parse(s)
	if err != nil {
		return err
	}

	switch strings.ToLower(u.Scheme) {
	case "http", "https":
		log.Debug("source is jsonrpc-based", "parsed", u)
		if err := checkSourceJSONRpc(s); err != nil {
			log.Error("failed to check jsonrpc endpoint", "error", err)
			return err
		}
	default:
		log.Debug("source is file-based", "parsed", u)
		if st, err := checkSourceDirectory(s); err != nil {
			log.Error("failed to check file-based source", "error", err)
			return err
		} else {
			stSource = st
		}
	}

	return nil
}

func checkSourceJSONRpc(s string) error {
	endpoint, err := common.ParseEndpoint(s)
	if err != nil {
		return err
	}

	jsonRPCEndpoint = endpoint

	args := runner.DBEchoArgs(common.NowISO8601())
	resp, err := request("DB.Echo", &args)
	if err != nil {
		return err
	}

	defer resp.Body.Close()

	var result runner.DBEchoResult
	if err := jsonrpc.DecodeClientResponse(resp.Body, &result); err != nil {
		return err
	}

	return nil
}

func currentDirectory() (string, error) {
	var c string
	var err error
	if c, err = os.Getwd(); err != nil {
		return c, err
	}

	return filepath.Abs(c)
}

func checkSourceDirectory(s string) (*storage.LevelDBBackend, error) {
	var source string
	if strings.HasPrefix(s, "file://") {
		source = s[7:]
	} else if !strings.HasPrefix(s, "/") {
		if c, err := currentDirectory(); err != nil {
			return nil, err
		} else {
			source = c + "/" + s
		}
	} else {
		source = s
	}

	if _, err := os.Stat(source); os.IsNotExist(err) {
		return nil, err
	} else {
		d, err := os.Open(source)
		if err != nil {
			return nil, err
		}
		defer d.Close()

		if _, err = d.Readdirnames(1); err == io.EOF {
			return nil, fmt.Errorf("db source source, `%source` is empty", source)
		}
	}

	if storageConfig, err := storage.NewConfigFromString("file://" + source); err != nil {
		return nil, err
	} else {
		if st, err := storage.NewStorage(storageConfig); err != nil {
			return nil, err
		} else {
			return st, nil
		}
	}
}

func request(method string, args interface{}) (*http.Response, error) {
	message, err := jsonrpc.EncodeClientRequest(method, &args)
	if err != nil {
		return nil, err
	}

	req, err := http.NewRequest("POST", jsonRPCEndpoint.String(), bytes.NewBuffer(message))
	if err != nil {
		return nil, err
	}

	req.Header.Set("Content-Type", "application/json")
	client := new(http.Client)
	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}

	if resp.StatusCode != 200 {
		return nil, fmt.Errorf("failed to get response: status=%v", resp.StatusCode)
	}

	return resp, nil
}

func shortLogValue(s []byte) []byte {
	if len(s) < 10 {
		return s
	}

	return s[:10]
}

type GzipWriter struct {
	f  *os.File
	gw *gzip.Writer
}

func NewGzipWriter(name string) (*GzipWriter, error) {
	f, err := os.OpenFile(
		name+".gz",
		os.O_CREATE|os.O_WRONLY,
		0644,
	)
	if err != nil {
		return nil, err
	}

	gw, _ := gzip.NewWriterLevel(f, flate.BestSpeed)
	return &GzipWriter{f: f, gw: gw}, nil
}

func (g *GzipWriter) Write(b []byte) (int, error) {
	return g.gw.Write(b)
}

func (g *GzipWriter) Close() error {
	if err := g.gw.Flush(); err != nil {
		return err
	}

	if err := g.gw.Close(); err != nil {
		return err
	}

	if err := g.f.Close(); err != nil {
		return err
	}

	return nil
}

type ListFlags []string

func (i *ListFlags) Type() string {
	return "list"
}

func (i *ListFlags) String() string {
	return strings.Join([]string(*i), " ")
}

func (i *ListFlags) Set(value string) error {
	*i = append(*i, value)
	return nil
}

func saveItemToOutput(cmd *cobra.Command, prefix string, item storage.IterItem) error {
	if stOutput != nil {
		return stOutput.Core.Put(item.Key, item.Value, nil)
	}

	if jsonFiles != nil {
		var err error
		var f *GzipWriter
		l, found := jsonFiles.Load(prefix)
		if found {
			f = l.(*GzipWriter)
		} else {
			f, err = NewGzipWriter(filepath.Join(flagOutput, allPrefixesWithName[prefix]) + ".json")
			if err != nil {
				cmdcommon.PrintError(cmd, fmt.Errorf("failed to create GzipWriter: %v", err))
			}
			jsonFiles.Store(prefix, f)
		}

		var b []byte
		if b, err = json.Marshal(item); err != nil {
			cmdcommon.PrintError(cmd, fmt.Errorf("failed to marshal storage.Item: %v", err))
		}
		f.Write(append(b, []byte("\n")...))
	}

	return nil
}
