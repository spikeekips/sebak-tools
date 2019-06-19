package main

import (
	"fmt"
	"os"

	cmdcommon "boscoin.io/sebak/cmd/sebak/common"
	"github.com/spf13/cobra"

	"boscoin.io/sebak/lib/block"
	"boscoin.io/sebak/lib/storage"
)

func main() {
	var cmd *cobra.Command

	cmd = &cobra.Command{
		Use:   "sebak-latest-block <leveldb directory>",
		Short: "sebak-latest-block",
		Run: func(c *cobra.Command, args []string) {
			var stOutput *storage.LevelDBBackend
			if storageConfig, err := storage.NewConfigFromString("file://" + os.Args[1]); err != nil {
				cmdcommon.PrintFlagsError(cmd, "<output directory>", err)
			} else {
				if st, err := storage.NewStorage(storageConfig); err != nil {
					cmdcommon.PrintFlagsError(cmd, "<output directory>", err)
				} else {
					stOutput = st
					defer stOutput.Close()
				}
			}

			bk := block.GetLatestBlock(stOutput)
			fmt.Println(bk)
		},
	}

	if err := cmd.Execute(); err != nil {
		cmdcommon.PrintFlagsError(cmd, "", err)
	}

	os.Exit(0)
}
