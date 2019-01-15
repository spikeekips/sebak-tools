package main

import (
	cmdcommon "boscoin.io/sebak/cmd/sebak/common"
)

func main() {
	if err := cmd.Execute(); err != nil {
		cmdcommon.PrintFlagsError(cmd, "", err)
	}
}
