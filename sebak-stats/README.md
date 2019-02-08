# `sebak-stats`
`sebak-stats` builds the statistic information from sebak. The basic output format is csv.

## Informations

### Inflation
```
# initial balance, block inflation, pf inflation
```

### Total Supply
```
# block height, total supply
```

### Top Holders
```
# order,address,balance
```

### Frozen Accounts
```
# number of membership, number of frozen, total frozen amount, number of unfrozen, total unfrozen amount
```


## Example
```
go run sebak-stats/main.go \
    -sebak http://localhost:12345 \
    -sebak-jsonrpc http://localhost:54321/jsonrpc \
    -aws-access-key <aws access key> \
    -aws-secret-key <aws secret key> \
    -log-level debug \
    -region ap-northeast-2 \
    -s3-bucket <your bucket name> \
```

This will gather the statistic information and upload to the s3.


## Build

```
$ go build -o /tmp/sebak-stats sebak-stats/*.go
```
