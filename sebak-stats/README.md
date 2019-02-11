# `sebak-stats`
`sebak-stats` builds the statistic information from sebak. The basic output format is csv.

## Stats

The list data are formatted as *CSV**.

### frozen-accounts.txt

Frozen accounts

```
# number of membership, number of frozen, total frozen amount, number of unfrozen, total unfrozen amount
1726,4260,281990000.0000000,27,420000.0000000
```

### latest-block.txt

Latest block to be used

### top-holders.txt

Top holder list, it is ordered by balance.

```
# order,address,balance
0,GCD2K7NFW6IBLSLYX5IZMYVVN2ETASI674Q4V4VAPHBIHRXXBTUWKTXT,144089280.1028540
1,GCPQQIX2LRX2J63C7AHWDXEMNGMZR2UI2PRN5TCSOVMEMF7BAUADMKH5,62550474.3890000
2,GC7KIQFUL4Z7OKOMDRPUEBJNOY2V2SR2BKQVK722GRUCQAYW4XASUQ6H,40000005.0972105
3,GBYVEZGOMQGBCHXSVFVNZIHNPD6JWS6T4KXK64JQGCR7YQK63ISD22R7,34596524.3744575
4,GCJCVV63I7XLCNQBJFAF3KDFVL7CQXIBNFFWTKXGRCAYX3U2GDBGJKC4,12000001.5291626
5,GDISLQZIN3PLBHLEM5WNZ3D3WLKH3BXEMEO6KGQLIDLB7LPF7MNJB4VL,9140000.0000000
...
```

### top-holders-3000.txt
Top holder list up to 3000, it is ordered by balance.

```
# order,address,balance
0,GCD2K7NFW6IBLSLYX5IZMYVVN2ETASI674Q4V4VAPHBIHRXXBTUWKTXT,144089280.1028540
1,GCPQQIX2LRX2J63C7AHWDXEMNGMZR2UI2PRN5TCSOVMEMF7BAUADMKH5,62550474.3890000
2,GC7KIQFUL4Z7OKOMDRPUEBJNOY2V2SR2BKQVK722GRUCQAYW4XASUQ6H,40000005.0972105
3,GBYVEZGOMQGBCHXSVFVNZIHNPD6JWS6T4KXK64JQGCR7YQK63ISD22R7,34596524.3744575
4,GCJCVV63I7XLCNQBJFAF3KDFVL7CQXIBNFFWTKXGRCAYX3U2GDBGJKC4,12000001.5291626
5,GDISLQZIN3PLBHLEM5WNZ3D3WLKH3BXEMEO6KGQLIDLB7LPF7MNJB4VL,9140000.0000000
...
```

### total-inflation.txt

Inflation data

```
# initial balance, block inflation, pf inflation
500000000.0000000,62550450.0000000,160833600.0000000
```

### total-supply.txt
Total supply amount

```
723384050.0000000
```

### total-supply-details.txt

Total supply amount with block height

```
# block height, total supply
1251010,723384050.0000000
```

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


## Usage

```
$ sebak-stats -h
Usage of sebak-stats  <secret seed> <accounts>
  -aws-access-key string
    	aws access key
  -aws-secret-key string
    	aws secret key
  -dry-run
    	dry-run
  -init
    	initialize
  -log string
    	set log file
  -log-format string
    	log format, {terminal, json} (default "terminal")
  -log-level string
    	log level, {crit, error, warn, info, debug} (default "info")
  -region string
    	s3 region (default "ap-northeast-2")
  -s3-acl string
    	s3 acl; {public-read} (default "public-read")
  -s3-bucket string
    	s3 bucket name
  -s3-path string
    	s3 file path
  -sebak string
    	sebak endpoint (default "http://127.0.0.1:12345")
  -sebak-jsonrpc string
    	sebak jsonrpc (default "http://127.0.0.1:54321/jsonrpc")
  -top-holders-limit int
    	limit for number of top holders (default 3000)
```

### `-init`

`-init` will start from genesis block and does not concern the latest aggregated data from s3.


### `-dry-run`

`-dry-run` does not upload data to s3, just will save them in temp directory.


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
