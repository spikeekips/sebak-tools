# SEBAK Storage Dump & Import

## `sebak-storage`

Dump and import sebak storage. For more detail usage, run `sebak-storage`.

### Build

```
$ go build -o /tmp/sebak-storage sebak-storage/*.go
```

## `jsonrpc-dump.py`

```
$ python jsonrpc-dump.py  -h
usage: jsonrpc-dump.py [-h] [--format {leveldb,json}] [--dry-run] [--verbose]
                       sebak output

Dump sebak storage thru jsonrpc

positional arguments:
  sebak                 sebak jsonrpc
  output                output leveldb directory

optional arguments:
  -h, --help            show this help message and exit
  --format {leveldb,json}
                        verbose (default: json)
  --dry-run             don't change anything (default: False)
  --verbose             verbose (default: False)
```

### Usage

* `$ python jsonrpc-dump.py http://localhost:54321/jsonrpc /tmp/sebak-dumped`

This dumps storage data from `http://localhost:54321/jsonrpc` to `/tmp/sebak-dumped`. The dumped files are gzipped json files.


* `$ python jsonrpc-dump.py --format leveldb http://localhost:54321/jsonrpc /tmp/sebak-dumped`

The dumped result is leveldb format, but it is experimental and some problems are found.
