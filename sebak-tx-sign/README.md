# sebak-tx-sign

Sign the existing tx message again with the other secret seed.

## Usage

At first create new keypair.
```
$ sebak key generate
       Secret Seed: SCLVWAGTDVWORKEL3GIAJWJHGNHK2YOBHBAXSMU43XK234RYMNDXFDDD
    Public Address: GCHCK63DIJXCDGCB5W54CPX2YJMIPTUOVX6MOLO5C2W4GXGMIBCYSIAU
```

```
$ go run sebak-tx-sign/main.go SCLVWAGTDVWORKEL3GIAJWJHGNHK2YOBHBAXSMU43XK234RYMNDXFDDD '{
  "T": "transaction",
  "H": {
    "version": "",
    "created": "2018-09-27T14:27:28.050051320+09:00",
    "signature": "3bY1S6FBXZcZP4AXM2X9KXuxpXcecmVwyX9xeJydnsXiFeBYW5Z7fccVhJUE8v87tk4dyaLHGt8oPUE2Va3zByoU"
  },
  "B": {
    "source": "GDTEPFWEITKFHSUO44NQABY2XHRBBH2UBVGJ2ZJPDREIOL2F6RAEBJE4",
    "fee": "10000",
    "sequenceid": 0,
    "operations": [
      {
        "H": {
          "type": "payment"
        },
        "B": {
          "target": "GDIRF4UWPACXPPI4GW7CMTACTCNDIKJEHZK44RITZB4TD3YUM6CCVNGJ",
          "amount": "990001"
        }
      }
    ]
  }
}'
< original ======================================================================
{
  "T": "transaction",
  "H": {
    "version": "",
    "created": "2018-09-27T14:27:28.050051320+09:00",
    "signature": "3bY1S6FBXZcZP4AXM2X9KXuxpXcecmVwyX9xeJydnsXiFeBYW5Z7fccVhJUE8v87tk4dyaLHGt8oPUE2Va3zByoU"
  },
  "B": {
    "source": "GDTEPFWEITKFHSUO44NQABY2XHRBBH2UBVGJ2ZJPDREIOL2F6RAEBJE4",
    "fee": "10000",
    "sequenceid": 0,
    "operations": [
      {
        "H": {
          "type": "payment"
        },
        "B": {
          "target": "GDIRF4UWPACXPPI4GW7CMTACTCNDIKJEHZK44RITZB4TD3YUM6CCVNGJ",
          "amount": "990001"
        }
      }
    ]
  }
}
> signed ========================================================================
{
  "T": "transaction",
  "H": {
    "version": "",
    "created": "2018-09-27T14:27:28.050051320+09:00",
    "signature": "2XQUQ4Su6mgn6aFZhWXq5isJYd7fnySEQ4oKXakwHbiMT8hXdWcrDDL85wmHrAUvqVrZRc4eUArithnrFDdNHm3S"
  },
  "B": {
    "source": "GCHCK63DIJXCDGCB5W54CPX2YJMIPTUOVX6MOLO5C2W4GXGMIBCYSIAU",
    "fee": "10000",
    "sequenceid": 0,
    "operations": [
      {
        "H": {
          "type": "payment"
        },
        "B": {
          "target": "GDIRF4UWPACXPPI4GW7CMTACTCNDIKJEHZK44RITZB4TD3YUM6CCVNGJ",
          "amount": "990001"
        }
      }
    ]
  }
}
```

Or it can receive the stdin too.
```
$ echo '{
  "T": "transaction",
  "H": {
    "version": "",
    "created": "2018-09-27T14:27:28.050051320+09:00",
    "signature": "3bY1S6FBXZcZP4AXM2X9KXuxpXcecmVwyX9xeJydnsXiFeBYW5Z7fccVhJUE8v87tk4dyaLHGt8oPUE2Va3zByoU"
  },
  "B": {
    "source": "GDTEPFWEITKFHSUO44NQABY2XHRBBH2UBVGJ2ZJPDREIOL2F6RAEBJE4",
    "fee": "10000",
    "sequenceid": 0,
    "operations": [
      {
        "H": {
          "type": "payment"
        },
        "B": {
          "target": "GDIRF4UWPACXPPI4GW7CMTACTCNDIKJEHZK44RITZB4TD3YUM6CCVNGJ",
          "amount": "990001"
        }
      }
    ]
  }
}' | go run sebak-tx-sign/main.go SCLVWAGTDVWORKEL3GIAJWJHGNHK2YOBHBAXSMU43XK234RYMNDXFDDD
```
