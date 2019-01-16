from pprint import pprint  # noqa
import argparse
import base64
import gzip
import json
import os  # noqa
import requests
import sys  # noqa
import tempfile
import time


prefixes = {
    'block-hash'                       : b'\x00',  # noqa
    'block-confirmed'                  : b'\x01',  # noqa
    'block-height'                     : b'\x02',  # noqa
    'block-transaction-hash'           : b'\x10',  # noqa
    'block-transaction-source'         : b'\x11',  # noqa
    'block-transaction-confirmed'      : b'\x12',  # noqa
    'block-transaction-account'        : b'\x13',  # noqa
    'block-transaction-block'          : b'\x14',  # noqa
    'block-operation-hash'             : b'\x20',  # noqa
    'block-operation-txhash'           : b'\x21',  # noqa
    'block-operation-source'           : b'\x22',  # noqa
    'block-operation-target'           : b'\x23',  # noqa
    'block-operation-peers'            : b'\x24',  # noqa
    'block-operation-typesource'       : b'\x25',  # noqa
    'block-operation-typetarget'       : b'\x26',  # noqa
    'block-operation-typepeers'        : b'\x27',  # noqa
    'block-operation-createfrozen'     : b'\x28',  # noqa
    'block-operation-frozenlinked'     : b'\x29',  # noqa
    'block-operation-blockheight'      : b'\x2a',  # noqa
    'block-account-address'            : b'\x30',  # noqa
    'block-account-created'            : b'\x31',  # noqa
    'block-account-sequenceid'         : b'\x32',  # noqa
    'block-account-sequenceidbyaddress': b'\x33',  # noqa
    'transaction-pool'                 : b'\x40',  # noqa
    'internal'                         : b'\x50',  # noqa
}

_prefixes_reverse = dict(map(lambda x: (x[1], x[0]), prefixes.items()))
HEADERS = {'content-type': 'application/json'}
PARSER = None
OPTIONS = None
SNAPSHOT = None
DB = None
LIMIT = 1000000000000000000


class dummy_db:
    def put(self, *a, **kw):
        pass

    def close(self, *a, **kw):
        pass


class JSONDump:
    output = None
    files = None

    def __init__(self, output):
        assert not os.path.exists(output)

        os.makedirs(output)
        self.output = output
        self.files = dict()

    def _get_file(self, key):
        prefix_name = _prefixes_reverse[key[:1]]
        try:
            f = self.files[prefix_name]
        except KeyError:
            f = gzip.open(
                os.path.join(self.output, prefix_name + '.json.gz'),
                mode='wb',
                compresslevel=1,  # best speed
            )
            self.files[prefix_name] = f

        return f

    def put(self, key, value, *a, **kw):
        f = self._get_file(key)

        f.write(json.dumps(dict(
            Key=base64.b64encode(key).decode('utf-8'),
            Value=base64.b64encode(value).decode('utf-8'),
        )).encode('utf-8'))
        f.write('\n'.encode('utf-8'))

    def close(self, *a, **kw):
        for _, f in self.files.items():
            f.close()

        return


def panic(*err):
    print('- error:', err)
    sys.exit(1)


def info(*a):
    print(*a)


def debug(*a):
    if not OPTIONS.verbose:
        return

    print(*a)

    return


def print_response(response):
    if response.status_code != 200:
        panic(response)

    # debug(response)
    js = response.json()

    return js


def print_flag_error(*a):
    PARSER.error(*a)
    sys.exit(1)


request_id = 1


def request(method, params):
    global request_id

    payload = {
        'method': method,
        'params': params,
        'id': request_id,
    }

    response = requests.post(OPTIONS.sebak, data=json.dumps(payload), headers=HEADERS)
    request_id += 1

    return print_response(response)


def fetch_items(prefix):
    start_time = time.time()

    prefix_name = _prefixes_reverse[prefix]

    debug('> starting %s' % prefix_name)
    count = 0
    cursor = None
    while True:
        params = (dict(
            snapshot=SNAPSHOT,
            prefix=prefix.decode('iso-8859-1'),
            options=dict(
                limit=LIMIT,
                reverse=False,
                cursor=base64.b64encode(cursor).decode('iso-8859-1') if cursor is not None else None,
            ),
        ),)

        js = request('DB.GetIterator', params)
        err = js['error']
        if err is not None:
            panic(err)

        result = js['result']['items']
        limit = js['result']['limit']
        for i in result:
            key = base64.b64decode(i['Key'])
            v = base64.b64decode(i['Value'])

            # debug('key=%s value=%s' % (key, v))
            DB.put(key, v, sync=True)

            if OPTIONS.verbose:
                if count > 0 and count % 100000 == 0:
                    debug('* put items: %s: %d' % (prefix_name, count))

            count += 1

        # debug('> len:', len(result))
        if len(result) < limit:
            break

        # debug('> cursor:', key)
        cursor = key

    debug('< done: %s: %d: %0.4fs' % (prefix_name, count, time.time() - start_time))

    return count


if __name__ == '__main__':
    PARSER = argparse.ArgumentParser(
        description='Dump sebak storage thru jsonrpc',
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    PARSER.add_argument('--format', choices=('leveldb', 'json'), default='json', help='verbose')
    PARSER.add_argument(
        '--dry-run',
        action='store_true',
        dest='dry_run',
        default=False,
        help='don\'t change anything',
    )
    PARSER.add_argument('--verbose', action='store_true', dest='verbose', default=False, help='verbose')
    PARSER.add_argument('sebak', help='sebak jsonrpc')
    PARSER.add_argument('output', help='output leveldb directory')

    OPTIONS = PARSER.parse_args()
    debug('- options:', OPTIONS)

    if OPTIONS.output is None or len(OPTIONS.output.strip()) < 1:
        PARSER.error('<output> is empty')
        sys.exit(1)

    if len(OPTIONS.sebak) < 1:
        print_flag_error('--sebak must be given')

    if not OPTIONS.dry_run:
        if OPTIONS.output is None:
            OPTIONS.output = tempfile.mkdtemp(suffix=None, prefix=None, dir=None)
        elif len(OPTIONS.output) < 1:
            print_flag_error('--output must be given')
        elif os.path.exists(OPTIONS.output) and len(os.listdir(OPTIONS.output)) > 0:
                print_flag_error('--output directory is not empty')

    if OPTIONS.dry_run:
        DB = dummy_db()
    else:
        if OPTIONS.format == 'leveldb':
            import plyvel
            DB = plyvel.DB(OPTIONS.output, create_if_missing=True)
        elif OPTIONS.format == 'json':
            DB = JSONDump(OPTIONS.output)

    # DB.OpenSnapshot method
    debug('-' * 30, 'DB.OpenSnapshot')
    js = request('DB.OpenSnapshot', tuple())
    SNAPSHOT = js['result']['snapshot']
    debug('- snapshot: %s' % SNAPSHOT)

    # DB.GetIterator method; all
    all_count = 0
    for _, prefix in prefixes.items():
        all_count += fetch_items(prefix)

    info('- total:', all_count)

    # DB.ReleaseSnapshot method
    debug('-' * 30, 'DB.ReleaseSnapshot')
    js = request('DB.ReleaseSnapshot', (dict(snapshot=SNAPSHOT,),))
    debug('- release snapshot: %s' % js)

    if not OPTIONS.dry_run:
        info('- dumped to', OPTIONS.output)

    DB.close()

    sys.exit(0)
