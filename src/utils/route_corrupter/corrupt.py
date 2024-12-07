from collections.abc import Callable, Iterable, Iterator

import capnp
import os, random, argparse, bz2, urllib, zstd, requests

# set original vals to test account
ACCOUNT_TOKEN = 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJleHAiOjE3NDg1ODI0NjUsIm5iZiI6MTcxNzA0NjQ2NSwiaWF0IjoxNzE3MDQ2NDY1LCJpZGVudGl0eSI6IjBkZWNkZGNmZGYyNDFhNjAifQ.g3khyJgOkNvZny6Vh579cuQj1HLLGSDeauZbfZri9jw'
DONGLE_ID = '1d3dc3e03047b0c7'
ROUTE_ID = '000000dd--455f14369d'

IMP_ROUTE_PATH = './imported_routes'
EXP_ROUTE_PATH = './exported_routes'
LOG_SCHEMA = os.path.join(os.path.dirname(os.path.abspath(__file__)),'schema')

# https://api.comma.ai/#route-segments
# https://github.com/commaai/openpilot/blob/master/tools/lib/logreader.py#L155

#----------------------------------------------------

class _LogFileReader:
    def __init__(self, fn, only_union_types=False, dat=None):
        self.data_version = None
        self._only_union_types = only_union_types

        ext = None
        if not dat:
            _, ext = os.path.splitext(urllib.parse.urlparse(fn).path)
            if ext not in ('', '.bz2', '.zst'):
                # old rlogs weren't compressed
                raise ValueError(f"unknown extension {ext}")

            with open(fn, 'rb') as f:
                dat = f.read()

        if ext == ".bz2" or dat.startswith(b'BZh9'):
            dat = bz2.decompress(dat)
        elif ext == ".zst" or dat.startswith(b'\x28\xB5\x2F\xFD'):
            dat = zstd.decompress(dat)

        def __iter__(self) -> Iterator[capnp._DynamicStructReader]:
            for ent in self._ents:
                if self._only_union_types:
                    try:
                        ent.which()
                        yield ent
                    except capnp.lib.capnp.KjException:
                        pass
                else:
                    yield ent

class _RetrieveOnlineLogFiles:
    def __init__(self, account_token=ACCOUNT_TOKEN, dongle=DONGLE_ID, route=ROUTE_ID):
        self.url = f'https://api.commadotai.com/v1/route/{dongle}|{route}/files'
        self.route = route
        self.api_key = account_token

    def req_files(self, index, fileName):
        response = requests.get(
            self.url,
            headers={ 'Authorization': f'JWT {self.api_key}' }
        )
        if response.status_code == 200:
            # save to route folder
            dir = f'{IMP_ROUTE_PATH}/{self.route}--{index}/'
            # create directory if it doesnt exist
            os.makedirs(os.path.dirname(dir), exist_ok=True)
            # join dir and append filename then write the data to it
            fn = os.path.join(dir, fileName)
            with open(fn, 'wb') as f:
                for chunk in response.iter_content(chunk_size=8192):
                    f.write(chunk)
            return fn
        else:
            raise Exception(f'Error code {response.status_code}, {response.text}')

