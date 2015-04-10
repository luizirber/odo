from __future__ import absolute_import, division, print_function

from odo.backends.fasta import *
from odo.utils import tmpfile, ignoring
from odo import into
from odo.temp import Temp, _Temp
from contextlib import contextmanager
from datashape import dshape
import datetime
import os
import gzip
import os
import json

@contextmanager
def fasta_file(data):
    with tmpfile('.fasta') as fn:
        with open(fn, 'w') as f:
            for seq in data:
                f.write(">{name} {description}\n{sequence}\n".format(**seq))
        yield fn

dat = [{'name': 'chr1', 'description': 'human', "sequence": "ACGT"},
       {'name': 'chr4', 'description': 'mouse', "sequence": "TCAG"}]


def test_discover_fasta():
    with fasta_file(dat) as fn:
        j = FASTA(fn)
        assert discover(j) == discover(dat)


def test_resource():
    with tmpfile('fasta') as fn:
        assert isinstance(resource('json://' + fn), FASTA)


def test_append_fasta():
    with tmpfile('fasta') as fn:
        j = FASTA(fn)
        append(j, dat)
        with open(j.path) as f:
            lines = f.readlines()
        assert len(lines) == 4
        assert '>chr1 human' in lines[0]
        assert '>chr4 mouse' in lines[2]


def test_convert_fasta_list():
    with fasta_file(dat) as fn:
        j = FASTA(fn)
        assert convert(list, j) == dat


def test_read_gzip():
    with tmpfile('json.gz') as fn:
        f = gzip.open(fn, 'wb')
        text = fasta_dumps(dat)
        f.write(text.encode('utf-8'))
        f.close()
        js = FASTA(fn)
        assert convert(list, js) == dat


def test_write_gzip():
    with tmpfile('json.gz') as fn:
        j = FASTA(fn)
        append(j, dat)

        f = gzip.open(fn)
        text = f.read()
        f.close()
        assert text.decode('utf-8') == fasta_dumps(dat)


def test_resource_gzip():
    with tmpfile('fasta.gz') as fn:
        assert isinstance(resource(fn), FASTA)
        assert isinstance(resource('fasta://' + fn), FASTA)


def test_convert_to_temp_fasta():
    js = convert(Temp(FASTA), [
        {'name': 'chr1', 'description': 'human', 'sequence': 'GTAT'},
        {'name': 'chr4', 'description': 'mouse', 'sequence': 'TTCA'}
    ])
    assert isinstance(js, FASTA)
    assert isinstance(js, _Temp)

    assert convert(list, js) == [
        {'name': 'chr1', 'description': 'human', 'sequence': 'GTAT'},
        {'name': 'chr4', 'description': 'mouse', 'sequence': 'TTCA'}
    ]


def test_drop():
    with tmpfile('fasta') as fn:
        js = FASTA(fn)
        append(js, [
            {'name': 'chr2', 'description': 'human', 'sequence': 'TTGG'},
            {'name': 'chr5', 'description': 'mouse', 'sequence': 'CAGG'}
        ])

        assert os.path.exists(fn)
        drop(js)
        assert not os.path.exists(fn)
