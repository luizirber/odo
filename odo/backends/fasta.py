from __future__ import absolute_import, division, print_function

from toolz.curried import map, take, pipe, pluck, get, concat, filter
from collections import Iterator, Iterable
import os
from contextlib import contextmanager

from datashape import discover, var, dshape, Record, DataShape
from datashape import coretypes as ct
from datashape.dispatch import dispatch
import gzip
import datetime
import uuid
from ..append import append
from ..convert import convert, ooc_types
from ..resource import resource
from ..chunks import chunks
from ..temp import Temp
from ..drop import drop
from ..utils import tuples_to_records


class FASTA(object):
    """ Proxy for a FASTA file

    Parameters
    ----------

    path : str
        Path to file on disk

    """
    canonical_extension = 'fasta'

    def __init__(self, path, **kwargs):
        self.path = path


@discover.register(FASTA)
def discover_fasta(f, **kwargs):
    data = fasta_load(f.path)
    return discover(data)


def fasta_load(path, **kwargs):
    """ Return data of a fasta file

    Handles compression like gzip """
    if path.split(os.path.extsep)[-1] == 'gz':
        f = gzip.open(path)
        s = f.read().decode('utf-8')
    else:
        f = open(path)
        s = f.read()
    f.close()

    data = []
    seq = {}
    for line in s.split('\n'):
        line = line.strip()
        if line.startswith(">"):
            if seq:
                seq["sequence"] = "".join(seq["sequence"])
                data.append(seq)
                seq = {}
            metadata = line[1:].split(" ")
            seq['name'] = metadata[0]
            seq['description'] = " ".join(metadata[1:])
            seq['sequence'] = []
        else:
            if line:
                seq['sequence'].append(line)
    if seq:
        seq["sequence"] = "".join(seq["sequence"])
        data.append(seq)

    return data


@append.register(FASTA, list)
def list_to_fasta(j, seq, dshape=None, **kwargs):
    text = fasta_dumps(seq)

    if j.path.split(os.path.extsep)[-1] == 'gz':
        f = gzip.open(j.path, 'wb')
        text = text.encode('utf-8')
    else:
        f = open(j.path, 'w')

    f.write(text)

    f.close()

    return j


@append.register(FASTA, object)
def object_to_fasta(j, o, **kwargs):
    return append(j, convert(list, o, **kwargs), **kwargs)


@resource.register('fasta://.*\.fasta(\.gz)?', priority=11)
def resource_fasta(path, **kwargs):
    if 'fasta://' in path:
        path = path[len('fasta://'):]
    return FASTA(path)


@resource.register('.*\.fasta(\.gz)?')
def resource_fasta_ambiguous(path, **kwargs):
    """ Try to guess if this file is line-delimited or not """
    return resource_fasta(path, **kwargs)


@convert.register(chunks(list), (chunks(FASTA), chunks(Temp(FASTA))))
def convert_glob_of_fastas_into_chunks_of_lists(fastas, **kwargs):
    def _():
        return concat(convert(chunks(list), fs, **kwargs) for fs in fastas)
    return chunks(list)(_)


@convert.register(Temp(FASTA), list)
def list_to_temporary_fasta(data, **kwargs):
    fn = '.%s.fasta' % uuid.uuid1()
    target = Temp(FASTA)(fn)
    return append(target, data, **kwargs)


@drop.register(FASTA)
def drop_fasta(fs):
    if os.path.exists(fs.path):
        os.remove(fs.path)


def fasta_dumps(data):
    text = []
    for seq in data:
        text.append(">{name} {description}\n{sequence}\n".format(**seq))
    text = "".join(text)

    return text


ooc_types.add(FASTA)
