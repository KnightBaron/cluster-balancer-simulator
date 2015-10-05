import csv
import gzip
from itertools import izip_longest


class GZipCSVReader:
    """
    Gzipped CSV Reader

    Taken from http://stackoverflow.com/questions/9252812/using-csvreader-against-a-gzipped-file-in-python
    """
    def __init__(self, filename, fieldnames=[]):
        self.gzfile = gzip.open(filename)
        self.reader = csv.DictReader(self.gzfile, fieldnames)

    def next(self):
        return self.reader.next()

    def close(self):
        self.gzfile.close()

    def __iter__(self):
        return self.reader.__iter__()


def grouper(iterable, n, fillvalue=None):
    """
    grouper('ABCDEFG', 3, 'x') --> ABC DEF Gxx

    Taken from https://docs.python.org/2.7/library/itertools.html#recipes
    """
    "Collect data into fixed-length chunks or blocks"
    args = [iter(iterable)] * n
    return izip_longest(fillvalue=fillvalue, *args)


def merge_two_dicts(x, y):
    """
    Given two dicts, merge them into a new dict as a shallow copy.

    Taken from http://stackoverflow.com/questions/38987/how-can-i-merge-two-python-dictionaries-in-a-single-expression
    """
    z = x.copy()
    z.update(y)
    return z
