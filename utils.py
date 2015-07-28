import csv
import gzip


class GZipCSVReader:
    """
    Gzipped CSV Reader

    Taken from http://stackoverflow.com/questions/9252812/using-csvreader-against-a-gzipped-file-in-python
    """
    def __init__(self, filename):
        self.gzfile = gzip.open(filename)
        self.reader = csv.DictReader(self.gzfile)

    def next(self):
        return self.reader.next()

    def close(self):
        self.gzfile.close()

    def __iter__(self):
        return self.reader.__iter__()
