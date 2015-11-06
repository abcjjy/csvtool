#!/usr/bin/python
import unicodecsv as csv
import argparse
import sys
import itertools
import collections

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('-H', '--header', action='store_true', help='Use first row as header')
    #ap.add_argument('-g', '--groupby', nargs='*', help='Group rows by columns')
    ap.add_argument('-f', '--filter', nargs='?', default="True", help='Python expression for filtering rows')
    ap.add_argument('-s', '--select', help='Python expression producing new row based on the input row')
    ap.add_argument('-a', '--aggregate', nargs=2, help='The 1st arg is expression for build the group key, the 2nd arg is python expression')
    ap.add_argument('-o', '--output', default=None, help='Output file name')
    ap.add_argument('-S', '--sort', help='Sort output expression')
    ap.add_argument('-t', '--outheader', help="Output headers", default='')
    ap.add_argument('-ie', '--inencoding', help='Input file encoding', default='utf-8')
    ap.add_argument('-d', '--delimiter', help='Input csv delimiter', default=',')
    ap.add_argument('input', help='input csv file or "-" for std input')

    args = ap.parse_args()
    instream = None
    if args.input == '-':
        instream = sys.stdin
    else:
        instream = open(args.input)
    
    incsv = csv.DictReader(instream, encoding=args.inencoding, delimiter=args.delimiter) if args.header else csv.reader(instream, encoding=args.inencoding, delimiter=args.delimiter)
    def fil(row):
        try:
            return eval(args.filter) and len(row)>0
        except Exception,e:
            return False

    rows = itertools.ifilter(fil, incsv)
    if args.select:
        mp = lambda row : eval(args.select)
        rows = itertools.imap(mp, rows)

    rows = process(args, rows)

    if args.sort:
        fv = lambda row: eval(args.sort)
        rows = sorted(rows, lambda x, y: cmp(fv(x), fv(y)))

    outs = open(args.output, 'w') if args.output else sys.stdout
    irows = iter(rows)
    try:
        row = next(irows)
        hdr = row.keys() if not args.outheader and type(row) == dict else args.outheader.split()
        outcsv = csv.DictWriter(outs, hdr) if type(row)==dict else csv.writer(outs)
        if type(row) == dict:
            outcsv.writeheader()
        while True:
            outcsv.writerow(row)
            row = next(irows)
    except StopIteration:
        pass
    outs.close();
    instream.close();

def pick(d, ks, m):
    if m:
        return {k:m(d[k]) for k in ks}
    else:
        return {k:d[k] for k in ks}

def median(l, key):
    return sorted(l, lambda x, y: cmp(x[key], y[key]))[len(l)/2][key]

def process(args, rows):
    if args.aggregate:
        key = lambda row: eval(args.aggregate[0])
        expr = lambda rows: eval(args.aggregate[1])
        grps = collections.defaultdict(list)
        for row in rows:
            grps[key(row)].append(row)
        for rs in grps.itervalues():
            yield expr(rs)
    else:
        for r in rows:
            yield r

if __name__ == '__main__':
    main()
