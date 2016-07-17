#!/usr/bin/env python
# -*- coding: utf-8 -*-

import csv
import argparse


def parse_command_line():
    """Parse command line arguments
    """
    parser = argparse.ArgumentParser(description="Demographics data to Inputs")

    parser.add_argument('input', type=str, help='Demographics CSV file')
    parser.add_argument("-o", "--output", type=str, default="inputs.csv",
                        help="ZIP2WD CSV input file (default: 'inputs.csv')")
    parser.add_argument('year', type=int, help='Year to be query')

    return parser.parse_args()


if __name__ == "__main__":
    args = parse_command_line()

    zipcode = set()

    with open(args.input) as f:
        reader = csv.DictReader(f)
        for r in reader:
            zipcode.add(r['zip_code'])

    with open(args.output, 'wb') as o:
        writer = csv.DictWriter(o, fieldnames=['uniqid', 'zip',
                                               'from.year', 'from.month',
                                               'from.day', 'to.year',
                                               'to.month', 'to.day'])
        writer.writeheader()
        for i, z in enumerate(zipcode):
            r = {'uniqid': i,
                 'zip': z,
                 'from.year': args.year,
                 'from.month': '01',
                 'from.day': '01',
                 'to.year': args.year,
                 'to.month': '12',
                 'to.day': '31'}
            writer.writerow(r)
        print("Total unique zip code = {:d}".format(i))
