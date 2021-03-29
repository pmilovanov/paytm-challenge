import csv
from typing import Type

import apache_beam as beam

OUT_PARSED = "parsed"
OUT_INVALID = "invalid"


def parse_csv_kv_row(keytype: type, valtype: type, keylen: int = -1, vallen: int = -1):
    def fn(line: str):
        parts = line.split(",")
        assert len(parts) == 2
        if keylen >= 0:
            assert len(parts[0]) == keylen
        if vallen >= 0:
            assert len(parts[1]) == vallen
        return (keytype(parts[0]), valtype(parts[1]))

    return fn


def parse_weather_entry(line: str):
    for row in csv.reader([line]):
        assert len(row) == 16


class ParseFn(beam.DoFn):
    def __init__(self, parser):
        self.parser = parser

    def process(self, element: str, **kwargs):
        try:
            yield beam.pvalue.TaggedOutput(OUT_PARSED, self.parser(element))
        except:
            yield beam.pvalue.TaggedOutput(OUT_INVALID, element)
