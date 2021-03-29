import csv
import datetime
import typing
from typing import Type

import apache_beam as beam
from apache_beam.transforms.window import TimestampedValue

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


class WeatherEntry(typing.NamedTuple):
    stn: int
    obsdate: datetime.datetime
    temp: float
    windspeed: float
    tornado_or_funnel: bool

def parse_weather_entry(line: str):
    for row in csv.reader([line]):
        assert len(row) == 16
        stn, yearmoda, temp, windspeed, frshtt = row[0], row[2], row[3], row[8], row[15]
        assert len(frshtt) == 6
        if temp == "9999.9":
            temp = "-1.0"
        if windspeed == "999.9":
            windspeed = "-1.0"
        return WeatherEntry(
            int(stn),
            datetime.datetime.strptime(yearmoda, "%Y%m%d"),
            float(temp),
            float(windspeed),
            frshtt[5] == "1",
        )

class ParseFn(beam.DoFn):
    def __init__(self, parser):
        self.parser = parser

    def process(self, element: str, **kwargs):
        try:
            yield beam.pvalue.TaggedOutput(OUT_PARSED, self.parser(element))
        except:
            yield beam.pvalue.TaggedOutput(OUT_INVALID, element)

class EntryAddTimestampFn(beam.DoFn):
    def process(self, element: WeatherEntry, *args, **kwargs):
        yield TimestampedValue(element, int(element.obsdate.timestamp()))

