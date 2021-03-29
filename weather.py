import csv
import datetime
import os
import typing
from typing import Type

import apache_beam as beam
from apache_beam import PTransform
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.transforms.window import TimestampedValue

OUT_PARSED = "parsed"
OUT_INVALID = "invalid"


def parse_csv_kv_row(keytype: type, valtype: type, keylen: int = -1, vallen: int = -1):
    def fn(line: str):
        parts = line.split(",")

        # Key is before the first comma, everything else is after
        # To handle stuff like "NK,KOREA, NORTH"
        assert len(parts) >= 2
        k = parts[0]
        v = parts[1]
        if len(parts) > 2:
            v = ",".join(parts[1:])

        if keylen >= 0:
            assert len(k) == keylen
        if vallen >= 0:
            assert len(v) == vallen
        return (keytype(k), valtype(v))

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


class WeatherPipelineOptions(PipelineOptions):
    @classmethod
    def _add_argparse_args(cls, parser):
        parser.add_argument(
            "--input_weather_dir", help="Dir with *.csv files with weather data"
        )
        parser.add_argument("--input_stationlist", help="stationlist file")
        parser.add_argument("--input_countrylist", help="countrylist file")
        parser.add_argument("--outputdir", help="Dir to output data")


def _printer(label: str):
    def fn(x):
        print("%s: %s" % (label, x))

    return fn


class Printer(PTransform):
    def __init__(self, label: str):
        self.label = label

    def expand(self, input_or_inputs):
        return input_or_inputs | self.label >> beam.Map(_printer(self.label))


def run():

    opts = PipelineOptions()

    with beam.Pipeline(options=opts) as p:

        opts = opts.view_as(WeatherPipelineOptions)

        weather_entries, weather_badrows = (
            p
            | "Read weather entries" >> beam.io.ReadFromText(os.path.join(opts.input_weather_dir, "*.csv"))
            | "Parse weather" >> beam.ParDo(ParseFn(parse_weather_entry)).with_outputs("parsed", "invalid")
        )

        _ = (
            weather_badrows
            | "Log invalid weather rows"
            >> beam.io.WriteToText(os.path.join(opts.outputdir, "invalid_input_rows"))
        )

        country_entries, country_badrows = (
            p
            | "Read countries" >> beam.io.ReadFromText(opts.input_countrylist)
            | "Parse countries" >> beam.ParDo(ParseFn(parse_csv_kv_row(str, str, 2))).with_outputs("parsed", "invalid")
        )
        station_entries, station_badrows = (
                p
                | "Read stations" >> beam.io.ReadFromText(opts.input_stationlist)
                | "Parse stations" >> beam.ParDo(ParseFn(parse_csv_kv_row(int, str, 6, 2))).with_outputs("parsed", "invalid")
        )

        _ = (
                country_badrows
                | "Log invalid country rows"
                >> beam.io.WriteToText(os.path.join(opts.outputdir, "invalid_country_rows"))
        )
        _ = (
                station_badrows
                | "Log invalid station rows"
                >> beam.io.WriteToText(os.path.join(opts.outputdir, "invalid_station_rows"))
        )



if __name__ == "__main__":
    run()
