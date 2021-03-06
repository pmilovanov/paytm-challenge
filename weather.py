import csv
import datetime
import os
import typing

import apache_beam as beam
import numpy
from apache_beam import PTransform
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.transforms.window import TimestampedValue
from numpy import mean

OUT_PARSED = "parsed"
OUT_INVALID = "invalid"


# Run like so:
#
# > python weather.py --input_weather_dir=data/2019 \
#                     --input_stationlist=stationlist.csv \
#                     --input_countrylist=countrylist.csv --outputdir=/tmp/paytm


def parse_csv_kv_row(keytype: type, valtype: type, keylen: int = -1, vallen: int = -1):
    def fn(line: str):
        parts = line.split(",")

        # Key is before the first comma, everything else is after
        # To handle stuff like "NK,KOREA, NORTH"
        assert len(parts) >= 2
        k = parts[0].strip()
        v = parts[1].strip()
        if len(parts) > 2:
            v = ",".join(parts[1:]).strip()

        if keylen >= 0:
            assert len(k) == keylen
        if vallen >= 0:
            assert len(v) == vallen
        return (keytype(k), valtype(v))

    return fn


def load_dict(filename, parser):
    result = dict()
    with open(filename, "r") as f:
        for i, line in enumerate(f.readlines()):
            if i != 0:
                try:
                    k, v = parser(line)
                    result[k] = v
                except Exception as e:
                    print("Error while loading %s:%d:  [%s]" % (filename, i, e))
    return result


class WeatherEntry(typing.NamedTuple):
    stn: int
    obsdate: datetime.datetime
    temp: float
    windspeed: float
    tornado_or_funnel: bool
    country_code: str


class ParseFn(beam.DoFn):
    def __init__(self, stations):
        self.stations = stations

    def parse_weather_entry(self, line: str):
        for row in csv.reader([line]):
            assert len(row) == 16
            stn, yearmoda, temp, windspeed, frshtt = (
                row[0],
                row[2],
                row[3],
                row[8],
                row[15],
            )
            stn = int(stn)
            country_code = self.stations.get(stn, "?")
            assert len(frshtt) == 6
            # Missing value later detected simply by checking whether above 999.9
            return WeatherEntry(
                int(stn),
                datetime.datetime.strptime(yearmoda, "%Y%m%d"),
                float(temp),
                float(windspeed),
                frshtt[5] == "1",
                country_code,
            )

    def process(self, element: str, **kwargs):
        try:
            yield beam.pvalue.TaggedOutput(
                OUT_PARSED, self.parse_weather_entry(element)
            )
        except:
            yield beam.pvalue.TaggedOutput(OUT_INVALID, element)


class WeatherPipelineOptions(PipelineOptions):
    @classmethod
    def _add_argparse_args(cls, parser):
        parser.add_argument(
            "--input_weather_dir", help="Dir with *.csv files with weather data"
        )
        parser.add_argument("--input_stationlist", help="stationlist file")
        parser.add_argument("--input_countrylist", help="countrylist file")
        parser.add_argument("--outputdir", help="Dir to output data")


# Returns length of longest contiguous subsequence of values `val` in sequence `seq`
def max_consec_sequence_len(seq: typing.Sequence, val):
    maxlen = 0
    curlen = 0
    for el in seq:
        if el == val:
            curlen += 1
        else:
            maxlen = max(maxlen, curlen)
            curlen = 0
    maxlen = max(maxlen, curlen)
    return maxlen


def run():
    opts = PipelineOptions()
    with beam.Pipeline(options=opts) as p:

        opts = opts.view_as(WeatherPipelineOptions)

        stations = load_dict(opts.input_stationlist, parse_csv_kv_row(int, str, 6, 2))

        weather_entries, weather_badrows = (
            p
            | "Read weather entries"
            >> beam.io.ReadFromText(os.path.join(opts.input_weather_dir, "*.csv"))
            | "Parse weather"
            >> beam.ParDo(ParseFn(stations)).with_outputs("parsed", "invalid")
        )

        # windowed_weather_entries = (weather_entries | "Add timestamp" >> beam.ParDo(EntryAddTimestampFn())
        #                             | beam.WindowInto(window.FixedWindows(24*60*60)))

        _ = weather_badrows | "Log invalid weather rows" >> beam.io.WriteToText(
            os.path.join(opts.outputdir, "invalid_input_rows")
        )

        def any(seq: typing.Iterable[bool]):
            result = False
            for x in seq:
                result = result or x
            return result

        def process_weather_entries(el):
            key = el[0]
            weather = el[1]
            return beam.Row(
                country_code=key.country_code,
                obsdate=key.obsdate,
                temp=mean([x.temp for x in weather if x.temp < 9999.9]),
                windspeed=mean([x.windspeed for x in weather if x.windspeed < 999.9]),
                tornadoes=any([x.tornado_or_funnel for x in weather]),
            )

        weather_by_country_by_day = (
            weather_entries
            | beam.GroupBy("country_code", "obsdate")
            | beam.Map(process_weather_entries)
        )

        weather_by_country_by_year = weather_by_country_by_day | beam.GroupBy(
            "country_code", year=lambda x: x.obsdate.year
        )

        averages = weather_by_country_by_year | beam.MapTuple(
            lambda k, v: (
                k.year,
                beam.Row(
                    country=k.country_code,
                    temp=mean([y.temp for y in v]),
                    windspeed=mean([y.windspeed for y in v]),
                ),
            )
        )

        hottest = (
            averages
            | beam.Filter(lambda x: not numpy.isnan(x[1].temp))
            | "Top hottest" >> beam.combiners.Top.PerKey(1, key=lambda x: x.temp)
            | beam.Map(str)
            | "Write hottest"
            >> beam.io.WriteToText(os.path.join(opts.outputdir, "hottest"))
        )

        second_windiest = (
            averages
            | beam.Filter(lambda x: not numpy.isnan(x[1].windspeed))
            | "Top two windiest"
            >> beam.combiners.Top.PerKey(2, key=lambda x: x.windspeed)
            | beam.MapTuple(lambda k, v: (k, v[1]))
            | "Write second windiest"
            >> beam.io.WriteToText(os.path.join(opts.outputdir, "second_windiest"))
        )

        def calc_max_consecutive_tornado_days(el):
            key, entries = el
            entries = sorted(entries, key=lambda x: x.obsdate, reverse=True)
            return (
                key.year,
                beam.Row(
                    country_code=key.country_code,
                    max_consecutive_tornado_days=max_consec_sequence_len(
                        [x.tornadoes for x in entries], True
                    ),
                ),
            )

        max_consec_tornado_days = (
            weather_by_country_by_year
            | beam.Map(calc_max_consecutive_tornado_days)
            | "Top consec days tornado"
            >> beam.combiners.Top.PerKey(
                1, key=lambda x: x.max_consecutive_tornado_days
            )
            | "Write consecutively tornadiest"
            >> beam.io.WriteToText(
                os.path.join(opts.outputdir, "consecutively_tornadiest")
            )
        )


if __name__ == "__main__":
    run()
