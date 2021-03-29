import datetime

import pytest

from weather import *


def test_parse_csv_kv_row():
    assert parse_csv_kv_row(int, str)("421110,IN") == (421110, "IN")
    assert parse_csv_kv_row(int, str, 6, 2)("421110,IN") == (421110, "IN")
    assert parse_csv_kv_row(int, str, 6)("421110,IND") == (421110, "IND")
    assert parse_csv_kv_row(str, str)("421110,IN") == ("421110", "IN")
    assert parse_csv_kv_row(str, str, 6, 2)("421110,IN") == ("421110", "IN")
    assert parse_csv_kv_row(str, str)("  ,  ") == ("  ", "  ")

    assert parse_csv_kv_row(str, str, 2)("NK,KOREA, NORTH") == ("NK", "KOREA, NORTH")
    with pytest.raises(ValueError):
        assert parse_csv_kv_row(int, str)("  ,  ")
    with pytest.raises(AssertionError):
        assert parse_csv_kv_row(int, str, 6, 2)(" 121121,IN")
    with pytest.raises(AssertionError):
        assert parse_csv_kv_row(int, str, 6, 2)("1211212,IN")
    with pytest.raises(AssertionError):
        assert parse_csv_kv_row(int, str, 6, 2)("1211212,IND")
    with pytest.raises(ValueError):
        assert parse_csv_kv_row(int, str)("HI,THERE")


def test_parse_fn():
    pp = ParseFn(parse_csv_kv_row(int, str, 6, 2))

    for el in pp.process("121121,IN"):
        assert el.tag == "parsed"
        assert el.value == (121121, "IN")
    for el in pp.process("S12112,IN"):
        assert el.tag == "invalid"
        assert el.value == "S12112,IN"
    for el in pp.process("121121,IND"):
        assert el.tag == "invalid"
        assert el.value == "121121,IND"


def test_parse_weather_entry():
    assert parse_weather_entry(
        "958360,99999,20190129,79.1,60.2,9999.9,9999.9,999.9,5.5,11.1,999.9,93.6*,65.3,0.11G,999.9,010000"
    ) == WeatherEntry(958360, datetime.datetime(2019, 1, 29, 0, 0), 79.1, 5.5, False)

    assert parse_weather_entry(
        "958360,99999,20190129,9999.9,60.2,9999.9,9999.9,999.9,999.9,11.1,999.9,93.6*,65.3,0.11G,999.9,010001"
    ) == WeatherEntry(958360, datetime.datetime(2019, 1, 29, 0, 0), 9999.9, 999.9, True)


# Which country had the hottest average mean temperature over the year?
# Which country had the most consecutive days of tornadoes/funnel cloud formations?
# Which country had the second highest average mean wind speed over the year?
