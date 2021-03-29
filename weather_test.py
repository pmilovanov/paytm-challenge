import datetime

import pytest

from weather import *


def test_parse_csv_kv_row():
    assert parse_csv_kv_row(int, str)("421110,IN") == (421110, "IN")
    assert parse_csv_kv_row(int, str, 6, 2)("421110,IN") == (421110, "IN")
    assert parse_csv_kv_row(int, str, 6)("421110   ,IND") == (421110, "IND")
    assert parse_csv_kv_row(str, str)("421110,IN") == ("421110", "IN")
    assert parse_csv_kv_row(str, str)("    421110,IN") == ("421110", "IN")
    assert parse_csv_kv_row(str, str, 6, 2)("421110,IN") == ("421110", "IN")
    assert parse_csv_kv_row(str, str)("  ,  ") == ("", "")

    assert parse_csv_kv_row(str, str, 2)("NK,KOREA, NORTH") == ("NK", "KOREA, NORTH")
    with pytest.raises(ValueError):
        assert parse_csv_kv_row(int, str)("  ,  ")
    with pytest.raises(AssertionError):
        assert parse_csv_kv_row(int, str, 6, 2)("Q 121121,IN")
    with pytest.raises(AssertionError):
        assert parse_csv_kv_row(int, str, 6, 2)("1211212,IN")
    with pytest.raises(AssertionError):
        assert parse_csv_kv_row(int, str, 6, 2)("1211212,IND")
    with pytest.raises(ValueError):
        assert parse_csv_kv_row(int, str)("HI,THERE")


def test_max_consec_sequence_len():
    assert max_consec_sequence_len([1, 1, 1, 0, 0, 0, 1, 0, 1, 1], 1) == 3
    assert max_consec_sequence_len([1, 1, 1, 0, 0, 0, 0, 1, 0, 1, 1], 0) == 4
    assert max_consec_sequence_len([1, 1, 1, 0, 0, 0, 1, 0, 1, 1], 2) == 0
    assert max_consec_sequence_len([1, 1, 1, 0, 0, 0, 2, 1, 0, 1, 1], 2) == 1
    assert max_consec_sequence_len([], 2) == 0


def test_parse_fn():
    stations = load_dict("stationlist.csv", parse_csv_kv_row(int, str, 6, 2))
    pp = ParseFn(stations)

    for el in pp.process(
        "958360,99999,20190129,79.1,60.2,9999.9,9999.9,999.9,5.5,11.1,999.9,93.6*,65.3,0.11G,999.9,010000"
    ):
        assert el.tag == "parsed"
        assert el.value == WeatherEntry(
            958360, datetime.datetime(2019, 1, 29, 0, 0), 79.1, 5.5, False, "AS"
        )
    for el in pp.process(
        "958360,99999,20190129,9999.9,60.2,9999.9,9999.9,999.9,999.9,11.1,999.9,93.6*,65.3,0.11G,999.9,010001"
    ):
        assert el.tag == "parsed"
        assert el.value == WeatherEntry(
            958360, datetime.datetime(2019, 1, 29, 0, 0), 9999.9, 999.9, True, "AS"
        )
    for el in pp.process(
        "A958360,99999,20190129,9999.9,60.2,9999.9,9999.9,999.9,999.9,11.1,999.9,93.6*,65.3,0.11G,999.9,010001"
    ):
        assert el.tag == "invalid"
        assert (
            el.value
            == "A958360,99999,20190129,9999.9,60.2,9999.9,9999.9,999.9,999.9,11.1,999.9,93.6*,65.3,0.11G,999.9,010001"
        )
