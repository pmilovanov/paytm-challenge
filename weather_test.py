import pytest

from weather import *

def test_parse_csv_kv_row():
    assert parse_csv_kv_row(int, str)("421110,IN") == (421110, "IN")
    assert parse_csv_kv_row(int, str, 6, 2)("421110,IN") == (421110, "IN")
    assert parse_csv_kv_row(int, str, 6)("421110,IND") == (421110, "IND")
    assert parse_csv_kv_row(str, str)("421110,IN") == ("421110", "IN")
    assert parse_csv_kv_row(str, str, 6, 2)("421110,IN") == ("421110", "IN")
    assert parse_csv_kv_row(str, str)("  ,  ") == ("  ", "  ")
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