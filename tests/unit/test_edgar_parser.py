from pathlib import Path

from argus.sec_poller.edgar import Form4Data, parse_form4

FIXTURE = Path(__file__).parent.parent / "fixtures" / "sec" / "form4_sample.xml"


def test_parse_form4_extracts_fields():
    xml_bytes = FIXTURE.read_bytes()
    data = parse_form4(xml_bytes)
    assert isinstance(data, Form4Data)
    assert data.ticker == "NVDA"
    assert data.filer_name == "HUANG JENSEN"
    assert data.filer_role == "CEO"
    assert data.transaction_code == "P"
    assert data.shares == 50000
    assert data.price_per_share == 48.20
    assert data.value_usd == 2_410_000.0


def test_parse_form4_handles_sale():
    # Modify fixture in-memory: replace P -> S
    xml = FIXTURE.read_text()
    xml = xml.replace(
        "<transactionCode>P</transactionCode>", "<transactionCode>S</transactionCode>"
    )
    data = parse_form4(xml.encode())
    assert data.transaction_code == "S"


def test_parse_form4_director_flag_numeric():
    # EDGAR commonly emits <isDirector>1</isDirector> instead of "true"
    xml = FIXTURE.read_text()
    xml = xml.replace("<isOfficer>true</isOfficer>", "<isOfficer>0</isOfficer>")
    xml = xml.replace("<officerTitle>CEO</officerTitle>", "")
    xml = xml.replace("<isDirector>false</isDirector>", "<isDirector>1</isDirector>")
    data = parse_form4(xml.encode())
    assert data.filer_role == "Director"


def test_parse_form4_director_flag_true_string():
    xml = FIXTURE.read_text()
    xml = xml.replace("<officerTitle>CEO</officerTitle>", "")
    xml = xml.replace("<isDirector>false</isDirector>", "<isDirector>true</isDirector>")
    data = parse_form4(xml.encode())
    assert data.filer_role == "Director"


def test_parse_form4_raises_on_invalid_xml():
    import pytest

    with pytest.raises(ValueError):
        parse_form4(b"not xml")
