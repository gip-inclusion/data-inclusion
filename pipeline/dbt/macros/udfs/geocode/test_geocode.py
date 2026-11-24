import csv
import dataclasses
import io
import logging

import pytest
import requests

from geocode.geocode import (
    OUTPUT_COLUMNS,
    Address,
    clean,
    clean_adresse,
    clean_commune,
    geocode,
    is_geocodable,
    keep_result,
    normalize,
    parse_csv,
    to_csv,
)

# Responses captured from the BAN on 2026-11-18, with the request of `post_to_ban`.
# Line 4 is not found because its `code_insee` (Anjou) does not match the address
# (Lille), and the BAN post-filters on it.
BAN_HEADER = (
    "id|adresse|code_postal|commune|code_insee|longitude|latitude|result_score"
    "|result_score_next|result_label|result_type|result_id|result_banId"
    "|result_housenumber|result_name|result_street|result_postcode|result_city"
    "|result_context|result_citycode|result_oldcitycode|result_oldcity"
    "|result_district|result_status"
)
BAN_FOUND = (
    "1|17 rue Malus|59000|Lille|59350|3.067408|50.627088|0.9746272727272727"
    "||17 Rue Malus 59000 Lille|housenumber|59350_5835_00017"
    "|9795009a-bba2-4ee4-a9dd-e1d183b08ab7|17|17 Rue Malus|Rue Malus|59000|Lille"
    "|59, Nord, Hauts-de-France|59350|59350|Lille||ok"
)
BAN_MUNICIPALITY = (
    "2|bla||Anjou|38009|4.882439|45.34745|0.4811027272727273|0.39334454545454545"
    "|Anjou|municipality|38009|27f3a893-6b5b-4b37-839b-f7cfcb591467||Anjou||38150"
    "|Anjou|38, Isère, Auvergne-Rhône-Alpes|38009||||ok"
)
BAN_MISSING_ADDRESS = (
    "3|||Anjou|38009|4.882439|45.34745|0.9356481818181817||Anjou|municipality"
    "|38009|27f3a893-6b5b-4b37-839b-f7cfcb591467||Anjou||38150|Anjou"
    "|38, Isère, Auvergne-Rhône-Alpes|38009||||ok"
)
BAN_NOT_FOUND = "4|17 rue Malus|59000|Lille|38009|||||||||||||||||||not-found"
BAN_RESPONSE = (
    "\n".join(
        [BAN_HEADER, BAN_FOUND, BAN_MUNICIPALITY, BAN_MISSING_ADDRESS, BAN_NOT_FOUND]
    )
    + "\n"
)

# the inputs that gave BAN_RESPONSE
BAN_INPUTS = [
    {
        "id": "1",
        "adresse": "17 rue Malus",
        "code_postal": "59000",
        "code_insee": "59350",
        "commune": "Lille",
    },
    {
        "id": "2",
        "adresse": "bla",
        "code_postal": None,
        "code_insee": "38009",
        "commune": "Anjou",
    },
    {
        "id": "3",
        "adresse": None,
        "code_postal": None,
        "code_insee": "38009",
        "commune": "Anjou",
    },
    {
        "id": "4",
        "adresse": "17 rue Malus",
        "code_postal": "59000",
        "code_insee": "38009",
        "commune": "Lille",
    },
]


def make_address(**kwargs) -> Address:
    return Address(
        **{
            "id": "1",
            "adresse": None,
            "code_postal": None,
            "code_insee": None,
            "commune": None,
        }
        | kwargs
    )


def read_payload(payload: bytes) -> list[dict]:
    return list(csv.DictReader(io.StringIO(payload.decode()), delimiter="|"))


def echo_ban(payload: bytes) -> str:
    """Answer every address of the payload with a found house number."""
    lines = ["|".join(["id", *OUTPUT_COLUMNS[1:], "result_status"])]
    for row in read_payload(payload):
        lines.append(
            "|".join(
                [
                    row["id"],
                    "0.9",  # result_score
                    "label",  # result_label
                    "city",  # result_city
                    "housenumber",  # result_type
                    row["code_insee"],  # result_citycode
                    "59000",  # result_postcode
                    "name",  # result_name
                    "3.06",  # longitude
                    "50.62",  # latitude
                    "ok",  # result_status
                ]
            )
        )
    return "\n".join(lines) + "\n"


class FakeBan:
    """Records the payloads it receives, answers with `respond`."""

    def __init__(self, respond=echo_ban):
        self.respond = respond
        self.payloads = []

    def __call__(self, payload: bytes) -> str:
        self.payloads.append(payload)
        return self.respond(payload)


class FlakyClient:
    """Raises `exception` on the first `failures` calls, then echoes."""

    def __init__(self, failures: int, exception: Exception):
        self.failures = failures
        self.exception = exception
        self.calls = 0

    def __call__(self, payload: bytes) -> str:
        self.calls += 1
        if self.calls <= self.failures:
            raise self.exception
        return echo_ban(payload)


# rule 1


@pytest.mark.parametrize(
    ("row", "expected"),
    [
        pytest.param(
            {
                "id": "1",
                "adresse": "17 rue Malus",
                "code_postal": "59000",
                "code_insee": "59350",
                "commune": "Lille",
            },
            make_address(
                adresse="17 rue Malus",
                code_postal="59000",
                code_insee="59350",
                commune="Lille",
            ),
            id="unchanged",
        ),
        pytest.param(
            {"id": "1", "adresse": "", "code_postal": "", "commune": ""},
            make_address(),
            id="empty string",
        ),
        pytest.param(
            {"id": "1", "adresse": None, "code_postal": None, "commune": None},
            make_address(),
            id="none",
        ),
        pytest.param(
            {"id": "1", "adresse": "   ", "commune": " Lille  "},
            make_address(commune="Lille"),
            id="whitespace",
        ),
        pytest.param(
            {"id": 1, "code_insee": 38009},
            make_address(code_insee="38009"),
            id="integer",
        ),
    ],
)
def test_normalize(row, expected):
    assert normalize(row=row) == expected


# rule 2


@pytest.mark.parametrize(
    ("values", "expected"),
    [
        ({}, False),
        ({"code_postal": "59000"}, False),
        ({"code_insee": "59350"}, False),
        ({"commune": "Lille"}, False),
        ({"code_postal": "59000", "code_insee": "59350"}, True),
        ({"code_postal": "59000", "commune": "Lille"}, True),
        ({"code_insee": "59350", "commune": "Lille"}, True),
        ({"code_postal": "59000", "code_insee": "59350", "commune": "Lille"}, True),
        # the address does not count
        ({"adresse": "17 rue Malus", "commune": "Lille"}, False),
    ],
)
def test_is_geocodable(values, expected):
    assert is_geocodable(address=make_address(**values)) is expected


def test_is_geocodable_empty_string_is_missing():
    row = {"id": "1", "code_postal": "", "code_insee": "59350", "commune": "Lille"}
    assert is_geocodable(address=normalize(row=row))

    row = {"id": "1", "code_postal": "", "code_insee": "59350", "commune": ""}
    assert not is_geocodable(address=normalize(row=row))


# rule 3


@pytest.mark.parametrize(
    ("adresse", "expected"),
    [
        (None, None),
        ("17 rue Malus", "17 rue Malus"),
        (" - 17 rue Malus - ", "17 rue Malus"),
        # post box
        ("17 rue Malus BP 12345", "17 rue Malus"),
        ("3 route de Cobonne bp 7872387", "3 route de Cobonne"),
        ("3 route de Cobonne CS 23123", "3 route de Cobonne"),
        ("CS 23123 3 route de Cobonne", "3 route de Cobonne"),
        ("BP 12", None),
        # abbreviations
        ("3 rte de avenue de Cobonne", "3 route de avenue de Cobonne"),
        ("3 Rte de Cobonne", "3 route de Cobonne"),
        ("3 RTE de Cobonne", "3 route de Cobonne"),
        ("3 rd pt de la Liberté", "3 rond point de la Liberté"),
        ("3 rue d'allemagne", "3 rue d'allemagne"),
        # changed: word boundary around BP/CS
        ("12 rue des Francs 12", "12 rue des Francs 12"),
        # changed: dots removed
        ("17 rue Malus B.P. 123", "17 rue Malus"),
        ("Avenue de l'U.R.S.S.", "Avenue de l'URSS"),
        # hyphens inside the address are kept
        ("12 rue de la Ville-Nouvelle", "12 rue de la Ville-Nouvelle"),
    ],
)
def test_clean_adresse(adresse, expected):
    assert clean_adresse(adresse=adresse) == expected


# rule 4


@pytest.mark.parametrize(
    ("commune", "expected"),
    [
        (None, None),
        ("Lille", "Lille"),
        ("Lille cedex 12345", "Lille"),
        ("Aouste-sur-Sye CEDEX 88793", "Aouste-sur-Sye"),
        ("Aouste-sur-Sye cedex 88793", "Aouste-sur-Sye"),
        ("Lille - Cedex", "Lille"),
        ("Cedex 9", None),
    ],
)
def test_clean_commune(commune, expected):
    assert clean_commune(commune=commune) == expected


# rule 5


@pytest.mark.parametrize(
    ("address", "expected"),
    [
        pytest.param(
            make_address(
                adresse="17 rue Malus BP 123", code_postal="59012", commune="Lille"
            ),
            make_address(adresse="17 rue Malus", commune="Lille"),
            id="post box",
        ),
        pytest.param(
            make_address(
                adresse="17 rue Malus B.P. 123", code_postal="59012", commune="Lille"
            ),
            make_address(adresse="17 rue Malus", commune="Lille"),
            id="post box with dots",
        ),
        pytest.param(
            make_address(
                adresse="17 rue Malus", code_postal="59012", commune="Lille cedex"
            ),
            make_address(adresse="17 rue Malus", commune="Lille"),
            id="cedex",
        ),
        pytest.param(
            make_address(
                adresse="17 r Malus", code_postal="59000", commune="- Lille -"
            ),
            make_address(adresse="17 rue Malus", code_postal="59000", commune="Lille"),
            id="other cleanup",
        ),
        pytest.param(
            make_address(code_postal="59000", commune="Lille"),
            make_address(code_postal="59000", commune="Lille"),
            id="missing adresse",
        ),
        pytest.param(
            make_address(adresse="17 rue Malus", code_postal="59000"),
            make_address(adresse="17 rue Malus", code_postal="59000"),
            id="missing commune",
        ),
    ],
)
def test_clean(address, expected):
    assert clean(address=address) == expected


# rule 6


def test_to_csv():
    addresses = [
        make_address(
            adresse="17 rue Malus",
            code_postal="59000",
            code_insee="59350",
            commune="Lille",
        ),
        make_address(id="2", code_insee="38009", commune="Anjou"),
        make_address(id="3", adresse='rue "a|b"', commune="Lille"),
    ]

    assert to_csv(addresses=addresses) == (
        b'"id"|"adresse"|"code_postal"|"commune"|"code_insee"\n'
        b'"1"|"17 rue Malus"|"59000"|"Lille"|"59350"\n'
        b'"2"|""|""|"Anjou"|"38009"\n'
        b'"3"|"rue ""a|b"""|""|"Lille"|""\n'
    )


def test_to_csv_utf8():
    payload = to_csv(addresses=[make_address(adresse="allée", commune="Œuilly")])
    assert "allée".encode() in payload
    assert "Œuilly".encode() in payload


# rule 8


def test_parse_csv():
    found, municipality, missing_address, not_found = parse_csv(text=BAN_RESPONSE)

    assert found["id"] == "1"
    assert found["result_type"] == "housenumber"
    assert found["result_citycode"] == "59350"
    assert found["result_score"] == 0.9746272727272727
    assert found["longitude"] == 3.067408
    assert found["latitude"] == 50.627088
    assert found["result_score_next"] is None
    assert found["result_district"] is None

    assert municipality["result_type"] == "municipality"
    assert municipality["code_postal"] is None
    assert municipality["result_context"] == "38, Isère, Auvergne-Rhône-Alpes"

    assert missing_address["adresse"] is None
    assert missing_address["result_type"] == "municipality"

    assert not_found["id"] == "4"
    assert not_found["result_status"] == "not-found"
    assert not_found["result_type"] is None
    assert not_found["result_score"] is None
    assert not_found["longitude"] is None
    assert not_found["latitude"] is None


def test_parse_csv_skips_malformed_line(caplog):
    text = "\n".join([BAN_HEADER, BAN_FOUND, "2|bla|38009", BAN_NOT_FOUND]) + "\n"

    with caplog.at_level(logging.WARNING):
        results = parse_csv(text=text)

    assert [result["id"] for result in results] == ["1", "4"]
    assert "line 3" in caplog.text


@pytest.mark.parametrize(
    "text",
    [
        "",
        "adresse|result_type\nbla|street\n",
        "id|adresse\n1|bla\n",
    ],
)
def test_parse_csv_missing_column(text):
    with pytest.raises(ValueError, match="missing columns"):
        parse_csv(text=text)


def test_csv_round_trip():
    addresses = [
        make_address(
            adresse="17 rue Malus",
            code_postal="59000",
            code_insee="59350",
            commune="Lille",
        ),
        make_address(id="2", code_insee="38009", commune="Anjou"),
        make_address(id="3", adresse='rue "a|b"', commune="Œuilly"),
    ]

    # like the BAN, echo the input and append a result column
    header, *lines = to_csv(addresses=addresses).decode().splitlines()
    text = "\n".join([header + '|"result_type"'] + [line + '|""' for line in lines])

    fields = [field.name for field in dataclasses.fields(Address)]
    assert [
        Address(**{field: result[field] for field in fields})
        for result in parse_csv(text=text)
    ] == addresses


# rule 9


@pytest.mark.parametrize(
    ("address", "result_type", "expected"),
    [
        (make_address(adresse="bla", commune="Anjou"), "housenumber", True),
        (make_address(adresse="bla", commune="Anjou"), "street", True),
        (make_address(adresse="bla", commune="Anjou"), "municipality", False),
        (make_address(commune="Anjou"), "municipality", True),
        (make_address(adresse="Le Bourg", commune="Anjou"), "municipality", True),
        (make_address(adresse="Mairie", commune="Anjou"), "municipality", True),
        (make_address(adresse="au village", commune="Anjou"), "municipality", True),
        (make_address(adresse="anjou 07300", commune="Anjou"), "municipality", True),
        (make_address(adresse="Anjou", commune="anjou 07300"), "municipality", True),
        # changed: place words as whole words
        (
            make_address(adresse="12 rue du Faubourg", commune="Lille"),
            "municipality",
            False,
        ),
        (
            make_address(adresse="1 place de Strasbourg", commune="Paris"),
            "municipality",
            False,
        ),
        # changed: missing values are not compared as text
        (make_address(commune="Nantes"), "municipality", True),
        (make_address(adresse="12 rue de Nantes"), "municipality", False),
        (make_address(adresse="12 rue de la None"), "municipality", False),
        # not found
        (make_address(adresse="bla", commune="Anjou"), None, True),
    ],
)
def test_keep_result(address, result_type, expected):
    assert keep_result(address=address, result={"result_type": result_type}) is (
        expected
    )


# rule 7


@pytest.mark.parametrize(
    "exception",
    [requests.ConnectionError("down"), requests.HTTPError("503")],
)
def test_geocode_drops_failed_batch(caplog, exception):
    client = FlakyClient(failures=1, exception=exception)

    with caplog.at_level(logging.WARNING):
        results = geocode(data=make_rows(5), client=client, batch_size=2)

    # no retry: the first batch is dropped, the others are geocoded
    assert client.calls == 3
    assert [result["id"] for result in results] == ["2", "3", "4"]
    assert "BAN request failed, dropping batch" in caplog.text


def test_geocode_raises_other_exceptions():
    client = FlakyClient(failures=1, exception=ValueError("bug"))

    with pytest.raises(ValueError):
        geocode(data=make_rows(1), client=client)
    assert client.calls == 1


# wiring


def make_rows(count: int) -> list[dict]:
    return [
        {
            "id": str(index),
            "adresse": f"{index} rue Malus",
            "code_postal": "59000",
            "code_insee": "59350",
            "commune": "Lille",
        }
        for index in range(count)
    ]


def test_geocode_batches():
    client = FakeBan()

    results = geocode(data=make_rows(5), client=client, batch_size=2)

    assert [len(read_payload(payload)) for payload in client.payloads] == [2, 2, 1]
    assert [result["id"] for result in results] == ["0", "1", "2", "3", "4"]


def test_geocode_keeps_input_order():
    def reversed_echo_ban(payload: bytes) -> str:
        header, *lines = echo_ban(payload).splitlines()
        return "\n".join([header, *reversed(lines)]) + "\n"

    results = geocode(data=make_rows(3), client=FakeBan(respond=reversed_echo_ban))

    assert [result["id"] for result in results] == ["0", "1", "2"]


def test_geocode_nothing_geocodable():
    client = FakeBan()
    rows = [
        {"id": "1", "adresse": "17 rue Malus", "commune": "Lille"},
        {"id": "2", "adresse": None, "code_postal": "", "code_insee": "59350"},
    ]

    assert geocode(data=rows, client=client) == []
    assert geocode(data=[], client=client) == []
    assert client.payloads == []


def test_geocode_single_dict():
    results = geocode(data=make_rows(1)[0], client=FakeBan())

    assert [result["id"] for result in results] == ["0"]


def test_geocode_output():
    (result,) = geocode(data=BAN_INPUTS[0], client=FakeBan())

    assert list(result) == list(OUTPUT_COLUMNS)
    assert result == {
        "id": "1",
        "result_score": 0.9,
        "result_label": "label",
        "result_city": "city",
        "result_type": "housenumber",
        "result_citycode": "59350",
        "result_postcode": "59000",
        "result_name": "name",
        "longitude": 3.06,
        "latitude": 50.62,
    }


def test_geocode_filters_fixtures():
    results = geocode(data=BAN_INPUTS, client=FakeBan(respond=lambda _: BAN_RESPONSE))

    # 2 is a municipality-only result for a street address
    assert [result["id"] for result in results] == ["1", "3", "4"]
    found, missing_address, not_found = results
    assert found["result_type"] == "housenumber"
    assert isinstance(found["result_score"], float)
    assert missing_address["result_type"] == "municipality"
    assert not_found == {column: None for column in OUTPUT_COLUMNS} | {"id": "4"}


def test_geocode_skips_unknown_id(caplog):
    def unknown_id_ban(payload: bytes) -> str:
        return echo_ban(payload) + "999|0.9|||housenumber||||||ok\n"

    with caplog.at_level(logging.WARNING):
        results = geocode(data=make_rows(1), client=FakeBan(respond=unknown_id_ban))

    assert [result["id"] for result in results] == ["0"]
    assert "unknown id=999" in caplog.text


def test_geocode_keeps_postal_code_in_mixed_batch():
    client = FakeBan()
    rows = [
        {
            "id": "0",
            "adresse": "17 rue Malus",
            "code_postal": "59000",
            "code_insee": "59350",
            "commune": None,
        },
        {
            "id": "1",
            "adresse": None,
            "code_postal": "59000",
            "code_insee": "59350",
            "commune": "Lille",
        },
        {
            "id": "2",
            "adresse": "17 rue Malus BP 12",
            "code_postal": "59012",
            "code_insee": "59350",
            "commune": "Lille",
        },
    ]

    geocode(data=rows, client=client)

    (payload,) = client.payloads
    assert [row["code_postal"] for row in read_payload(payload)] == [
        "59000",
        "59000",
        "",
    ]


@pytest.mark.integration
def test_geocode_ban():
    (result,) = geocode(
        data={
            "id": "1",
            "adresse": "17 rue Malus",
            "code_postal": "59000",
            "code_insee": "59350",
            "commune": "Lille",
        }
    )

    assert result["result_type"] == "housenumber"
    assert result["result_citycode"] == "59350"
