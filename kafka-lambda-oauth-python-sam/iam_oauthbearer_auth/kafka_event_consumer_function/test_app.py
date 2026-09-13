import json
import pathlib

import app

EVENT = json.loads((pathlib.Path(__file__).resolve().parents[1] / "events" / "event.json").read_text())


def test_parse_records():
    messages = app.parse_records(EVENT)
    assert len(messages) == 2
    assert messages[0]["topic"] == "myTopic"
    assert messages[0]["partition"] == 0
    assert messages[0]["offset"] == 250
    assert messages[0]["timestamp"] == 1678072110111
    assert messages[0]["timestampType"] == "CREATE_TIME"
    assert messages[0]["decodedKey"] == "null"
    assert messages[0]["decodedValue"] == "f"
    assert messages[1]["offset"] == 251
    assert messages[1]["decodedValue"] == "g"


def test_handler_returns_200_without_table(monkeypatch):
    # With no DYNAMODB_TABLE_NAME set, the handler parses/logs but skips DynamoDB.
    monkeypatch.delenv("DYNAMODB_TABLE_NAME", raising=False)
    assert app.lambda_handler(EVENT, None) == "200 OK"
