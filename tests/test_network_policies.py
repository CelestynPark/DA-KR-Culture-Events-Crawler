import types
from crawl_events import parse_delay, throttle

def test_parse_delay_ranges():
    assert parse_delay("2.0-3.5") == (2.0, 3.5)
    assert parse_delay("2") == (2.0, 2.0)
    # invalid -> default
    assert parse_delay("-1-0") == (1.2, 2.5)

def test_throttle_calls_sleep(monkeypatch):
    calls = {"val": 0}
    def fake_sleep(x):
        calls["val"] = x
    monkeypatch.setattr("time.sleep", fake_sleep)
    # monkeypatch random.uniform to deterministic 1.5
    monkeypatch.setattr("random.uniform", lambda a,b: 1.5)
    throttle((1.0, 2.0))
    assert calls["val"] == 1.5