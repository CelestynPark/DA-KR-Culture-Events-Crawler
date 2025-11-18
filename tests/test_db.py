import sqlite3
from crawl_events import ensure_db, upsert_events, Event, make_id

def _count(conn):
    return conn.execute("SELECT COUNT(*) FROM events").fetchone()[0]

def test_ensure_db_created_table(tmp_path):
    db = tmp_path / "events.db"
    ensure_db(str(db))
    conn = sqlite3.connect(str(db))
    try:
        # table exists
        name = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='events'"
        ).fetchone()[0]
        assert name == "events"
        # indices exist
        idx = dict(conn.execute(
            "SELECT name FROM sqlite_master WHERE type='index'"
        ).fetchall())
        assert "idx_events_start" in idx
        assert "idx_events_cat" in idx
        assert "idx_events_place" in idx
    finally:
        conn.close()

def test_update_insert_and_update(tmp_path):
    db = tmp_path / "events.db"
    ensure_db(str(db))

    eid = make_id("행사A", "2025-01-01", "서울", "demo")
    e1 = Event(
        id=eid, title="행사A", start_date="2025-01-01", end_date="2025-01-07",
        place="서울", price="무료", category="전시",
        url="https://demo.local/1", collected_at="2025-01-01T00:00:00Z", source="demo"
    )
    n1 = upsert_events(str(db), [e1])
    assert n1 >= 1

    conn = sqlite3.connect(str(db))
    try:
        assert _count(conn) == 1
    finally:
        conn.close()
    
    # update some fields, same id
    e2 = Event(
        id=eid, title="행사A", start_date="2025-01-01", end_date="2025-01-08",
        place="서울시립", price="5000원", category="전시",
        url="https://demo.local/1", collected_at="2025-01-02T00:00:00Z", source="demo"
    )
    n2 = upsert_events(str(db), [e2])
    assert n2 >= 1  # upsert update

    conn = sqlite3.connect(str(db))
    try:
        row = conn.execute(
            "SELECT end_date, place, price FROM events WHERE id=?", (eid,)
        ).fetchone()
        assert row == ("2025-01-08", "서울시립", "5000원")
    finally:
        conn.close()
    
