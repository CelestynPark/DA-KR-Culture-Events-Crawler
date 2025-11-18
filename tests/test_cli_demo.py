import sys, sqlite3
from crawl_events import main

def test_cli_demo_creates_row_and_report(tmp_path, capsys, monkeypatch):
    db = tmp_path / "events.db"

    # collect demo
    monkeypatch.setenv("PYTHONIOENCODING", "utf-8")
    monkeypatch.setenv("LC_ALL", "C.UTF-8")
    sys.argv = ["crawl_events.py", "--db", str(db), "--sources", "demo", "--pages", "1"]
    # verify db row
    main()
    conn = sqlite3.connect(str(db))
    try:
        n = conn.execute("SELECT COUNT(*) FROM events").fetchone()[0]
        assert n == 1
    finally:
        conn.close()

    # report
    sys.argv = ["crawl_events.py", "--db", str(db), "--report"]
    captured = None
    main()
    captured = capsys.readouterr().out
    assert "== 총 건수 ==" in captured
    assert "== 월별 건수 ==" in captured
    assert "== 카테고리 상위 ==" in captured
    