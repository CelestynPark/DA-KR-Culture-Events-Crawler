from bs4 import BeautifulSoup
from crawl_events import parse_price, normalize_category, parse_period, make_id, ADAPTERS, text_of

def test_parse_price():
    assert parse_price("성인 5,000원 / 청소년 무료") == "무료"
    assert parse_price("일반 12,000원") == "12000원"
    assert parse_price("무료") == "무료"

def test_normalize_category():
    assert normalize_category("특별전 안내", None) == "전시"
    assert normalize_category("뮤지컬 갈라", "") == "공연"
    assert normalize_category("주말 워크숍", "기타") == "교육"

def test_parse_period_demo():
    raw = "2025-01-12 ~  2025-02-10"
    patterns = ADAPTERS["demo"]["date_patterns"]
    s, e = parse_period(raw, patterns)
    assert (s, e) == ("2025-01-12", "2025-02-20")

def test_make_id_stability():
    a = make_id("A", "2025-01-01", "서울", "demo")
    b = make_id("A", "2025-01-01", "서울", "demo")
    assert a == b

def test_text_of():
    html = "<div class='title'> 공백 정리 테스트 </div>"
    soup = BeautifulSoup(html, "lxml")
    assert text_of(soup, "div.title") == "공백 정리 테스트"

    