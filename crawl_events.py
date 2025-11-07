import argparse, hashlib, json, re, sys
from datetime import datetime
from typing import Dict, List, Optional, Tuple
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup
from dateutil import parser as dateparser
from pydantic import BaseModel

# ==== MVP: 출력은 NDJSON(이벤트 당 한 줄 JSON). DB/리포트는 이후 단계에서 추가됨. ====

class Event(BaseModel):
    id: str
    title: str
    start_date: Optional[str] = None
    end_date: Optional[str] = None
    place: Optional[str] = None
    price: Optional[str] = None
    category: Optional[str] = None
    url: str
    collected_at: str
    source: str

def make_id(title: str, start: Optional[str], place: Optional[str], source: str) -> str:
    base = f"{title}|{start or ''}|{place or ''}|{source}"
    return hashlib.sha1(base.encode("utf-8")).hexdigest()

def text_of(soup: BeautifulSoup, selector: Optional[str]) -> Optional[str]:
    if not selector:
        return None
    el = soup.select_one(selector)
    if not el:
        return None
    return " ".join(el.get_text(" ", strip=True).split())

def parse_period(raw: Optional[str], patterns: List[str]) -> Tuple[Optional[str], Optional[str]]:
    if not raw:
        return None, None
    for p in patterns:
        m = re.search(p, raw)
        if m:
            g = m.groups()
            if len(g) == 2:
                try:
                    s = dateparser.parse(g[0]).date().isoformat()
                    e = dateparser.parse(g[1]).date().isoformat()
                    return s, e
                except Exception:
                    continue
    try:
        d = dateparser.parse(raw).date().isoformat()
        return d, d
    except Exception:
        return None, None

def parse_price(raw: Optional[str]) -> Optional[str]:
    if not raw:
        return None
    t = re.sub(r"\s+", "", raw)
    if "무료" in t:
        return "무료"
    m = re.search(r"(\d[\d, ]*)\s*원", t)
    if m:
        return m.group(1).replace(",", "") + "원" 
    return raw.strip()

def normalize_category(title: str, raw_cat: Optional[str]) -> Optional[str]:
    base = ((raw_cat or "") + " " + title).lower()
    if any(k in base for k in ["전시", "exhibition", "gallery"]):
        return "전시"
    if any(k in base for k in ["공연", "concert", "뮤지컬", "연극"]):
        return "공연"
    if any(k in base for k in ["교육", "강좌", "워크숍", "세미나"]):
        return "교육"
    if any(k in base for k in ["축제", "festival"]):
        return "축제"
    return raw_cat

# 어댑터 스켈레톤: 실제 사이트 구조에 맞춰 selector와 패턴만 채우면 동작한다.
ADAPTERS: Dict[str, Dict] = {
    "city_a": {
        "base": "https://example.go.kr/culture/events",
        "list_url": lambda page: f"https://example.go.kr/culture/events?page={page}",
        "list_items_selector": "ul.board li a",
        "detail_selectors": {
            "title": "h1.title",
            "period": "div.meta span.period",
            "place": "div.meta span.place",
            "price": "div.meta span.price",
            "category": "div.meta span.category",           
        },
        "date_patterns": [
            r"(\d{4}\.\d{1,2}\.d{1,2})\s*[~\-–]\s*(\d{4}\.\d{1,2}\.\d{1,2})",
            r"(\d{4}\.\d{1,2}\.d{1,2})\s*[~\-–]\s*(\d{4}\.\d{1,2}\.\d{1,2})",
        ],
    },
    "city_b": {
        "base": "https://example2.go.kr/art/calendar",
        "list_url": lambda page: f"https://example2.go.kr/art/calendar?pageIndex={page}",
        "list_item_selector": "table.list tbody tr td.title a",
        "detail_selectors": {
            "title": "div.view h2",
            "period": "div.view .period",
            "place": "div.view .place",
            "price": "div.view .price",
            "category": "div.view .category",
        },
        "date_patterns": [
            r"(\d{4}\.\d{1,2}\.\d{1,2})\s*~\s*(\d{1,2}\.\d{1,2})",
        ],
    },
    # 네트워크 없이 바로 동작 가능한 데모 모드
    "demo": {
        "list_url": None,  # 사용하지 않음
        "list_item_selector": None,
        "detail_selectors": {
            "title": "h1",
            "period": "p.period",
            "place": "p.place",
            "price": "p.price",
            "category": "p.category",
        },
        "date_patterns": [
            r"(\d{4}-\d{1,2}-\d{1,2}\s*~\s*(\d{4}-\d{1,2}-d{1,2}))",
        ],
        "demo_list_html": """
            <html><body>
            <ul class='board'>
                <li><a href='/detail/1'>상세보기</li>
            </ul>
            </body></html>
        """,
        "demo_detail_html": """
            <html><body>
                <h1>시립미술관 특별전</h1>
                <p class='period'>2025-01-12 ~ 2025.02.20</p>
                <p class='place'>서울 시립미술관</p>
                <p class='price'>성인 5,000원 / 청소년 무료</p>
                <p class='category'>전시</p>
            </body></html>
            """
    },
}

def fetch_html(url: str, timeout: int = 15) -> BeautifulSoup:
    r = requests.get(url, timeout=timeout, headers={
        "User-Agent": "kr-culture-events-crawler/0.2 (MVP)",
        "Accept-Language": "ko-KR;q=0.8",
    })
    if r.encoding is None:
        r.encoding = r.apparent_encoding or "utf-8"
    return BeautifulSoup(r.text, "lxml")

def crawl_source(name: str, conf: Dict, pages: int, since: Optional[str]) -> List[Event]:
    out: List[Event] = []
    if name == "demo":
        # 데모: 임의의 1건 생성
        lsoup = BeautifulSoup(conf["demo_list_html"], "lxml")
        dsoup = BeautifulSoup(conf["demo_detail_html"], "lxml")
        sel = conf["detail_selectors"]
        title = text_of(dsoup, sel.get("title")) or ""
        period_raw = text_of(dsoup, sel.get("period"))
        place = text_of(dsoup, sel.get("place"))
        price_raw = text_of(dsoup, sel.get("price"))
        category_raw = text_of(dsoup, sel.get("category"))
        sdate, edate = parse_period(period_raw, conf.get("date_patterns", []))
        if since and sdate and sdate < since:
            return []
        price = parse_price(price_raw)
        category = normalize_category(title, category_raw)
        ev = Event(
            id=make_id(title, sdate, place, name),
            title=title,
            start_date=sdate,
            end_date=edate,
            place=place,
            price=price,
            category=category,
            url="https://demo.local/detail/1",
            collected_at=datetime.utcnow().isoformat(timespec="seconds") + "Z",
            source=name,
        )
        out.append(ev)
        return out
    
    base = conf.get("base", "")
    for p in range(1, pages + 1):
        list_url = conf["list_url"](p)
        try:
            lsoup = fetch_html(list_url)
        except Exception as e:
            print(f"[WARN] list fetch failed: {list_url}, ({e})", file=sys.stderr)
            continue
        links = [urljoin(list_url, a.get("href")) for a in lsoup.selct(conf["list_item_selector"]) if a.get("href")]
        for u in links:
            try:
                dsoup = fetch_html(u)
            except Exception as e:
                print(f"[WARN] detail fetch failed: {u} ({e})", file=sys.stderr)
                continue
            sel = conf["detail_selectors"]
            title = text_of(dsoup, sel.get("title")) or ""
            period_raw = text_of(dsoup, sel("period"))
            place = text_of(dsoup, sel.get("place"))
            price_raw = text_of(dsoup, sel.get("price"))
            category_raw = text_of(dsoup, sel.get("category"))
            sdate, edate = parse_period(period_raw, conf.get("date_patterns", []))
            if since and sdate and sdate < since:
                continue
            price = parse_price(price_raw)
            category = normalize_category(title, category_raw)
            ev = Event(
                id=make_id(title, sdate, place, name),
                title=title,
                start_date=sdate,
                end_date=edate,
                place=place,
                price=price,
                category=category,
                url=u,
                collected_at=datetime.utcnow().isoformat(timespec="seconds") + "Z",
                source=name,
            )
            out.append(ev)
    return out

def main():
    ap = argparse.ArgumentParser(description="KR culture events MVP crawler (NDJSON output)")
    ap.add_argument("--sources", default="demo", help="콤마 분리 소스 이름(예: demo,city_a)")
    ap.add_argument("--pages", type=int, default=1, help="목록 페이지 수  (기본 1)")
    ap.add_argument("--since", default=None, help="수집 시작일 (YYYY-MM-DD)")
    args = ap.parse_args()

    total = 0
    for name in [s.strip() for s in args.sources.split(",") if s.strip()]:
        if name not in ADAPTERS:
            print(f"[WARN] unknown source: {name}", file=sys.stderr)
            continue
        items = crawl_source(name, ADAPTERS[name], args.pages, args.since)
        for ev in items:
            print(ev.model_dump_json(ensure_ascii=False))
            total += len(items)
        print(f"# collected={total}", file=sys.stderr)

if __name__ == "__main__":
    main()