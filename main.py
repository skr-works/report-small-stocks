import re
import sys
import csv
import datetime as dt
from io import BytesIO
from typing import List, Optional, Tuple, Dict

import requests
import pdfplumber
from bs4 import BeautifulSoup


FUND_PAGE_URL = "https://www.sbiokasan-am.co.jp/fund/552375/"
UA = "MonthlyFundReportBot/0.3 (personal test; github-actions)"
MASTER_CSV_PATH = "data/master.csv"


def http_get(url: str, timeout: int = 30) -> requests.Response:
    r = requests.get(url, headers={"User-Agent": UA}, timeout=timeout)
    r.raise_for_status()
    return r


def parse_jp_date(text: str) -> Optional[dt.date]:
    text = text.strip()
    m = re.search(r"(\d{4})\s*年\s*(\d{1,2})\s*月\s*(\d{1,2})\s*日", text)
    if m:
        y, mo, d = map(int, m.groups())
        return dt.date(y, mo, d)

    m = re.search(r"(\d{4})\s*年\s*(\d{1,2})\s*月\s*末", text)
    if m:
        y, mo = map(int, m.groups())
        if mo == 12:
            return dt.date(y, 12, 31)
        return dt.date(y, mo + 1, 1) - dt.timedelta(days=1)
    return None


def find_latest_monthly_pdf(page_html: str, base_url: str) -> Tuple[str, Optional[dt.date]]:
    soup = BeautifulSoup(page_html, "lxml")

    candidates = []
    for a in soup.select("a[href]"):
        href = (a.get("href") or "").strip()
        if not href:
            continue
        if "/data/fund_pdf/monthly/" not in href:
            continue
        if not href.lower().endswith(".pdf"):
            continue
        text = a.get_text(" ", strip=True)
        full = requests.compat.urljoin(base_url, href)
        candidates.append((full, text))

    if not candidates:
        raise RuntimeError("No monthly PDF candidates found (filter: /data/fund_pdf/monthly/).")

    scored = []
    for url, text in candidates:
        d = parse_jp_date(text)
        scored.append((d, url, text))

    scored.sort(key=lambda x: (x[0] is not None, x[0] or dt.date(1900, 1, 1)), reverse=True)
    latest_date, latest_url, latest_text = scored[0]

    print("=== Monthly PDF candidates (first 10) ===")
    for i, (d, url, text) in enumerate(scored[:10], 1):
        print(f"{i:02d}. date={d} url={url} text='{text}'")

    print("=== Selected latest ===")
    print(f"latest_date={latest_date} latest_url={latest_url} latest_link_text='{latest_text}'")

    return latest_url, latest_date


def extract_top10_holdings_names(pdf_bytes: bytes) -> List[str]:
    text_all = []
    with pdfplumber.open(BytesIO(pdf_bytes)) as pdf:
        for page in pdf.pages:
            t = page.extract_text() or ""
            if t:
                text_all.append(t)

    full_text = "\n".join(text_all)
    lines = [ln.strip() for ln in full_text.splitlines() if ln.strip()]

    holdings: List[str] = []
    in_block = False

    pat = re.compile(r"(\d{1,2})\s+([^\d%]+?)\s+(\d+(?:\.\d+)?)%")

    for ln in lines:
        if "組入上位10銘柄" in ln:
            in_block = True
            continue
        if not in_block:
            continue

        if ("当レポートは" in ln) or ("(1/8)" in ln) or ("ご注意" in ln):
            break

        matches = list(pat.finditer(ln))
        if not matches:
            continue

        m = matches[-1]
        rank = int(m.group(1))
        name = m.group(2).strip()

        if 1 <= rank <= 10 and name not in holdings:
            holdings.append(name)
        if len(holdings) >= 10:
            break

    return holdings[:10]


def normalize_name(s: str) -> str:
    """
    簡易正規化：社名表記ゆれを潰す（完全一致精度を上げる）
    """
    s = s.strip()
    s = s.replace("　", " ").replace("\u3000", " ")
    s = re.sub(r"\s+", "", s)

    # 会社種別の揺れ
    s = s.replace("株式会社", "").replace("(株)", "").replace("（株）", "")
    s = s.replace("有限会社", "").replace("合同会社", "")

    # 括弧の揺れ
    s = s.replace("（", "(").replace("）", ")")

    return s


def load_master_csv(path: str) -> List[Tuple[str, str]]:
    """
    CSV: code,name
    """
    rows: List[Tuple[str, str]] = []
    with open(path, "r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        if "code" not in reader.fieldnames or "name" not in reader.fieldnames:
            raise RuntimeError(f"master.csv must have headers: code,name (got {reader.fieldnames})")
        for r in reader:
            code = (r.get("code") or "").strip()
            name = (r.get("name") or "").strip()
            code4 = re.sub(r"\D", "", code)[:4]
            if len(code4) != 4 or not name:
                continue
            rows.append((code4, name))
    return rows


def build_indexes(master_rows: List[Tuple[str, str]]) -> Tuple[Dict[str, str], List[Tuple[str, str, str]]]:
    """
    1) 完全一致用 dict: normalized_name -> code
    2) 部分一致用 list: (code, raw_name, normalized_name)
    """
    exact: Dict[str, str] = {}
    partial: List[Tuple[str, str, str]] = []
    for code, name in master_rows:
        n = normalize_name(name)
        if n:
            exact[n] = code
            partial.append((code, name, n))
    return exact, partial


def resolve_code(name: str, exact: Dict[str, str], partial: List[Tuple[str, str, str]]) -> Tuple[Optional[str], str]:
    """
    戻り: (code, status)
      status:
        EXACT / PARTIAL / NOT_FOUND / AMBIGUOUS
    """
    key = normalize_name(name)

    # 1) 完全一致
    if key in exact:
        return exact[key], "EXACT"

    # 2) 部分一致（安全策：候補が1件のみなら採用、複数はAMBIGUOUS）
    hits = []
    for code, raw, norm in partial:
        # 双方向に見る（どっちが長いか不明なため）
        if key and (key in norm or norm in key):
            hits.append((code, raw))

    # ノイズ対策：短すぎるキー（2文字以下）は部分一致しない
    if len(key) <= 2:
        return None, "NOT_FOUND"

    if len(hits) == 1:
        return hits[0][0], "PARTIAL"
    if len(hits) >= 2:
        # 候補をログに出せるよう、ここではAMBIGUOUS
        print(f"AMBIGUOUS: '{name}' -> candidates={hits[:10]}")
        return None, "AMBIGUOUS"

    return None, "NOT_FOUND"


def main() -> int:
    now_jst = dt.datetime.utcnow() + dt.timedelta(hours=9)
    print(f"RUN_DATE_JST={now_jst:%Y-%m-%d %H:%M:%S} JST")

    fund_resp = http_get(FUND_PAGE_URL, timeout=30)
    latest_pdf_url, latest_date = find_latest_monthly_pdf(fund_resp.text, FUND_PAGE_URL)

    pdf_resp = http_get(latest_pdf_url, timeout=60)
    pdf_bytes = pdf_resp.content
    print(f"PDF_BYTES={len(pdf_bytes)} url={latest_pdf_url} report_date={latest_date}")

    holdings = extract_top10_holdings_names(pdf_bytes)
    print("=== Extracted holdings (names only) ===")
    for i, n in enumerate(holdings, 1):
        print(f"{i:02d}. {n}")

    # master (repo内)
    master_rows = load_master_csv(MASTER_CSV_PATH)
    exact, partial = build_indexes(master_rows)
    print(f"MASTER rows={len(master_rows)} exact_keys={len(exact)}")

    print("=== Resolved (name -> code) ===")
    for n in holdings:
        code, status = resolve_code(n, exact, partial)
        print(f"{n}\t{code if code else 'NONE'}\t{status}")

    return 0


if __name__ == "__main__":
    sys.exit(main())
