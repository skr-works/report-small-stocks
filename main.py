import re
import sys
import datetime as dt
from io import BytesIO
from typing import List, Optional, Tuple

import requests
import pdfplumber
from bs4 import BeautifulSoup


FUND_PAGE_URL = "https://www.sbiokasan-am.co.jp/fund/552375/"
UA = "MonthlyFundReportBot/0.2 (personal test; github-actions)"


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
    """
    想定レイアウト（今回のPDF）:
      1行に「業種 上位」＋「銘柄 上位」が同居し、行末に
        "... 1 キッツ 5.6%"
      のように銘柄側が来る。
    方針:
      「組入上位10銘柄」以降の行を走査し、各行で見つかる
      (順位, 名称, 比率%) のマッチのうち "最後のマッチ" を銘柄として採用。
    """
    text_all = []
    with pdfplumber.open(BytesIO(pdf_bytes)) as pdf:
        for page in pdf.pages:
            t = page.extract_text() or ""
            if t:
                text_all.append(t)

    full_text = "\n".join(text_all)
    lines = [ln.strip() for ln in full_text.splitlines() if ln.strip()]

    print("=== PDF text sample (first 60 lines) ===")
    for ln in lines[:60]:
        print(ln)

    holdings: List[str] = []
    in_block = False

    # 1〜10位の「順位 銘柄名 比率%」を拾う（複数マッチする行があるので最後を採用）
    pat = re.compile(r"(\d{1,2})\s+([^\d%]+?)\s+(\d+(?:\.\d+)?)%")

    for ln in lines:
        if "組入上位10銘柄" in ln:
            in_block = True
            continue
        if not in_block:
            continue

        # セクション終端の目安（必要なら増やす）
        if ("当レポートは" in ln) or ("(1/8)" in ln) or ("ご注意" in ln):
            break

        matches = list(pat.finditer(ln))
        if not matches:
            continue

        # その行に複数ある場合、最後が銘柄側であることが多いので最後を採用
        m = matches[-1]
        rank = int(m.group(1))
        name = m.group(2).strip()

        # 1〜10位だけ
        if 1 <= rank <= 10:
            # 順位が飛ぶ/重複する可能性もあるので、重複排除しつつ追加
            if name not in holdings:
                holdings.append(name)

        if len(holdings) >= 10:
            break

    if not holdings:
        print("WARN: No holdings extracted. Dumping context around '組入上位10銘柄' (if any).")
        idxs = [i for i, ln in enumerate(lines) if "組入上位10銘柄" in ln]
        for idx in idxs[:2]:
            print("=== Context around '組入上位10銘柄' ===")
            for j in range(max(0, idx - 10), min(len(lines), idx + 40)):
                print(lines[j])

    return holdings[:10]


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
    if holdings:
        for i, n in enumerate(holdings, 1):
            print(f"{i:02d}. {n}")
    else:
        print("(none)")

    return 0


if __name__ == "__main__":
    sys.exit(main())
