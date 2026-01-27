import re
import sys
import datetime as dt
from io import BytesIO
from typing import List, Optional, Tuple, Dict

import requests
import pandas as pd
import pdfplumber
from bs4 import BeautifulSoup


FUND_PAGE_URL = "https://www.sbiokasan-am.co.jp/fund/552375/"
JPX_LIST_XLS_URL = "https://www.jpx.co.jp/markets/statistics-equities/misc/tvdivq0000001vg2-att/data_j.xls"

UA = "MonthlyFundReportBot/0.1 (personal test; github-actions)"


def http_get(url: str, timeout: int = 30) -> requests.Response:
    r = requests.get(url, headers={"User-Agent": UA}, timeout=timeout)
    r.raise_for_status()
    return r


def parse_jp_date(text: str) -> Optional[dt.date]:
    """
    '2025年12月30日' / '2025年12月末' などから date を作る
    """
    text = text.strip()

    m = re.search(r"(\d{4})\s*年\s*(\d{1,2})\s*月\s*(\d{1,2})\s*日", text)
    if m:
        y, mo, d = map(int, m.groups())
        return dt.date(y, mo, d)

    m = re.search(r"(\d{4})\s*年\s*(\d{1,2})\s*月\s*末", text)
    if m:
        y, mo = map(int, m.groups())
        # 月末
        if mo == 12:
            return dt.date(y, 12, 31)
        return dt.date(y, mo + 1, 1) - dt.timedelta(days=1)

    return None


def find_latest_monthly_pdf(page_html: str, base_url: str) -> Tuple[str, Optional[dt.date], List[Tuple[str, str]]]:
    """
    月次レポートPDFっぽいリンクを集め、リンクテキストの日付で最新を選ぶ。
    返り値:
      latest_pdf_url, latest_date, candidates[(pdf_url, link_text)]
    """
    soup = BeautifulSoup(page_html, "lxml")

    candidates: List[Tuple[str, str]] = []
    for a in soup.select("a[href]"):
        href = a.get("href", "").strip()
        if not href:
            continue

        # 月次PDFだけに寄せる（ページには目論見書等のPDFもある）
        if "/data/fund_pdf/monthly/" not in href:
            continue
        if not href.lower().endswith(".pdf"):
            continue

        text = a.get_text(" ", strip=True)
        full = requests.compat.urljoin(base_url, href)
        candidates.append((full, text))

    if not candidates:
        raise RuntimeError("No monthly PDF candidates found (filter: /data/fund_pdf/monthly/).")

    # 日付が読めるもの優先で最新を選ぶ
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

    return latest_url, latest_date, candidates


def extract_top10_holdings_names(pdf_bytes: bytes) -> List[str]:
    """
    PDFテキストから「組入上位10銘柄」配下の銘柄名を抜く。
    例:
      1 キッツ 5.6%
      2 西華産業 4.7%
    """
    text_all = []
    with pdfplumber.open(BytesIO(pdf_bytes)) as pdf:
        for page in pdf.pages:
            t = page.extract_text() or ""
            if t:
                text_all.append(t)

    full_text = "\n".join(text_all)
    lines = [ln.strip() for ln in full_text.splitlines() if ln.strip()]

    # デバッグ：最初の数十行
    print("=== PDF text sample (first 60 lines) ===")
    for ln in lines[:60]:
        print(ln)

    # 「組入上位10銘柄」検出後、行パターンで拾う
    holdings: List[str] = []
    in_block = False

    for ln in lines:
        if "組入上位10銘柄" in ln:
            in_block = True
            continue
        if in_block:
            # 別セクションで打ち切り
            if ("ポートフォリオ" in ln) or ("組入銘柄数" in ln) or ("国内株式" in ln):
                break

            m = re.match(r"^\s*(\d{1,2})\s+(.+?)\s+(\d+(?:\.\d+)?)%\s*$", ln)
            if m:
                name = m.group(2).strip()
                holdings.append(name)

    # 取れない場合は「上位10銘柄」が別レイアウトの可能性があるので、ヒントログ
    if not holdings:
        print("WARN: No holdings extracted from '組入上位10銘柄' block. Layout may differ.")
        # 念のため、全文中の「組入上位10銘柄」周辺行を出す
        idxs = [i for i, ln in enumerate(lines) if "組入上位10銘柄" in ln]
        for idx in idxs[:2]:
            print("=== Context around '組入上位10銘柄' ===")
            for j in range(max(0, idx - 10), min(len(lines), idx + 30)):
                print(lines[j])

    # 重複除去しつつ順序維持
    seen = set()
    uniq = []
    for n in holdings:
        if n not in seen:
            seen.add(n)
            uniq.append(n)

    return uniq[:10]


def normalize_name(s: str) -> str:
    s = s.strip()
    # 全角スペース/半角スペース除去、記号ちょい整形
    s = s.replace("　", " ").replace("\u3000", " ")
    s = re.sub(r"\s+", "", s)
    # よくある表記ゆれ（必要最低限）
    s = s.replace("（", "(").replace("）", ")")
    return s


def load_jpx_master() -> pd.DataFrame:
    r = http_get(JPX_LIST_XLS_URL, timeout=60)
    df = pd.read_excel(BytesIO(r.content), dtype=str)
    df.columns = [str(c).strip() for c in df.columns]
    return df


def build_name_to_code(df: pd.DataFrame) -> Tuple[Dict[str, str], str, str]:
    # カラム当て推測
    code_col = None
    name_col = None

    for c in df.columns:
        if code_col is None and "コード" in c:
            code_col = c
        if name_col is None and ("銘柄名" in c) and ("英" not in c):
            name_col = c

    if not code_col or not name_col:
        raise RuntimeError(f"JPX master columns not found. columns={list(df.columns)[:20]}")

    # 辞書化（同名は後勝ちになるので、簡易テスト用途と割り切り）
    m = {}
    for _, row in df.iterrows():
        code = str(row.get(code_col, "") or "").strip()
        name = str(row.get(name_col, "") or "").strip()
        if not code or not name:
            continue
        # 4桁に寄せる（念のため）
        code4 = re.sub(r"\D", "", code)[:4]
        if len(code4) != 4:
            continue
        m[normalize_name(name)] = code4

    return m, code_col, name_col


def resolve_codes(holdings: List[str], name_to_code: Dict[str, str]) -> List[Tuple[str, Optional[str]]]:
    out = []
    for n in holdings:
        key = normalize_name(n)
        code = name_to_code.get(key)
        out.append((n, code))
    return out


def main() -> int:
    print(f"RUN_DATE_JST={dt.datetime.utcnow() + dt.timedelta(hours=9):%Y-%m-%d %H:%M:%S} JST")

    # 1) fund page
    fund_resp = http_get(FUND_PAGE_URL, timeout=30)
    latest_pdf_url, latest_date, _cands = find_latest_monthly_pdf(fund_resp.text, FUND_PAGE_URL)

    # 2) download pdf
    pdf_resp = http_get(latest_pdf_url, timeout=60)
    pdf_bytes = pdf_resp.content
    print(f"PDF_BYTES={len(pdf_bytes)} url={latest_pdf_url} report_date={latest_date}")

    # 3) extract holdings names (top10)
    holdings = extract_top10_holdings_names(pdf_bytes)
    print("=== Extracted holdings (names) ===")
    if holdings:
        for i, n in enumerate(holdings, 1):
            print(f"{i:02d}. {n}")
    else:
        print("(none)")

    # 4) map to codes via JPX master
    print("=== Downloading JPX master (data_j.xls) ===")
    df_jpx = load_jpx_master()
    name_to_code, code_col, name_col = build_name_to_code(df_jpx)
    print(f"JPX_MASTER rows={len(df_jpx)} code_col='{code_col}' name_col='{name_col}' dict_size={len(name_to_code)}")

    resolved = resolve_codes(holdings, name_to_code)

    print("=== Resolved codes (name -> code) ===")
    ok = 0
    for name, code in resolved:
        print(f"{name}\t{code if code else 'NOT_FOUND'}")
        if code:
            ok += 1
    print(f"RESOLVE_SUMMARY total={len(resolved)} found={ok} not_found={len(resolved)-ok}")

    # 5) return success even if not found (this is a test runner)
    return 0


if __name__ == "__main__":
    sys.exit(main())
