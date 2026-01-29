import re
import sys
import csv
import datetime as dt
from io import BytesIO
from typing import List, Optional, Tuple, Dict, Any
from dateutil.relativedelta import relativedelta  # 日付計算用

import requests
import pdfplumber
from bs4 import BeautifulSoup

# --- Configuration ---
UA = "MonthlyFundReportBot/0.5 (github-actions test)"
MASTER_CSV_PATH = "data/master.csv"

# ファンド設定リスト
TARGET_FUNDS = [
    {
        "id": "SBI_SmallMonsters",  # 旧: 552375 (スモール・モンスターズ・ジャパン)
        "url": "https://www.sbiokasan-am.co.jp/fund/552375/",
        "finder_type": "sbi_scrape",
        "extract_trigger": "組入上位10銘柄",  # 部分一致
        "skip_keywords": ["当レポートは", "(1/8)", "ご注意", "※"],
        "extractor_type": "text_regex",
    },
    {
        "id": "Sparx_Gensen",  # 新規: スパークス・厳選投資
        "url": "https://www.sparx.co.jp/mutual/rsn.html", # エラー時の参照用
        "finder_type": "sparx_backtrack",
        # URLテンプレート: {ym} が YYYYMM (例: 202412) に置換される
        "pdf_url_template": "https://www.sparx.co.jp/mutual/rsn_{ym}.pdf",
        "extract_trigger": "組入上位10銘柄",  # 表の近傍判定に使う（完全一致は不要）
        "skip_keywords": ["銘柄総数", "コード", "銘柄名", "業種", "比率"], # ヘッダや総数行をスキップ
        "extractor_type": "sparx_table",
    },
    {
        # 追加: ひふみマイクロスコープpro
        "id": "Hifumi_MicroscopePro",
        "url": "https://hifumi.rheos.jp/fund/microscope/",
        "finder_type": "sparx_backtrack",  # 既存の後方探索ロジックを流用（-1 / -2 を含む）
        "pdf_url_template": "https://hifumi.rheos.jp/fund/microscope/pdf/report{ym}.pdf",
        "extract_trigger": "銘柄紹介（基準日時点の組入比率1~10位）",
        "skip_keywords": [],
        "extractor_type": "hifumi_rank_code",  # PDFから銘柄名＋コードを直接抜く
    },
]

# --- HTTP Helpers ---

def http_get(url: str, timeout: int = 30) -> requests.Response:
    try:
        r = requests.get(url, headers={"User-Agent": UA}, timeout=timeout)
        r.raise_for_status()
        return r
    except Exception as e:
        print(f"[Error] HTTP GET failed: {url} ({e})")
        raise

def http_head(url: str, timeout: int = 10) -> bool:
    """ファイルの存在確認 (200 OKならTrue)"""
    try:
        r = requests.head(url, headers={"User-Agent": UA}, timeout=timeout)
        return r.status_code == 200
    except:
        return False

# --- Finder Strategies ---

def parse_jp_date(text: str) -> Optional[dt.date]:
    text = text.strip()
    # 2024年1月31日
    m = re.search(r"(\d{4})\s*年\s*(\d{1,2})\s*月\s*(\d{1,2})\s*日", text)
    if m:
        y, mo, d = map(int, m.groups())
        return dt.date(y, mo, d)
    # 2024年1月末
    m = re.search(r"(\d{4})\s*年\s*(\d{1,2})\s*月\s*末", text)
    if m:
        y, mo = map(int, m.groups())
        # 末日は厳密でなくてもソートできれば良いが、一応計算
        return dt.date(y, mo, 1) + relativedelta(months=1, days=-1)
    return None

def find_pdf_sbi(base_url: str) -> Tuple[str, Optional[dt.date]]:
    """SBI岡三サイト用: HTMLからリンクを探索して最新日付を取得"""
    resp = http_get(base_url)
    soup = BeautifulSoup(resp.text, "lxml")

    candidates = []
    for a in soup.select("a[href]"):
        href = (a.get("href") or "").strip()
        if "/data/fund_pdf/monthly/" not in href:
            continue
        if not href.lower().endswith(".pdf"):
            continue

        text = a.get_text(" ", strip=True)
        full_url = requests.compat.urljoin(base_url, href)
        d = parse_jp_date(text)
        candidates.append((d, full_url, text))

    if not candidates:
        raise RuntimeError("No monthly PDF candidates found.")

    # 日付順にソート（日付がNoneのものは後ろへ）
    candidates.sort(key=lambda x: (x[0] is not None, x[0] or dt.date(1900, 1, 1)), reverse=True)

    best_date, best_url, _ = candidates[0]
    return best_url, best_date

def find_pdf_sparx_backtrack(url_template: str) -> Tuple[str, Optional[dt.date]]:
    """スパークス用: 先月から遡って存在確認 (推測アタック)"""
    today = dt.date.today()

    for i in range(1, 4):
        target_month = today - relativedelta(months=i)
        ym_str = target_month.strftime("%Y%m")
        url = url_template.format(ym=ym_str)

        print(f"  Checking: {url} ... ", end="")
        if http_head(url):
            print("FOUND")
            # 基準日は「その月の末日」として仮定
            report_date = target_month + relativedelta(day=31)
            return url, report_date
        print("404")

    raise RuntimeError("Latest PDF not found (checked last 3 months).")

# --- Extractor ---

def extract_top10_holdings(pdf_bytes: bytes, trigger: str, skip_keywords: List[str]) -> List[str]:
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

    # Regex: 順位(1~2桁) + 空白 + 銘柄名(数字%以外) + 空白 + 比率(数字.数字%)
    pat = re.compile(r"(\d{1,2})\s+([^\d%]+?)\s+(\d+(?:\.\d+)?)%")

    for ln in lines:
        # トリガーチェック
        if trigger in ln:
            in_block = True
            continue

        if not in_block:
            continue

        # 除外キーワード
        if any(sk in ln for sk in skip_keywords):
            continue

        matches = list(pat.finditer(ln))
        if not matches:
            continue

        # SBIのPDFでは「業種」と「銘柄」が同一行に並ぶため、複数マッチ時は右側(最後)だけ採用
        if len(matches) >= 2:
            matches = [matches[-1]]

        for m in matches:
            rank = int(m.group(1))
            name = m.group(2).strip()

            if 1 <= rank <= 10 and name not in holdings:
                holdings.append(name)

        if len(holdings) >= 10:
            break

    return holdings[:10]

def _fw_to_hw_digits(s: str) -> str:
    trans = str.maketrans({chr(0xFF10 + i): chr(0x30 + i) for i in range(10)})
    return s.translate(trans)

def extract_top10_holdings_sparx_table(pdf_bytes: bytes, trigger: str) -> List[str]:
    """
    スパークスのPDFは表が主体で、extract_textだと行順が崩れる。
    罫線ベースで table を抜いて 1〜10位の銘柄名だけ取る。
    """
    holdings: List[str] = []

    table_settings = {
        "vertical_strategy": "lines",
        "horizontal_strategy": "lines",
        "intersection_tolerance": 5,
        "snap_tolerance": 3,
        "join_tolerance": 3,
        "edge_min_length": 3,
        "min_words_vertical": 1,
        "min_words_horizontal": 1,
    }

    with pdfplumber.open(BytesIO(pdf_bytes)) as pdf:
        for page in pdf.pages:
            # trigger文字がPDF内で字形崩れすることがあるため、ここではトリガー必須にしない
            tbl = None
            try:
                tbl = page.extract_table(table_settings)
            except Exception:
                tbl = None

            if not tbl:
                # extract_tableが1発で取れない場合に備えて複数テーブルを試す
                try:
                    tables = page.extract_tables(table_settings) or []
                except Exception:
                    tables = []
                for t in tables:
                    # 1〜10がありそうなテーブルを選ぶ
                    flat = " ".join([" ".join([c or "" for c in row]) for row in (t or [])])
                    flat_hw = _fw_to_hw_digits(flat)
                    if re.search(r"\b1\b", flat_hw) and re.search(r"\b10\b", flat_hw):
                        tbl = t
                        break

            if not tbl:
                continue

            # 行を走査：先頭列が順位、次が銘柄名（スクショの形式）
            for row in tbl:
                if not row or len(row) < 2:
                    continue
                r0 = (row[0] or "").strip()
                r1 = (row[1] or "").strip()

                r0 = _fw_to_hw_digits(r0)
                m = re.match(r"^\s*(\d{1,2})\s*$", r0)
                if not m:
                    continue

                rank = int(m.group(1))
                if 1 <= rank <= 10 and r1:
                    if r1 not in holdings:
                        holdings.append(r1)

            if len(holdings) >= 10:
                break

    return holdings[:10]

def extract_top10_holdings_hifumi_rank_code(pdf_bytes: bytes, trigger: str) -> List[Tuple[str, str]]:
    """
    ひふみマイクロスコープpro:
    「銘柄紹介（基準日時点の組入比率1~10位）」ページから
    順位1〜10の「銘柄名」と「銘柄コード」を直接抽出する。
    """
    items: List[Tuple[str, str]] = []

    with pdfplumber.open(BytesIO(pdf_bytes)) as pdf:
        for page in pdf.pages:
            t = page.extract_text() or ""
            if not t or trigger not in t:
                continue

            lines = [ln.strip() for ln in t.splitlines() if ln.strip()]
            in_block = False
            pending_rank: Optional[int] = None

            # コードは 4桁 or 212A のような英字混在(3桁+英大文字)を想定
            code_pat = re.compile(r"^(.+?)\s+([0-9]{4}|[0-9]{3}[A-Z])\b")

            for ln in lines:
                if trigger in ln:
                    in_block = True
                    continue
                if not in_block:
                    continue

                ln_hw = _fw_to_hw_digits(ln)

                # rank 行（1〜10）が単独で出る
                if re.fullmatch(r"\d{1,2}", ln_hw):
                    r = int(ln_hw)
                    if 1 <= r <= 10:
                        pending_rank = r
                    else:
                        pending_rank = None
                    continue

                if pending_rank is None:
                    continue

                m = code_pat.match(ln_hw)
                if not m:
                    continue

                name = m.group(1).strip()
                code = m.group(2).strip()

                # 期待レンジのみ採用（重複も抑制）
                if 1 <= pending_rank <= 10 and name and code:
                    if name not in [x[0] for x in items]:
                        items.append((name, code))

                pending_rank = None  # 次の順位待ち

                if len(items) >= 10:
                    break

            break  # trigger があるページだけ見れば十分

    return items[:10]

# --- Master Data & Resolver ---

def normalize_name(s: str) -> str:
    s = s.strip()
    s = s.replace("　", " ").replace("\u3000", " ")
    s = re.sub(r"\s+", "", s)
    s = s.replace("株式会社", "").replace("(株)", "").replace("（株）", "")
    s = s.replace("（", "(").replace("）", ")")
    s = s.upper()

    # Full-width alpha to half-width
    trans = str.maketrans({
        chr(0xFF21 + i): chr(0x41 + i) for i in range(26)
    })
    s = s.translate(trans)
    trans_num = str.maketrans({
        chr(0xFF10 + i): chr(0x30 + i) for i in range(10)
    })
    s = s.translate(trans_num)

    s = s.replace("ホールディングス", "").replace("HOLDINGS", "").replace("HLDGS", "")
    s = re.sub(r"\bHD\b", "", s).replace("HD", "")
    return s

def load_master_csv(path: str) -> Tuple[Dict[str, str], List[Tuple[str, str, str]]]:
    """
    code, name, sector の3列があっても、code, name のみを使用する
    """
    exact = {}
    partial = []

    try:
        with open(path, "r", encoding="utf-8-sig", newline="") as f:
            reader = csv.DictReader(f)
            # 必須カラムチェック
            if "code" not in reader.fieldnames or "name" not in reader.fieldnames:
                print(f"[Warning] CSV headers missing code or name: {reader.fieldnames}")
                return {}, []

            for r in reader:
                code = (r.get("code") or "").strip()
                name = (r.get("name") or "").strip()
                # sector は無視

                code4 = re.sub(r"\D", "", code)[:4]
                if len(code4) != 4 or not name:
                    continue

                n = normalize_name(name)
                if n:
                    exact[n] = code4
                    partial.append((code4, name, n))

    except FileNotFoundError:
        print(f"[Error] {path} not found.")
        sys.exit(1)

    return exact, partial

def resolve_code(name: str, exact: Dict[str, str], partial: List[Tuple[str, str, str]]) -> Tuple[Optional[str], str]:
    key = normalize_name(name)
    if key in exact:
        return exact[key], "EXACT"

    # 部分一致
    hits = []
    for code, raw, norm in partial:
        if key and (key in norm or norm in key):
            hits.append(code)

    if len(key) <= 2:
        return None, "TOO_SHORT"

    if len(hits) == 1:
        return hits[0], "PARTIAL"
    if len(hits) >= 2:
        return None, "AMBIGUOUS"

    return None, "NOT_FOUND"

# --- Main ---

def main():
    print(f"=== Job Start: {dt.datetime.now()} ===")

    # Master Load
    exact, partial = load_master_csv(MASTER_CSV_PATH)
    print(f"Master loaded: {len(exact)} exact keys.")

    results = []

    for fund in TARGET_FUNDS:
        fid = fund["id"]
        print(f"\n--- Processing: {fid} ---")

        try:
            # 1. Find PDF
            pdf_url = ""
            report_date = None

            if fund["finder_type"] == "sbi_scrape":
                pdf_url, report_date = find_pdf_sbi(fund["url"])
            elif fund["finder_type"] == "sparx_backtrack":
                pdf_url, report_date = find_pdf_sparx_backtrack(fund["pdf_url_template"])

            print(f"  Target PDF: {pdf_url}")
            print(f"  Report Date: {report_date}")

            # 2. Download
            pdf_resp = http_get(pdf_url)
            pdf_bytes = pdf_resp.content

            # 3. Extract
            raw_names: List[str] = []
            raw_items: List[Tuple[str, str]] = []

            if fund.get("extractor_type") == "sparx_table":
                raw_names = extract_top10_holdings_sparx_table(pdf_bytes, fund["extract_trigger"])
            elif fund.get("extractor_type") == "hifumi_rank_code":
                raw_items = extract_top10_holdings_hifumi_rank_code(pdf_bytes, fund["extract_trigger"])
                raw_names = [n for n, _ in raw_items]
            else:
                raw_names = extract_top10_holdings(
                    pdf_bytes,
                    fund["extract_trigger"],
                    fund["skip_keywords"]
                )

            if not raw_names:
                print("  [Warning] No holdings found.")
                continue

            # 4. Resolve Codes / or use PDF codes
            print("  [Holdings]")
            if fund.get("extractor_type") == "hifumi_rank_code":
                for name, code_raw in raw_items:
                    print(f"    - {name} -> {code_raw} (PDF)")
                    results.append({
                        "fund_id": fid,
                        "report_date": str(report_date),
                        "rank_name": name,
                        "code": code_raw,
                        "status": "PDF"
                    })
            else:
                for name in raw_names:
                    code, status = resolve_code(name, exact, partial)
                    print(f"    - {name} -> {code} ({status})")

                    results.append({
                        "fund_id": fid,
                        "report_date": str(report_date),
                        "rank_name": name,
                        "code": code,
                        "status": status
                    })

        except Exception as e:
            print(f"  [Error] Failed to process {fid}: {e}")
            import traceback
            traceback.print_exc()

    print("\n=== Job Finished ===")
    # ここで results をファイルに出力するなどの処理が可能
    # 今回は標準出力のみ

if __name__ == "__main__":
    main()
