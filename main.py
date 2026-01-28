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

# 東証33業種（PDFの「組入上位10業種」に出るやつを弾くため）
TSE_33_SECTORS = {
    "水産・農林業", "鉱業", "建設業", "食料品", "繊維製品", "パルプ・紙", "化学",
    "医薬品", "石油・石炭製品", "ゴム製品", "ガラス・土石製品", "鉄鋼", "非鉄金属",
    "金属製品", "機械", "電気機器", "輸送用機器", "精密機器", "その他製品",
    "電気・ガス業", "陸運業", "海運業", "空運業", "倉庫・運輸関連業", "情報・通信業",
    "卸売業", "小売業", "銀行業", "証券、商品先物取引業", "保険業",
    "その他金融業", "不動産業", "サービス業"
}

# ファンド設定リスト
TARGET_FUNDS = [
    {
        "id": "SBI_SmallMonsters",  # 旧: 552375 (スモール・モンスターズ・ジャパン)
        "url": "https://www.sbiokasan-am.co.jp/fund/552375/",
        "finder_type": "sbi_scrape",
        "extract_trigger": "組入上位10銘柄",  # 部分一致
        "skip_keywords": ["当レポートは", "(1/8)", "ご注意", "※"],
    },
    {
        "id": "SBI_RoboPro",  # 新規: ROBO PRO
        "url": "https://www.sbiokasan-am.co.jp/fund/553175/",
        "finder_type": "sbi_scrape",
        "extract_trigger": "組入上位10銘柄",
        "skip_keywords": ["当レポートは", "(1/8)", "ご注意", "※"],
    },
    {
        "id": "Sparx_Gensen",  # 新規: スパークス・厳選投資
        "url": "https://www.sparx.co.jp/mutual/rsn.html", # エラー時の参照用
        "finder_type": "sparx_backtrack",
        # URLテンプレート: {ym} が YYYYMM (例: 202412) に置換される
        "pdf_url_template": "https://www.sparx.co.jp/mutual/rsn_{ym}.pdf",
        "extract_trigger": "【組⼊上位10銘柄】", # 厳密一致
        "skip_keywords": ["銘柄総数", "コード", "銘柄名", "業種", "比率"], # ヘッダや総数行をスキップ
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
    
    pat = re.compile(r"(\d{1,2})\s+([^\d%]+?)\s+
