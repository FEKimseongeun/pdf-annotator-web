# app/services/common.py
import os
import sys
import hashlib
from typing import Dict, List, Tuple, Optional

import pandas as pd
import fitz  # PyMuPDF
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor

# ===== 실행 환경 / 풀 선택 =====
IS_FROZEN = getattr(sys, "frozen", False)  # PyInstaller 동결 여부
MAX_WORKERS = os.cpu_count() or 4  # ✅ 모든 코어 사용 (-1 제거)
DEFAULT_POOL = "thread" if IS_FROZEN else "process"   # exe는 thread가 유리

def get_executor(kind: Optional[str] = None, max_workers: int = MAX_WORKERS):
    """
    동결(exe) 환경에선 thread 풀, 개발 환경에선 process 풀.
    환경변수 E2M_POOL=thread|process 로 강제 가능.
    """
    use = (kind or os.environ.get("E2M_POOL", DEFAULT_POOL)).lower()
    if use.startswith("t"):
        return ThreadPoolExecutor(max_workers=max_workers)
    return ProcessPoolExecutor(max_workers=max_workers)

# ✅ PDF 메모리 로드 함수 추가
def load_pdf_to_bytes(pdf_path: str) -> bytes:
    """PDF 파일을 메모리에 로드하여 반복 I/O 방지"""
    with open(pdf_path, 'rb') as f:
        return f.read()

# ===== 색상/검색 유틸 =====
def hex_to_rgb01(hex_str: str) -> Tuple[float, float, float]:
    s = hex_str.strip()
    if s.startswith("#"):
        s = s[1:]
    if len(s) != 6:
        raise ValueError(f"Invalid hex color: {hex_str}")
    r = int(s[0:2], 16) / 255.0
    g = int(s[2:4], 16) / 255.0
    b = int(s[4:6], 16) / 255.0
    return (r, g, b)

def search_flags(ignore_case: bool, whole_word: bool) -> int:
    flags = 0
    try:
        if ignore_case:
            flags |= fitz.TEXT_IGNORECASE
    except AttributeError:
        pass
    try:
        if whole_word:
            flags |= fitz.TEXT_MATCH_WHOLE_WORDS
    except AttributeError:
        pass
    return flags

def color_hex_from_sheet_name(sheet_name: str) -> str:
    """ 시트명 해시 기반 0x40~0xC0 범위 색상 """
    h = hashlib.sha256(sheet_name.encode("utf-8")).hexdigest()
    def pick(i):
        v = int(h[i:i+2], 16)
        v = 0x40 + int((v / 255.0) * (0xC0 - 0x40))
        return v
    r, g, b = pick(0), pick(2), pick(4)
    return "#{:02X}{:02X}{:02X}".format(r, g, b)

# ===== 페이지 텍스트(line) 추출 =====
def page_lines_with_words(page: fitz.Page):
    words = page.get_text("words")  # [x0,y0,x1,y1,word, block_no, line_no, word_no]
    lines_map = {}
    for w in words:
        x0, y0, x1, y1, token, block_no, line_no, word_no = w
        key = (block_no, line_no)
        if key not in lines_map:
            lines_map[key] = {"tokens": [], "rect": [x0, y0, x1, y1]}
        lines_map[key]["tokens"].append(token)
        r = lines_map[key]["rect"]
        r[0] = min(r[0], x0); r[1] = min(r[1], y0); r[2] = max(r[2], x1); r[3] = max(r[3], y1)

    out = []
    for (block_no, line_no), v in lines_map.items():
        text = " ".join(v["tokens"])
        out.append({"block": block_no, "line": line_no, "line_text": text, "rect": tuple(v["rect"])})
    return out

def rect_key(rect_tuple, ndigits: int = 2):
    x0, y0, x1, y1 = rect_tuple
    return (round(x0, ndigits), round(y0, ndigits), round(x1, ndigits), round(y1, ndigits))


# ===== RESTRICTED용 엑셀 로딩/중복 제거 =====
def gather_restricted_rows_from_df(
    df: pd.DataFrame,
    clean_terms: bool
) -> List[Tuple[Optional[str], Optional[str], Optional[str]]]:
    """
    단일 시트(df)에서 A(0), B(1), C(2) 열의 행 단위 조합 수집. 최소 2개 이상 값 필요.
    clean_terms=True면 strip만 수행.
    """
    rows: List[Tuple[Optional[str], Optional[str], Optional[str]]] = []
    for idx in range(len(df)):
        a = str(df.iloc[idx, 0]) if 0 in df.columns and pd.notna(df.iloc[idx, 0]) else ""
        b = str(df.iloc[idx, 1]) if 1 in df.columns and pd.notna(df.iloc[idx, 1]) else ""
        c = str(df.iloc[idx, 2]) if 2 in df.columns and pd.notna(df.iloc[idx, 2]) else ""
        if clean_terms:
            a, b, c = a.strip(), b.strip(), c.strip()
        a = a if a != "" else None
        b = b if b != "" else None
        c = c if c != "" else None
        non_empty = sum(x is not None for x in (a, b, c))
        if non_empty >= 2:
            rows.append((a, b, c))
    return rows

def dedupe_rows_in_sheet(
    rows: List[Tuple[Optional[str], Optional[str], Optional[str]]],
    ignore_case: bool
) -> List[Tuple[Optional[str], Optional[str], Optional[str]]]:
    """
    시트 내부 (A,B,C) '완전 동일' 중복 제거. 순서그대로, 처음 것만 남김.
    (유니코드 정규화/strip 등 추가 가공 없음. ignore_case=True면 소문자 비교만)
    """
    seen = set()
    out: List[Tuple[Optional[str], Optional[str], Optional[str]]] = []
    for a, b, c in rows:
        ka = a.lower() if (ignore_case and isinstance(a, str)) else a
        kb = b.lower() if (ignore_case and isinstance(b, str)) else b
        kc = c.lower() if (ignore_case and isinstance(c, str)) else c
        key = (ka, kb, kc)
        if key in seen:
            continue
        seen.add(key)
        out.append((a, b, c))
    return out

# ===== 저장 헬퍼 =====
def save_pdf(doc: fitz.Document, path: str, compact: bool = True):
    """compact=True: 무손실 재압축/정리(용량↓). False: 빠른 저장."""
    if compact:
        doc.save(path, deflate=True, garbage=4)
    else:
        doc.save(path, deflate=False, garbage=0)