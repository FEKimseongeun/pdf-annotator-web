# annotate_service.py

import os
import random
import hashlib
from concurrent.futures import ProcessPoolExecutor, as_completed
from multiprocessing import cpu_count
from typing import Dict, List, Tuple, Optional

import pandas as pd
import fitz  # PyMuPDF

# A/B/C/D → 엑셀 컬럼 인덱스 매핑 (0-based)
COLUMN_MAP: List[Tuple[int, str]] = [
    (0, "A"),
    (1, "B"),
    (2, "C"),
    (3, "D"),
]

MAX_WORKERS = max(1, cpu_count() - 1)

# ===========================
# 공통 유틸
# ===========================

def _hex_to_rgb01(hex_str: str) -> Tuple[float, float, float]:
    s = hex_str.strip()
    if s.startswith("#"):
        s = s[1:]
    if len(s) != 6:
        raise ValueError(f"Invalid hex color: {hex_str}")
    r = int(s[0:2], 16) / 255.0
    g = int(s[2:4], 16) / 255.0
    b = int(s[4:6], 16) / 255.0
    return (r, g, b)

def _search_flags(ignore_case: bool, whole_word: bool) -> int:
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

def _page_lines_with_words(page: fitz.Page):
    """
    페이지를 words 단위로 읽어 line별로 묶어서:
    - line_text: 라인 문자열
    - rect: 해당 라인의 전체 bbox (union)
    반환: List[dict{block,line,line_text,rect}]
    """
    words = page.get_text("words")  # [x0,y0,x1,y1,word, block_no, line_no, word_no]
    lines_map = {}
    for w in words:
        x0, y0, x1, y1, token, block_no, line_no, word_no = w
        key = (block_no, line_no)
        if key not in lines_map:
            lines_map[key] = {"tokens": [], "rect": [x0, y0, x1, y1]}
        lines_map[key]["tokens"].append(token)
        # bbox union
        r = lines_map[key]["rect"]
        r[0] = min(r[0], x0); r[1] = min(r[1], y0); r[2] = max(r[2], x1); r[3] = max(r[3], y1)

    out = []
    for (block_no, line_no), v in lines_map.items():
        text = " ".join(v["tokens"])
        out.append({
            "block": block_no,
            "line": line_no,
            "line_text": text,
            "rect": tuple(v["rect"])
        })
    return out

# ===========================
# Full tag (그대로)
# ===========================

def _scan_term_job(term, pdf_path, num_pages, flags):
    """Full tag용: 서브프로세스에서 실행"""
    label, rgb, text = term
    try:
        hits_by_page = []
        with fitz.open(pdf_path) as doc:
            for p in range(num_pages):
                page = doc.load_page(p)
                rects = page.search_for(text, flags=flags)  # 전처리 없이 원문 그대로
                if rects:
                    page_hits = [(label, text, rgb, (r.x0, r.y0, r.x1, r.y1)) for r in rects]
                    hits_by_page.append((p, page_hits))
        return (label, text, hits_by_page, None)
    except Exception as e:
        return (label, text, [], str(e))

def _gather_terms(excel_path: str, clean_terms: bool) -> List[Tuple[str, str]]:
    """
    Full tag용: 엑셀에서 A/B/C/D 열 순회하며 검색어 수집
    - clean_terms=False: 원문 그대로
    - 헤더 'A','B','C','D' 제외(대소문자 무시)
    """
    df = pd.read_excel(excel_path, header=None)
    out: List[Tuple[str, str]] = []

    for col_idx, label in COLUMN_MAP:
        if col_idx not in df.columns:
            continue
        s = df[col_idx].dropna().astype(str)
        if clean_terms:
            s = s.map(lambda x: x.strip())
        s = s[s != ""]
        s = s[~s.str.fullmatch(label, case=False)]
        # 순서 유지 중복 제거
        terms = list(dict.fromkeys(s.tolist()))
        for t in terms:
            out.append((label, t))
    return out

def annotate_pdf_with_excel(
    excel_path: str,
    pdf_input_path: str,
    pdf_output_path: str,
    not_found_xlsx_path: str,
    color_hex_map: Dict[str, str],
    opacity: float = 0.35,
    ignore_case: bool = True,
    whole_word: bool = False,
    clean_terms: bool = False
) -> Dict:
    """Full tag: 특정 문자열이 그대로 매치되면 그 영역 하이라이트"""
    if not os.path.exists(excel_path):
        raise FileNotFoundError(f"엑셀 파일 없음: {excel_path}")
    if not os.path.exists(pdf_input_path):
        raise FileNotFoundError(f"PDF 파일 없음: {pdf_input_path}")

    color_map_rgb = {k: _hex_to_rgb01(v) for k, v in color_hex_map.items()}
    terms = _gather_terms(excel_path, clean_terms=clean_terms)
    if not terms:
        raise ValueError("엑셀에서 검색할 텍스트가 없습니다. (A/B/C/D 열 확인)")

    doc = fitz.open(pdf_input_path)
    num_pages = len(doc)
    flags = _search_flags(ignore_case, whole_word)

    jobs = [(label, color_map_rgb.get(label, (1,1,0)), text) for (label, text) in terms]

    total_hits = 0
    not_found = []
    failed = []

    with ProcessPoolExecutor(max_workers=MAX_WORKERS) as ex:
        futures = {
            ex.submit(_scan_term_job, job, pdf_input_path, num_pages, flags): job
            for job in jobs
        }

        for fut in as_completed(futures):
            (label, rgb, text) = futures[fut]
            try:
                lbl, txt, hits_by_page, err = fut.result()
            except Exception as e:
                failed.append((label, text, str(e)))
                continue

            if err:
                failed.append((label, text, str(err)))
                continue

            if not hits_by_page:
                not_found.append((label, text))
                continue

            for pno, page_hits in hits_by_page:
                page = doc.load_page(pno)
                for (_label, _text, _rgb, (x0, y0, x1, y1)) in page_hits:
                    annot = page.add_highlight_annot(fitz.Rect(x0, y0, x1, y1))
                    annot.set_colors(stroke=_rgb)
                    annot.set_opacity(opacity)
                    annot.set_info(content=f"{_label}: {_text}", title="자동 검색")
                    annot.update()
                    total_hits += 1

    doc.save(pdf_output_path)
    doc.close()

    not_found_count = len(not_found)
    if not_found_count > 0:
        pd.DataFrame(not_found, columns=["Label", "Text"]).to_excel(not_found_xlsx_path, index=False)

    return {
        "pages": num_pages,
        "terms": len(terms),
        "hits": total_hits,
        "not_found_count": not_found_count,
        "failed_count": len(failed),
    }

# ===========================
# Restricted tag (개선)
#   - 여러 시트 지원
#   - 시트별 랜덤 색상 (시트명 기반 해시로 재현성 확보)
#   - 주석 info: "{시트이름} : {조각A / 조각B / 조각C}"
#   - not_found_xlsx: 시트별 워크시트로 기록
# ===========================

def _check_fragments_in_line(
    line_text: str,
    fragments: List[str],
    ignore_case: bool,
    require_order: bool
) -> bool:
    """
    한 라인 문자열에 fragments(A,B,C 등)가 모두 포함되는지 체크.
    - ignore_case: 대소문자 무시
    - require_order: True이면 A→B→C 순서로 등장해야 함.
    """
    if ignore_case:
        src = line_text.lower()
        frags = [f.lower() for f in fragments]
    else:
        src = line_text
        frags = fragments

    if require_order:
        pos = 0
        for f in frags:
            idx = src.find(f, pos)
            if idx < 0:
                return False
            pos = idx + len(f)
        return True
    else:
        # 순서 무관: 모두 포함 여부
        return all(f in src for f in frags)

def _gather_restricted_rows_from_df(df: pd.DataFrame, clean_terms: bool) -> List[Tuple[Optional[str], Optional[str], Optional[str]]]:
    """
    단일 시트(df)에서 A(0), B(1), C(2) 열의 행 단위 조합 수집.
    - 각 행에 대해 (A,B,C)를 튜플로 반환 (빈 문자열은 None 처리)
    - 최소 2개 이상 값이 있어야 유효
    """
    rows = []
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

def _scan_restricted_page_job_multi(
    page_no: int,
    pdf_path: str,
    rows_abc: List[Tuple[Optional[str], Optional[str], Optional[str]]],
    ignore_case: bool,
    require_order: bool
):
    """
    Restricted용: 페이지 단위 작업(시트 1개 기준)
    반환: (page_no, matches, err)
      - matches: List[ ( (x0,y0,x1,y1), "해당 라인의 전체 텍스트" ) ]
    """
    try:
        matches = []
        with fitz.open(pdf_path) as doc:
            page = doc.load_page(page_no)
            lines = _page_lines_with_words(page)

            for (a, b, c) in rows_abc:
                fragments = [x for x in (a, b, c) if x is not None]
                if not fragments:
                    continue
                for ln in lines:
                    if _check_fragments_in_line(ln["line_text"], fragments, ignore_case, require_order):
                        # 조각이 포함된 라인의 전체 텍스트를 저장
                        matches.append((ln["rect"], ln["line_text"]))
        return (page_no, matches, None)
    except Exception as e:
        return (page_no, [], str(e))

def _color_hex_from_sheet_name(sheet_name: str) -> str:
    """
    시트명 기반 해시로 6자리 색상 생성 (재현성 보장)
    너무 어둡거나 밝은 값을 피하기 위해 0x40~0xC0 범위로 채택
    """
    h = hashlib.sha256(sheet_name.encode("utf-8")).hexdigest()
    # 3채널로 나눠서 각 0x40~0xC0 범위로 매핑
    def pick(i):
        v = int(h[i:i+2], 16)  # 0..255
        v = 0x40 + int((v / 255.0) * (0xC0 - 0x40))  # 0x40..0xC0
        return v
    r, g, b = pick(0), pick(2), pick(4)
    return "#{:02X}{:02X}{:02X}".format(r, g, b)

def annotate_pdf_restricted_with_excel(
    excel_path: str,
    pdf_input_path: str,
    pdf_output_path: str,
    not_found_xlsx_path: str,
    color_hex: Optional[str] = None,      # 무시됨(시트별 자동 색상)
    opacity: float = 0.35,
    ignore_case: bool = True,
    require_order: bool = False,
    clean_terms: bool = False
) -> Dict:
    """
    Restricted tag (개선 버전):
    - 엑셀의 '모든 시트'를 순회
    - 각 시트에서 A/B/C 열의 조각 조합(최소 2개 이상 존재하는 행)을 사용
    - 같은 라인 문자열에 모든 조각이 포함되면 해당 라인 rect 하이라이트
    - 시트별로 서로 다른 색상(시트명 기반 랜덤)을 적용
    - 하이라이트 주석 info: "{시트이름} : {조각A / 조각B / 조각C}"
    - not_found_xlsx: 시트별 워크시트에 기록
    """
    if not os.path.exists(excel_path):
        raise FileNotFoundError(f"엑셀 파일 없음: {excel_path}")
    if not os.path.exists(pdf_input_path):
        raise FileNotFoundError(f"PDF 파일 없음: {pdf_input_path}")

    # 모든 시트 읽기
    sheets: Dict[str, pd.DataFrame] = pd.read_excel(excel_path, header=None, sheet_name=None)
    if not sheets:
        raise ValueError("엑셀에 읽을 시트가 없습니다.")

    # 시트별 rows 수집
    sheet_rows_map: Dict[str, List[Tuple[Optional[str], Optional[str], Optional[str]]]] = {}
    for sname, sdf in sheets.items():
        rows = _gather_restricted_rows_from_df(sdf, clean_terms=clean_terms)
        if rows:
            sheet_rows_map[sname] = rows
    if not sheet_rows_map:
        raise ValueError("어느 시트에서도 유효한 (A/B/C) 조합을 찾지 못했습니다. (최소 2개 이상 값이 있는 행 필요)")

    # 시트별 색상 (시트명 기반)
    sheet_color_rgb: Dict[str, Tuple[float, float, float]] = {
        sname: _hex_to_rgb01(_color_hex_from_sheet_name(sname)) for sname in sheet_rows_map.keys()
    }

    doc = fitz.open(pdf_input_path)
    num_pages = len(doc)

    # not_found를 시트별로 기록할 수 있도록 준비
    not_found_by_sheet: Dict[str, List[Tuple[Optional[str], Optional[str], Optional[str]]]] = {}

    total_hits = 0
    per_sheet_stats = []

    # 각 시트를 독립 처리 (로직 동일, 색상/info만 다름)
    for sheet_name, rows_abc in sheet_rows_map.items():
        rgb = sheet_color_rgb[sheet_name]
        sheet_total_hits = 0
        sheet_not_found: List[Tuple[Optional[str], Optional[str], Optional[str]]] = []

        # 페이지 병렬 처리
        with ProcessPoolExecutor(max_workers=MAX_WORKERS) as ex:
            futures = {
                ex.submit(_scan_restricted_page_job_multi, p, pdf_input_path, rows_abc, ignore_case, require_order): p
                for p in range(num_pages)
            }

            for fut in as_completed(futures):
                pno = futures[fut]
                try:
                    page_no, matches, err = fut.result()
                except Exception:
                    continue
                if err:
                    continue

                if not matches:
                    continue

                page = doc.load_page(page_no)
                for (x0, y0, x1, y1), line_text in matches:
                    annot = page.add_highlight_annot(fitz.Rect(x0, y0, x1, y1))
                    annot.set_colors(stroke=rgb)
                    annot.set_opacity(opacity)
                    annot.set_info(content=f"{sheet_name} : {line_text}", title="Restricted")
                    annot.update()
                    sheet_total_hits += 1
                    total_hits += 1

        # 시트별 not_found 재확인 (전역 스캔)
        # 행 단위로 문서 전체에서 한 번도 매칭 안 된 경우를 수집
        for (a, b, c) in rows_abc:
            fragments = [x for x in (a, b, c) if x is not None]
            found_any = False
            for p in range(num_pages):
                page = doc.load_page(p)
                lines = _page_lines_with_words(page)
                for ln in lines:
                    if _check_fragments_in_line(ln["line_text"], fragments, ignore_case, require_order):
                        found_any = True
                        break
                if found_any:
                    break
            if not found_any:
                sheet_not_found.append((a, b, c))

        if sheet_not_found:
            not_found_by_sheet[sheet_name] = sheet_not_found

        per_sheet_stats.append({
            "sheet": sheet_name,
            "rows": len(rows_abc),
            "hits": sheet_total_hits,
            "not_found_count": len(sheet_not_found),
            "color_rgb": rgb
        })

    # 저장
    doc.save(pdf_output_path)
    doc.close()

    # not_found를 시트별 시트로 기록
    if not_found_by_sheet:
        with pd.ExcelWriter(not_found_xlsx_path, engine="openpyxl") as writer:
            for sname, rows in not_found_by_sheet.items():
                df_nf = pd.DataFrame(rows, columns=["A", "B", "C"])
                # 시트명이 31자 제한 등으로 문제되면 잘라줌
                safe_name = sname[:31] if len(sname) > 31 else sname
                df_nf.to_excel(writer, sheet_name=safe_name if safe_name else "Sheet", index=False)

    return {
        "pages": num_pages,
        "sheets": len(sheet_rows_map),
        "hits": total_hits,
        "per_sheet": per_sheet_stats,
        "not_found_file_written": bool(not_found_by_sheet)
    }
