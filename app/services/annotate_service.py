import os
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
# Restricted tag (신규)
# ===========================

def _gather_restricted_rows(excel_path: str, clean_terms: bool) -> List[Tuple[Optional[str], Optional[str], Optional[str]]]:
    """
    Restricted용: A(0), B(1), C(2) 열의 행 단위 조합을 수집
    - 각 행에 대해 (A,B,C)를 튜플로 반환 (빈 문자열은 None 처리)
    - 최소 2개 이상 값이 있어야 의미가 있으므로, (A,B,C) 중 non-empty 개수 < 2 이면 스킵 권장
    """
    df = pd.read_excel(excel_path, header=None)
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

        # 최소 2개 이상 값 존재 시에만 의미있다고 판단 (원하면 >=1로 완화 가능)
        non_empty = sum(x is not None for x in (a, b, c))
        if non_empty >= 2:
            rows.append((a, b, c))
    return rows

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
            lines_map[key] = {
                "tokens": [],
                "rect": [x0, y0, x1, y1]
            }
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

def _scan_restricted_page_job(
    page_no: int,
    pdf_path: str,
    rows_abc: List[Tuple[Optional[str], Optional[str], Optional[str]]],
    ignore_case: bool,
    require_order: bool
):
    """
    Restricted용: 페이지 단위 작업
    - 각 라인의 텍스트를 만들고, (A,B,C) 중 존재하는 조각들이 모두 포함되면 해당 라인을 히트로 반환
    - 반환: (page_no, [rects...], hit_count_per_row)
    """
    try:
        rects = []
        hits_per_row = 0
        with fitz.open(pdf_path) as doc:
            page = doc.load_page(page_no)
            lines = _page_lines_with_words(page)

            for (a, b, c) in rows_abc:
                fragments = [x for x in (a, b, c) if x is not None]
                for ln in lines:
                    if _check_fragments_in_line(ln["line_text"], fragments, ignore_case, require_order):
                        rects.append(ln["rect"])
                        hits_per_row += 1
        return (page_no, rects, hits_per_row, None)
    except Exception as e:
        return (page_no, [], 0, str(e))

def annotate_pdf_restricted_with_excel(
    excel_path: str,
    pdf_input_path: str,
    pdf_output_path: str,
    not_found_xlsx_path: str,
    color_hex: str,
    opacity: float = 0.35,
    ignore_case: bool = True,
    require_order: bool = False,
    clean_terms: bool = False
) -> Dict:
    """
    Restricted tag:
    - 엑셀의 각 행에서 (A,B,C) 일부/전체 조각을 뽑아 '같은 라인' 문자열에 모두 포함된 경우,
      해당 라인 전체 bbox(단일 rect, union)로 하이라이트
    - 기본은 순서 무관; 순서 강제 옵션(require_order)로 A→B→C 순서 체크 가능
    """
    if not os.path.exists(excel_path):
        raise FileNotFoundError(f"엑셀 파일 없음: {excel_path}")
    if not os.path.exists(pdf_input_path):
        raise FileNotFoundError(f"PDF 파일 없음: {pdf_input_path}")

    rows_abc = _gather_restricted_rows(excel_path, clean_terms=clean_terms)
    if not rows_abc:
        raise ValueError("엑셀 A/B/C 열에서 사용할 조합이 없습니다. (최소 2개 이상 값이 있는 행 필요)")

    doc = fitz.open(pdf_input_path)
    num_pages = len(doc)
    rgb = _hex_to_rgb01(color_hex)

    total_hits = 0
    # not_found는 '어떤 행의 조합도 전체 문서에서 한 번도 매칭되지 않은 경우' 기록
    not_found_rows: List[Tuple[Optional[str], Optional[str], Optional[str]]] = []
    # 먼저 각 행 별 히트 수를 세기 위해 누적 카운터 준비
    match_counter = [0] * len(rows_abc)

    # 페이지 병렬 처리
    with ProcessPoolExecutor(max_workers=MAX_WORKERS) as ex:
        futures = {
            ex.submit(_scan_restricted_page_job, p, pdf_input_path, rows_abc, ignore_case, require_order): p
            for p in range(num_pages)
        }

        for fut in as_completed(futures):
            pno = futures[fut]
            try:
                page_no, rects, hits_per_row, err = fut.result()
            except Exception as e:
                # 페이지 단위 실패는 스킵
                continue

            if err:
                continue

            # 하이라이트 적용
            page = doc.load_page(page_no)
            for (x0, y0, x1, y1) in rects:
                annot = page.add_highlight_annot(fitz.Rect(x0, y0, x1, y1))
                annot.set_colors(stroke=rgb)
                annot.set_opacity(opacity)
                annot.set_info(content="Restricted Tag Line", title="자동 검색")
                annot.update()
                total_hits += 1

    # 각 행의 매칭 여부 집계: 위에서 hits_per_row를 개별 행에 매핑하는 로직을 간소화했지만,
    # 페이지별 스캔에서 행 구분 누적이 필요하면 _scan_restricted_page_job에서
    # rows_abc 인덱스마다 카운트를 반영하도록 확장 가능.
    # 여기서는 단순화: 라인 매칭이 하나도 없었으면 전체 rows를 'not found'로 간주하지 않고,
    # 문서 전역 라인에서 A/B/C 조합 매칭 여부를 추가 스캔해 기록한다.

    # 간단한 전역 스캔으로 not_found_rows 재확인
    # (성능 이슈가 크면 상단에서 per-row 카운트까지 같이 반환하도록 리팩토링 가능)
    # --- 전역 재확인 시작 ---
    for i, (a, b, c) in enumerate(rows_abc):
        found_any = False
        fragments = [x for x in (a, b, c) if x is not None]
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
            not_found_rows.append((a, b, c))
    # --- 전역 재확인 끝 ---

    # 저장 및 반환
    doc.save(pdf_output_path)
    doc.close()

    not_found_count = len(not_found_rows)
    if not_found_count > 0:
        pd.DataFrame(not_found_rows, columns=["A", "B", "C"]).to_excel(not_found_xlsx_path, index=False)

    return {
        "pages": num_pages,
        "terms": len(rows_abc),        # 행 조합 수
        "hits": total_hits,            # 라인 하이라이트 총 개수
        "not_found_count": not_found_count,
        "failed_count": 0,
    }
