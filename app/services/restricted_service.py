# app/services/restricted_service.py
import os
from concurrent.futures import as_completed
from typing import Dict, List, Tuple, Optional

import pandas as pd
import fitz  # PyMuPDF

from .common import (
    MAX_WORKERS,
    get_executor,
    hex_to_rgb01,
    page_lines_with_words,
    color_hex_from_sheet_name,
    rect_key,
    gather_restricted_rows_from_df,
    dedupe_rows_in_sheet,
    save_pdf,
    load_pdf_to_bytes,  # ✅ 추가
)


def _scan_restricted_page_multi_sheets(
        page_no: int,
        pdf_bytes: bytes,  # ✅ 경로 대신 바이트 데이터
        sheet_rows_map_norm: Dict[str, List[Tuple[int, List[str]]]],  # {sheet: [(row_idx, [frag,...])]}
        require_order: bool,
        ignore_case: bool,
):
    """
    페이지 단위로 모든 시트를 동시에 검사 + 중복 제거(라인/좌표).
    반환: (page_no, {sheet_name: {'matches':[(rect,line_text,row_idx),...],
                                  'line_seen':set(line_norm),
                                  'rect_seen':set(rect_key)}}, err)
    """
    try:
        per_sheet = {s: {'matches': [], 'line_seen': set(), 'rect_seen': set()} for s in sheet_rows_map_norm.keys()}

        # ✅ 메모리에서 PDF 열기
        with fitz.open(stream=pdf_bytes, filetype="pdf") as doc:
            page = doc.load_page(page_no)
            lines = page_lines_with_words(page)
            norm_lines = []
            for ln in lines:
                raw = ln["line_text"]
                norm = raw.lower() if ignore_case else raw
                norm_lines.append((ln["rect"], norm, raw))

            for sheet_name, rows in sheet_rows_map_norm.items():
                line_seen = per_sheet[sheet_name]['line_seen']
                rect_seen = per_sheet[sheet_name]['rect_seen']

                for (rect, ltext_norm, ltext_raw) in norm_lines:
                    if ltext_norm in line_seen:
                        continue

                    matched_row_idx = None
                    if require_order:
                        for row_idx, frags in rows:
                            pos = 0;
                            ok = True
                            for f in frags:
                                idx = ltext_norm.find(f, pos)
                                if idx < 0:
                                    ok = False;
                                    break
                                pos = idx + len(f)
                            if ok:
                                matched_row_idx = row_idx
                                break
                    else:
                        for row_idx, frags in rows:
                            if all(f in ltext_norm for f in frags):
                                matched_row_idx = row_idx
                                break

                    if matched_row_idx is not None:
                        rk = rect_key(rect)
                        if rk in rect_seen:
                            continue
                        per_sheet[sheet_name]['matches'].append((rect, ltext_raw, matched_row_idx))
                        line_seen.add(ltext_norm)
                        rect_seen.add(rk)

        return (page_no, per_sheet, None)
    except Exception as e:
        return (page_no, None, str(e))


def annotate_pdf_restricted_with_excel(
        excel_path: str,
        pdf_input_path: str,
        pdf_output_path: str,
        not_found_xlsx_path: str,
        color_hex: Optional[str] = None,  # 무시됨(시트별 자동 색상)
        opacity: float = 0.35,
        ignore_case: bool = True,
        require_order: bool = False,
        clean_terms: bool = False
) -> Dict:
    """
    Restricted:
    - 엑셀의 '모든 시트' 순회
    - 각 시트에서 A/B/C 조각(최소 2개) 수집
    - ★ 시트 '내부' 완전 동일(A,B,C) 중복 제거 후 검색
    - 라인에 모든 조각 포함되면 라인 전체 하이라이트
    - 시트별 색상(시트명 기반), not_found.xlsx는 시트별 워크시트
    """
    if not os.path.exists(excel_path):
        raise FileNotFoundError(f"엑셀 파일 없음: {excel_path}")
    if not os.path.exists(pdf_input_path):
        raise FileNotFoundError(f"PDF 파일 없음: {pdf_input_path}")

    # 모든 시트 읽기
    sheets: Dict[str, pd.DataFrame] = pd.read_excel(excel_path, header=None, sheet_name=None)
    if not sheets:
        raise ValueError("엑셀에 읽을 시트가 없습니다.")

    # 시트별 rows + 시트 내부 중복 제거
    sheet_rows_map: Dict[str, List[Tuple[Optional[str], Optional[str], Optional[str]]]] = {}
    per_sheet_rows_before_after: Dict[str, Tuple[int, int]] = {}

    for sname, sdf in sheets.items():
        rows = gather_restricted_rows_from_df(sdf, clean_terms=clean_terms)
        if not rows:
            continue
        dedup_rows = dedupe_rows_in_sheet(rows, ignore_case=ignore_case)
        if dedup_rows:
            sheet_rows_map[sname] = dedup_rows
            per_sheet_rows_before_after[sname] = (len(rows), len(dedup_rows))

    if not sheet_rows_map:
        raise ValueError("어느 시트에서도 유효한 (A/B/C) 조합을 찾지 못했습니다. (최소 2개 이상 값 필요)")

    # 색상
    sheet_color_rgb: Dict[str, Tuple[float, float, float]] = {
        sname: hex_to_rgb01(color_hex_from_sheet_name(sname)) for sname in sheet_rows_map.keys()
    }

    # 비교용 정규화 구조(소문자) : {sheet: [(row_idx, [frag,..]), ...]}
    sheet_rows_map_norm: Dict[str, List[Tuple[int, List[str]]]] = {}
    for sname, rows in sheet_rows_map.items():
        norm_rows: List[Tuple[int, List[str]]] = []
        for idx, (a, b, c) in enumerate(rows):
            frags = [x for x in (a, b, c) if x is not None]
            if ignore_case:
                frags = [f.lower() for f in frags]
            norm_rows.append((idx, frags))
        sheet_rows_map_norm[sname] = norm_rows

    # 발견된 행 인덱스 집합(미매칭 계산용)
    found_row_indices: Dict[str, set] = {s: set() for s in sheet_rows_map_norm.keys()}

    # ✅ PDF를 메모리에 로드 (한 번만)
    pdf_bytes = load_pdf_to_bytes(pdf_input_path)

    doc = fitz.open(pdf_input_path)
    num_pages = len(doc)
    total_hits = 0
    per_sheet_stats: List[Dict] = []

    # 페이지 병렬 처리 (동결환경에선 thread 풀로 자동 전환)
    with get_executor(max_workers=MAX_WORKERS) as ex:
        # ✅ pdf_bytes 전달
        futures = {
            ex.submit(
                _scan_restricted_page_multi_sheets,
                p, pdf_bytes, sheet_rows_map_norm, require_order, ignore_case
            ): p
            for p in range(num_pages)
        }

        for fut in as_completed(futures):
            pno = futures[fut]
            try:
                page_no, per_sheet, err = fut.result()
            except Exception:
                continue
            if err or per_sheet is None:
                continue

            page = doc.load_page(page_no)
            for sname, bucket in per_sheet.items():
                rgb = sheet_color_rgb[sname]
                for (x0, y0, x1, y1), line_text, row_idx in bucket['matches']:
                    annot = page.add_highlight_annot(fitz.Rect(x0, y0, x1, y1))
                    annot.set_colors(stroke=rgb)  # highlight는 fill 미지원
                    annot.set_opacity(opacity)
                    annot.set_info(content=f"{sname} : {line_text}", title="Restricted")
                    annot.update()
                    total_hits += 1
                    found_row_indices[sname].add(row_idx)

    # not_found 계산 (재스캔 없음)
    not_found_by_sheet: Dict[str, List[Tuple[Optional[str], Optional[str], Optional[str]]]] = {}
    for sname, rows in sheet_rows_map.items():
        found_idx_set = found_row_indices.get(sname, set())
        nf_rows = [row for idx, row in enumerate(rows) if idx not in found_idx_set]
        if nf_rows:
            not_found_by_sheet[sname] = nf_rows

        before, after = per_sheet_rows_before_after.get(sname, (len(rows), len(rows)))
        per_sheet_stats.append({
            "sheet": sname,
            "rows_before": before,
            "rows_after": after,
            "hits": len(found_idx_set),  # '발견된 행' 종류 수
            "not_found_count": len(nf_rows),
            "color_rgb": sheet_color_rgb[sname],
        })

    # ✅ 빠른 저장
    save_pdf(doc, pdf_output_path, compact=False)
    doc.close()

    # not_found.xlsx
    if not_found_by_sheet:
        with pd.ExcelWriter(not_found_xlsx_path, engine="openpyxl") as writer:
            for sname, rows in not_found_by_sheet.items():
                df_nf = pd.DataFrame(rows, columns=["A", "B", "C"])
                safe_name = sname[:31] if len(sname) > 31 else sname
                df_nf.to_excel(writer, sheet_name=safe_name if safe_name else "Sheet", index=False)

    rows_before_total = sum(b for (b, a) in per_sheet_rows_before_after.values()) if per_sheet_rows_before_after else 0
    rows_after_total = sum(a for (b, a) in per_sheet_rows_before_after.values()) if per_sheet_rows_before_after else 0

    return {
        "pages": num_pages,
        "sheets": len(sheet_rows_map),
        "hits": total_hits,
        "per_sheet": per_sheet_stats,
        "rows_before_total": rows_before_total,
        "rows_after_total": rows_after_total,
        "not_found_file_written": bool(not_found_by_sheet),
    }