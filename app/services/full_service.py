# app/services/full_service.py
import os
from concurrent.futures import as_completed
from typing import Dict, List, Tuple

import pandas as pd
import fitz  # PyMuPDF

from .common import (
    MAX_WORKERS, get_executor, hex_to_rgb01, search_flags,
    gather_terms_full, save_pdf
)


def _scan_page_for_all_terms(
        page_no: int,
        pdf_path: str,
        terms_with_meta: List[Tuple[str, str, Tuple[float, float, float]]],  # (label, text, rgb)
        flags: int
) -> Tuple[int, List[Tuple[str, str, Tuple[float, float, float], fitz.Rect]], str | None]:
    """
    【최적화된 함수】
    하나의 페이지를 열어 모든 검색어(terms)를 한 번에 검색합니다.
    반환값: (페이지 번호, [(label, text, rgb, rect), ...], 에러 메시지)
    """
    page_hits = []
    try:
        # 각 프로세스/스레드에서 문서를 독립적으로 엽니다.
        with fitz.open(pdf_path) as doc:
            page = doc.load_page(page_no)
            for label, text, rgb in terms_with_meta:
                rects = page.search_for(text, flags=flags)
                for r in rects:
                    page_hits.append((label, text, rgb, r))
        return (page_no, page_hits, None)
    except Exception as e:
        return (page_no, [], str(e))


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
    if not os.path.exists(excel_path):
        raise FileNotFoundError(f"엑셀 파일 없음: {excel_path}")
    if not os.path.exists(pdf_input_path):
        raise FileNotFoundError(f"PDF 파일 없음: {pdf_input_path}")

    # 1. 엑셀에서 모든 검색어 목록을 미리 준비합니다.
    color_map_rgb = {k: hex_to_rgb01(v) for k, v in color_hex_map.items()}
    all_terms = gather_terms_full(excel_path, clean_terms=clean_terms)
    if not all_terms:
        raise ValueError("엑셀에서 검색할 텍스트가 없습니다. (A/B/C/D 열 확인)")

    terms_with_meta = [(label, text, color_map_rgb.get(label, (1, 1, 0))) for label, text in all_terms]

    doc = fitz.open(pdf_input_path)
    num_pages = len(doc)
    flags = search_flags(ignore_case, whole_word)

    total_hits = 0
    found_terms = set()
    failed_pages = []

    # 2. 【로직 변경】페이지 단위로 작업을 병렬 처리합니다.
    with get_executor(max_workers=MAX_WORKERS) as ex:
        # 각 페이지를 스캔하는 작업을 제출
        futures = {
            ex.submit(_scan_page_for_all_terms, p, pdf_input_path, terms_with_meta, flags): p
            for p in range(num_pages)
        }

        for fut in as_completed(futures):
            try:
                page_no, page_hits, err = fut.result()
            except Exception as e:
                failed_pages.append((futures[fut], str(e)))
                continue

            if err:
                failed_pages.append((page_no, err))
                continue

            if not page_hits:
                continue

            # 3. 해당 페이지에 대한 모든 검색 결과를 한 번에 주석으로 추가합니다.
            page = doc.load_page(page_no)
            for label, text, rgb, rect in page_hits:
                annot = page.add_highlight_annot(rect)
                annot.set_colors(stroke=rgb)
                annot.set_opacity(opacity)
                annot.set_info(content=f"{label}: {text}", title="자동 검색")
                annot.update()
                total_hits += 1
                found_terms.add((label, text))

    # 4. 발견되지 않은 검색어를 계산합니다.
    all_terms_set = set(all_terms)
    not_found = list(all_terms_set - found_terms)

    save_pdf(doc, pdf_output_path, compact=True)
    doc.close()

    not_found_count = len(not_found)
    if not_found_count > 0:
        pd.DataFrame(not_found, columns=["Label", "Text"]).to_excel(not_found_xlsx_path, index=False)

    return {
        "pages": num_pages,
        "terms": len(all_terms),
        "hits": total_hits,
        "not_found_count": not_found_count,
        "failed_count": len(failed_pages),
    }