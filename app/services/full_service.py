# app/services/full_service.py
import os
from concurrent.futures import ProcessPoolExecutor, as_completed
from typing import Dict, List, Tuple

import pandas as pd
import fitz  # PyMuPDF

from .common import (
    MAX_WORKERS, hex_to_rgb01, search_flags, gather_terms_full
)

def _scan_term_job(term, pdf_path, num_pages, flags):
    """서브프로세스에서 실행 (Full tag)"""
    label, rgb, text = term
    try:
        hits_by_page = []
        with fitz.open(pdf_path) as doc:
            for p in range(num_pages):
                page = doc.load_page(p)
                rects = page.search_for(text, flags=flags)  # 원문 그대로
                if rects:
                    page_hits = [(label, text, rgb, (r.x0, r.y0, r.x1, r.y1)) for r in rects]
                    hits_by_page.append((p, page_hits))
        return (label, text, hits_by_page, None)
    except Exception as e:
        return (label, text, [], str(e))

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

    color_map_rgb = {k: hex_to_rgb01(v) for k, v in color_hex_map.items()}
    terms = gather_terms_full(excel_path, clean_terms=clean_terms)
    if not terms:
        raise ValueError("엑셀에서 검색할 텍스트가 없습니다. (A/B/C/D 열 확인)")

    doc = fitz.open(pdf_input_path)
    num_pages = len(doc)
    flags = search_flags(ignore_case, whole_word)

    jobs = [(label, color_map_rgb.get(label, (1,1,0)), text) for (label, text) in terms]
    total_hits = 0
    not_found = []
    failed = []

    with ProcessPoolExecutor(max_workers=MAX_WORKERS) as ex:
        futures = {ex.submit(_scan_term_job, job, pdf_input_path, num_pages, flags): job for job in jobs}
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
                    annot.set_colors(stroke=_rgb, fill=_rgb)
                    annot.set_opacity(opacity)
                    annot.set_info(content=f"{_label}: {_text}", title="자동 검색")
                    annot.update()
                    total_hits += 1

    doc.save(pdf_output_path, deflate=True, garbage=4)
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
