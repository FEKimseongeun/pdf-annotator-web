#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
이 소스코드는 AUTOCAD에서 잘못 PDF 변환을 하면서 텍스트가 안 긁어지는 문제가 있을떄 사용하는 코드입니다.
CAD에서 잘못 변환 시 이미지와 같이 PDF로 변환이 됩니다. 그렇지만 모든 요소가 주석으로 들어가므로, 해당 주석에서 로직을 실행합니다.

엑셀의 '값' 열 텍스트와 PDF 주석 텍스트(content/subject/title)가 '정확 일치'하면
해당 주석의 색과 불투명도를 변경하고(가시화), 옵션에 따라 같은 영역에 반투명 박스 오버레이를 추가한다.

설치:
  python -m pip install PyMuPDF==1.24.10 pandas==2.2.2 openpyxl==3.1.2

예시:
  python pdf_annot_mark_agent.py ^
    -x "C:\\path\\data.xlsx" ^
    -p "C:\\path\\input.pdf" ^
    -o "C:\\path\\out_marked.pdf" ^
    --sheet "Sheet1" ^
    --value-col "값" ^
    --color "#FFCC00" ^
    --opacity 0.3 ^
    --case-insensitive ^
    --add-overlay
"""

import argparse
import re
from typing import Set, Optional, Tuple, List

import pandas as pd
import fitz  # PyMuPDF


# -------------------------
# 유틸
# -------------------------
def hex_to_rgb01(hex_color: str) -> Tuple[float, float, float]:
    """#RRGGBB -> (r,g,b) 각 0~1 float"""
    hc = hex_color.strip().lstrip("#")
    if len(hc) != 6 or not re.fullmatch(r"[0-9A-Fa-f]{6}", hc):
        raise ValueError(f"Invalid HEX color: {hex_color}")
    r = int(hc[0:2], 16) / 255.0
    g = int(hc[2:4], 16) / 255.0
    b = int(hc[4:6], 16) / 255.0
    return (r, g, b)


def normalize_text(s: Optional[str], ignore_case: bool = True) -> str:
    """공백 정규화 + 대소문자 옵션"""
    if s is None:
        return ""
    s = re.sub(r"\s+", " ", str(s)).strip()
    if ignore_case:
        s = s.lower()
    return s


# -------------------------
# 엑셀 로딩
# -------------------------
def load_values_from_excel(
    excel_path: str,
    sheet_name: Optional[str],
    value_col: str,
    ignore_case: bool = True,
) -> Set[str]:
    df = pd.read_excel(excel_path, sheet_name=sheet_name, dtype=str)
    if value_col not in df.columns:
        raise KeyError(f"엑셀 시트에 '{value_col}' 열이 없습니다. 실제 헤더: {list(df.columns)}")

    values = set()
    for v in df[value_col].dropna().tolist():
        vv = normalize_text(v, ignore_case=ignore_case)
        if vv:
            values.add(vv)
    return values


# -------------------------
# PDF 처리
# -------------------------
def collect_annot_texts(annot) -> List[str]:
    """
    주석의 비교 대상 텍스트를 최대한 수집.
    - info['content'] (메모/자유텍스트 본문)
    - info['subject'], info['title'] 도 비교군에 포함
    """
    candidates = []
    try:
        info = annot.info or {}
    except Exception:
        info = {}

    for key in ("content", "subject", "title"):
        val = info.get(key)
        if isinstance(val, str) and val.strip():
            candidates.append(val)
    return candidates


def mark_annotation(
    page: fitz.Page,
    annot,
    rgb: Tuple[float, float, float],
    add_overlay: bool,
    opacity: float = 0.25,
):
    """주석 색 변경 및 오버레이(사각형) 추가"""
    try:
        annot.set_colors(stroke=rgb, fill=rgb)
        annot.set_border(width=1.5)
        annot.set_opacity(opacity)   # ✅ 불투명도 적용 (0.0~1.0)
        annot.update()
    except Exception:
        # 일부 주석 타입은 색/불투명도 변경이 제한될 수 있음
        pass

    if add_overlay:
        rect = annot.rect
        try:
            sq = page.add_rect_annot(rect)
            sq.set_colors(stroke=rgb, fill=rgb)
            sq.set_opacity(opacity)
            sq.set_border(width=0.5)
            sq.update()
        except Exception:
            pass


def process_pdf(
    pdf_path: str,
    out_path: str,
    target_values: Set[str],
    color_hex: str = "#FFCC00",
    ignore_case: bool = True,
    add_overlay: bool = True,
    opacity: float = 0.25,
) -> int:
    """PDF의 모든 주석을 순회하며 엑셀 '값'과 일치하면 표시. 반환: 매칭된 주석 수"""
    rgb = hex_to_rgb01(color_hex)
    doc = fitz.open(pdf_path)

    matched = 0
    for page_num in range(doc.page_count):
        page = doc.load_page(page_num)
        annot = page.first_annot
        while annot:
            texts = collect_annot_texts(annot)
            is_hit = False
            for t in texts:
                nt = normalize_text(t, ignore_case=ignore_case)
                if nt and nt in target_values:
                    is_hit = True
                    break

            if is_hit:
                mark_annotation(page, annot, rgb, add_overlay, opacity)
                matched += 1

            annot = annot.next

    # 저장
    if matched > 0:
        doc.save(out_path)
    # 매칭이 없을 때도 결과물을 강제로 저장하려면 위 if를 제거하고 항상 save

    doc.close()
    return matched


# -------------------------
# 메인
# -------------------------
def parse_args():
    p = argparse.ArgumentParser(
        description="엑셀의 '값' 열과 PDF 주석 텍스트를 비교하여 일치 주석을 색상/불투명도로 표시"
    )
    p.add_argument("-x", "--excel", required=True, help="엑셀 파일 경로 (xlsx)")
    p.add_argument("-p", "--pdf", required=True, help="입력 PDF 경로")
    p.add_argument("-o", "--out", required=True, help="출력 PDF 경로 (예: out_marked.pdf)")
    p.add_argument("--sheet", default=None, help="시트명 (기본: 첫 시트)")
    p.add_argument("--value-col", default="값", help="엑셀에서 비교할 열 헤더 (기본: '값')")
    p.add_argument("--color", default="#FFCC00", help="표시 색상 HEX (기본: #FFCC00)")
    p.add_argument(
        "--opacity",
        type=float,
        default=0.25,
        help="주석/오버레이 불투명도 0.0~1.0 (기본: 0.25)",
    )
    p.add_argument(
        "--case-insensitive",
        action="store_true",
        help="대소문자 무시(한국어/영문 섞여 있을 때 권장)",
    )
    p.add_argument(
        "--add-overlay",
        action="store_true",
        help="기존 주석 색 변경 외에 반투명 사각형 오버레이도 추가",
    )
    return p.parse_args()


def main():
    args = parse_args()

    values = load_values_from_excel(
        excel_path=args.excel,
        sheet_name=args.sheet,
        value_col=args.value_col,
        ignore_case=args.case_insensitive,
    )
    if not values:
        print("[WARN] 엑셀의 '값' 집합이 비었습니다. 종료합니다.")
        return

    print(f"[INFO] 로드된 비교 대상 개수: {len(values)}")
    print(f"[INFO] PDF 처리 시작: {args.pdf}")

    matched = process_pdf(
        pdf_path=args.pdf,
        out_path=args.out,
        target_values=values,
        color_hex=args.color,
        ignore_case=args.case_insensitive,
        add_overlay=args.add_overlay,
        opacity=args.opacity,
    )
    print(f"[DONE] 매칭된 주석 수: {matched}")
    if matched > 0:
        print(f"[SAVED] 결과 저장: {args.out}")
    else:
        print("[INFO] 매칭이 없어 출력 파일을 저장하지 않았습니다. (--add-overlay 유무와 무관)")

if __name__ == "__main__":
    main()
