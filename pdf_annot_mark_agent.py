#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
python pdf_pipeline_mark.py -p "E:\2_EDE\제주청정\발전\PIDForjeju.pdf" -x "E:\2_EDE\제주청정\발전\out.xlsx" -o "E:\2_EDE\제주청정\발전\out_marked.pdf"
"""

import argparse
import re
from typing import Dict, List, Optional, Set, Tuple

import pandas as pd
import fitz  # PyMuPDF


# -------------------------
# 텍스트/색상 유틸
# -------------------------
def hex_to_rgb01(hex_color: str) -> Tuple[float, float, float]:
    hc = hex_color.strip().lstrip("#")
    if len(hc) != 6 or not re.fullmatch(r"[0-9A-Fa-f]{6}", hc):
        raise ValueError(f"Invalid HEX color: {hex_color}")
    return (int(hc[0:2], 16) / 255.0, int(hc[2:4], 16) / 255.0, int(hc[4:6], 16) / 255.0)


def normalize_text(s: Optional[str], ignore_case: bool = True) -> str:
    if s is None:
        return ""
    s = re.sub(r"\s+", " ", str(s)).strip()
    return s.lower() if ignore_case else s


def is_only_letters_kor(s: str) -> bool:
    """영문/한글/공백만으로 구성되었는지(숫자, 특수문자, 하이픈 없으면 True)."""
    if s is None:
        return False
    s2 = s.replace(" ", "")
    # 한글/영문만 (단순 근사치)
    return bool(re.fullmatch(r"[A-Za-z가-힣]+", s2))


def hyphen_count(s: str) -> int:
    return s.count("-") if s else 0


def last_tail(s: str) -> str:
    """마지막 하이픈 뒤 문자열(없으면 빈 문자열). 공백은 보존."""
    if not s or "-" not in s:
        return ""
    return s.rsplit("-", 1)[1]


def is_digits_only(s: str) -> bool:
    s2 = s.strip()
    return len(s2) > 0 and s2.isdigit()

def has_parentheses(s: str) -> bool:
    return "(" in s or ")" in s

def is_digits_only_str(s: str) -> bool:
    if s is None:
        return False
    s2 = str(s).strip().replace(" ", "")
    return len(s2) > 0 and s2.isdigit()

def has_special_excluding_hyphen(s: str) -> bool:
    """하이픈(-)은 제외하고, 다른 특수문자가 하나라도 포함되면 True"""
    if s is None:
        return False
    # 영문/숫자/한글/공백/하이픈만 허용. 그 외 문자가 있으면 특수문자.
    return bool(re.search(r"[^A-Za-z0-9가-힣\s\-]", s))


# -------------------------
# PDF → 주석 텍스트 추출
# -------------------------
def collect_annot_texts(annot) -> List[str]:
    """주석의 text성 필드(content/subject/title)만 수집."""
    out = []
    try:
        info = annot.info or {}
    except Exception:
        info = {}
    for key in ("content", "subject", "title"):
        val = info.get(key)
        if isinstance(val, str) and val.strip():
            out.append(val.strip())
    return out


def extract_pdf_annots_to_df(pdf_path: str) -> pd.DataFrame:
    """PDF 전 페이지에서 텍스트 주석만 추출 → DataFrame[Page, 값]."""
    rows = []
    doc = fitz.open(pdf_path)
    for pno in range(doc.page_count):
        page = doc.load_page(pno)
        annot = page.first_annot
        while annot:
            texts = collect_annot_texts(annot)
            # 텍스트가 하나라도 있으면 각각 행으로 저장 (중복은 나중에 제거)
            for t in texts:
                rows.append({"Page": pno + 1, "값": t})
            annot = annot.next
    doc.close()
    df = pd.DataFrame(rows, columns=["Page", "값"])
    # 완전 중복 제거
    if not df.empty:
        df = df.drop_duplicates().reset_index(drop=True)
    return df


# -------------------------
# 데이터 정제 + 도면명(name_arr) 분리/저장 + PID NO(OWNER) 채우기
# -------------------------
def refine_df_and_build_name_arr(df: pd.DataFrame) -> Tuple[pd.DataFrame, Dict[int, str]]:
    if df.empty:
        return df.copy(), {}

    # 안전 캐스팅
    work = df.copy()
    work["Page"] = pd.to_numeric(work["Page"], errors="coerce").astype("Int64")
    work = work.dropna(subset=["Page", "값"]).reset_index(drop=True)
    work["값"] = work["값"].astype(str)

    mask_only_txt = work["값"].apply(is_only_letters_kor)
    mask_zero_hyphen = work["값"].apply(lambda x: hyphen_count(x) == 0)
    mask_one_hyphen = work["값"].apply(lambda x: hyphen_count(x) == 1)
    mask_parentheses = work["값"].apply(has_parentheses)
    mask_digits_only = work["값"].apply(is_digits_only_str)
    drop_mask1 = (
            mask_only_txt
            | mask_zero_hyphen
            | mask_one_hyphen
            | mask_parentheses
            | mask_digits_only
    )
    stage1 = work.loc[~drop_mask1].copy()

    # 2) 하이픈≥2 & 마지막 하이픈 뒤 절이 숫자만 → name_arr 저장 후 삭제
    mask_hy2 = stage1["값"].apply(lambda x: hyphen_count(x) >= 2)
    tail_series = stage1["값"].apply(last_tail)
    mask_tail_digits = tail_series.apply(is_digits_only)
    candidate_name = stage1[mask_hy2 & mask_tail_digits].copy()

    name_arr: Dict[int, str] = {}
    for _, r in candidate_name.iterrows():
        page = int(r["Page"])
        # 페이지 당 최초 도면명만 채택(이미 있으면 건너뜀) — 필요시 최신값으로 덮어쓰기 하려면 조건 지우고 항상 대입
        if page not in name_arr:
            name_arr[page] = r["값"]

    stage2 = stage1.loc[~(mask_hy2 & mask_tail_digits)].copy()

    # 3) 남은 행에 "PID NO(OWNER)" 채우기
    stage2["PID NO(OWNER)"] = stage2.apply(
        lambda row: name_arr.get(int(row["Page"])) if pd.notna(row["Page"]) and int(row["Page"]) in name_arr else "",
        axis=1,
    )

    # 정렬/인덱스 리셋
    stage2 = stage2.sort_values(["Page", "값"]).reset_index(drop=True)
    return stage2, name_arr


# -------------------------
# 엑셀 저장
# -------------------------
def save_df_to_excel(df: pd.DataFrame, path: str, sheet: str = "Sheet1") -> None:
    with pd.ExcelWriter(path, engine="openpyxl") as writer:
        df.to_excel(writer, index=False, sheet_name=sheet)


# -------------------------
# 마크업(페이지별 매칭) 재사용 로직
# -------------------------
def build_page_value_map_from_df(
    df: pd.DataFrame, page_col: str = "Page", value_col: str = "값", ignore_case: bool = True
) -> Dict[int, Set[str]]:
    if df.empty:
        return {}
    out: Dict[int, Set[str]] = {}
    for _, r in df.iterrows():
        try:
            page_no = int(float(str(r[page_col]).strip()))
        except Exception:
            continue
        val = normalize_text(r[value_col], ignore_case=ignore_case)
        if not val:
            continue
        out.setdefault(page_no, set()).add(val)
    return out


def mark_annotation(
    page: fitz.Page,
    annot,
    rgb: Tuple[float, float, float],
    add_overlay: bool,
    opacity: float = 0.25,
):
    try:
        annot.set_colors(stroke=rgb, fill=rgb)
        annot.set_border(width=1.5)
        annot.set_opacity(opacity)
        annot.update()
    except Exception:
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


def process_pdf_mark(
    pdf_path: str,
    out_path: str,
    page_value_map: Dict[int, Set[str]],
    color_hex: str = "#FFCC00",
    ignore_case: bool = True,
    add_overlay: bool = True,
    opacity: float = 0.25,
) -> int:
    rgb = hex_to_rgb01(color_hex)
    doc = fitz.open(pdf_path)
    matched = 0

    for pno in range(doc.page_count):
        page = doc.load_page(pno)
        page_no = pno + 1
        targets = page_value_map.get(page_no)
        if not targets:
            print(f"[INFO] PAGE {page_no}: 매칭 대상 없음, 스킵")
            continue

        annot = page.first_annot
        while annot:
            texts = collect_annot_texts(annot)
            hit = False
            for t in texts:
                if normalize_text(t, ignore_case=ignore_case) in targets:
                    hit = True
                    break
            if hit:
                mark_annotation(page, annot, rgb, add_overlay, opacity)
                matched += 1
            annot = annot.next

    if matched > 0:
        doc.save(out_path)
    doc.close()
    return matched


# -------------------------
# CLI
# -------------------------
def parse_args():
    p = argparse.ArgumentParser(
        description="PDF 주석 → 엑셀 추출/정제/도면명 분리 → 엑셀 저장 → 페이지별 주석 마크업"
    )
    p.add_argument("-p", "--pdf", required=True, help="입력 PDF 경로")
    p.add_argument("-x", "--excel-out", required=True, help="정제 후 엑셀 저장 경로 (xlsx)")
    p.add_argument("-o", "--out", required=True, help="마크업된 PDF 저장 경로")
    p.add_argument("--sheet", default="Sheet1", help="엑셀 시트명 (기본: Sheet1)")
    p.add_argument("--color", default="#FFCC00", help="마크업 색상 HEX (기본: #FFCC00)")
    p.add_argument("--opacity", type=float, default=0.25, help="마크업 불투명도 0.0~1.0 (기본 0.25)")
    p.add_argument("--case-insensitive", action="store_true", help="대소문자 무시 매칭")
    p.add_argument("--add-overlay", action="store_true", help="사각형 오버레이 추가")
    return p.parse_args()


def main():
    args = parse_args()

    print("[STEP1] PDF 주석 텍스트 추출 중 ...")
    df_raw = extract_pdf_annots_to_df(args.pdf)
    print(f"  -> 추출 행 수: {len(df_raw)}")

    # 정제/도면명 분리
    print("[STEP2] 데이터 정제 및 도면명(name_arr) 분리 중 ...")
    df_final, name_arr = refine_df_and_build_name_arr(df_raw)
    print(f"  -> 도면명(page→값) 개수: {len(name_arr)}")
    print(f"  -> 정제 후 남은 행 수: {len(df_final)}")

    # 엑셀 저장 (Page, 값, PID NO(OWNER))
    print(f"[STEP3] 엑셀 저장: {args.excel_out}")
    save_df_to_excel(df_final, args.excel_out, sheet=args.sheet)

    # 페이지별 값 맵 빌드 후 마크업
    print("[STEP4] 마크업용 페이지별 값 맵 빌드 ...")
    page_value_map = build_page_value_map_from_df(
        df_final, page_col="Page", value_col="값", ignore_case=args.case_insensitive
    )

    print("[STEP5] PDF 마크업 실행 ...")
    matched = process_pdf_mark(
        pdf_path=args.pdf,
        out_path=args.out,
        page_value_map=page_value_map,
        color_hex=args.color,
        ignore_case=args.case_insensitive,
        add_overlay=args.add_overlay,
        opacity=args.opacity,
    )
    print(f"[DONE] 매칭된 주석 수: {matched}")
    if matched > 0:
        print(f"[SAVED] 결과 저장: {args.out}")
    else:
        print("[INFO] 매칭이 없어 출력 PDF는 저장되지 않았습니다.")


if __name__ == "__main__":
    main()
