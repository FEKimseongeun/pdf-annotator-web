# app/services/full_service.py
import os
import logging
from typing import Dict
import pandas as pd
import fitz  # PyMuPDF
from flashtext import KeywordProcessor

logger = logging.getLogger(__name__)

COLOR_PALETTE = [
    (1, 1, 0),
    (0, 1, 0),
    (0, 1, 1),
    (1, 0, 1),
    (1, 0.5, 0),
    (0.5, 0.5, 1),
    (0.8, 0.8, 0.8)
]


def annotate_pdf_with_excel(
        excel_path: str,
        pdf_input_path: str,
        pdf_output_path: str,
        not_found_xlsx_path: str,
        opacity: float = 0.35,
        **kwargs
) -> Dict:
    logger.info("=" * 60)
    logger.info("Full tag 처리 시작")
    logger.info(f"엑셀: {os.path.basename(excel_path)}")
    logger.info(f"PDF: {os.path.basename(pdf_input_path)}")
    logger.info("=" * 60)

    if not os.path.exists(excel_path):
        raise FileNotFoundError(f"엑셀 파일 없음: {excel_path}")
    if not os.path.exists(pdf_input_path):
        raise FileNotFoundError(f"PDF 파일 없음: {pdf_input_path}")

    logger.info("📊 엑셀 파일 로딩 중...")
    df = pd.read_excel(excel_path)
    logger.info(f"   - 컬럼 수: {len(df.columns)}")
    logger.info(f"   - 컬럼 목록: {list(df.columns)}")

    keyword_processor = KeywordProcessor(case_sensitive=False)
    keyword_metadata = {}
    all_keywords_map = {}
    total_keywords = 0

    logger.info("🔍 검색 엔진 구축 중...")
    for idx, col_header in enumerate(df.columns):
        color = COLOR_PALETTE[idx % len(COLOR_PALETTE)]
        col_keyword_count = 0

        for keyword in df[col_header].dropna():
            word_str = str(keyword).strip()
            if not word_str:
                continue

            keyword_processor.add_keyword(word_str)
            word_lower = word_str.lower()

            keyword_metadata[word_lower] = {
                "header": str(col_header),
                "color": color,
                "original_word": word_str
            }

            all_keywords_map[word_lower] = {
                'word': word_str,
                'header': str(col_header)
            }

            total_keywords += 1
            col_keyword_count += 1

        logger.info(f"   - {col_header}: {col_keyword_count}개 키워드")

    if not all_keywords_map:
        raise ValueError("엑셀에서 검색할 텍스트가 없습니다.")

    logger.info(f"✅ 총 {total_keywords}개 키워드 등록 완료")

    logger.info("📄 PDF 파일 열기 중...")
    found_keywords = set()
    total_hits = 0
    failed_pages = []

    try:
        doc = fitz.open(pdf_input_path)

        if doc.is_repaired:
            logger.warning("⚠️  PDF가 손상되어 자동 복구되었습니다.")

        num_pages = len(doc)
        logger.info(f"   - 총 페이지 수: {num_pages}")
        logger.info("=" * 60)

        for page_num in range(num_pages):
            logger.info(f"📖 페이지 {page_num + 1}/{num_pages} 처리 중...")

            try:
                page = doc.load_page(page_num)

                try:
                    text_on_page = page.get_text("text")
                    text_length = len(text_on_page)
                    logger.info(f"   - 텍스트 추출: {text_length}자")
                except Exception as e:
                    logger.error(f"❌ 페이지 {page_num + 1} 텍스트 추출 실패: {e}")
                    failed_pages.append(page_num + 1)
                    continue

                keywords_on_page = keyword_processor.extract_keywords(text_on_page)
                unique_keywords_on_page = set(keywords_on_page)

                if not unique_keywords_on_page:
                    logger.info(f"   - 발견된 키워드: 0개")
                    continue

                logger.info(f"   - 발견된 키워드: {len(unique_keywords_on_page)}개")
                found_keywords.update(k.lower() for k in unique_keywords_on_page)
                page_hits = 0

                for keyword in unique_keywords_on_page:
                    keyword_lower = keyword.lower()

                    try:
                        quads = page.search_for(keyword)
                        quad_count = len(quads)

                        if quad_count > 0:
                            logger.debug(f"      · '{keyword}': {quad_count}개 위치 발견")
                    except Exception as e:
                        logger.error(f"❌ 키워드 '{keyword}' 검색 실패: {e}")
                        continue

                    meta = keyword_metadata.get(keyword_lower)
                    if not meta:
                        continue

                    annot_title = meta['header']
                    annot_color = meta['color']

                    for quad in quads:
                        try:
                            annot = page.add_highlight_annot(quad)
                            annot.set_colors(stroke=annot_color)
                            annot.set_opacity(opacity)
                            annot.set_info(content=keyword, title=annot_title)
                            annot.update()
                            total_hits += 1
                            page_hits += 1
                        except Exception as e:
                            logger.error(f"❌ 하이라이트 추가 실패: {e}")
                            continue

                logger.info(f"   ✅ 페이지 {page_num + 1} 완료: {page_hits}개 하이라이트 추가")

            except Exception as e:
                logger.error(f"❌ 페이지 {page_num + 1} 처리 실패: {e}")
                failed_pages.append(page_num + 1)
                continue

        logger.info("=" * 60)
        logger.info("💾 PDF 저장 중...")

        try:
            doc.save(pdf_output_path)
            logger.info(f"✅ PDF 저장 완료: {os.path.basename(pdf_output_path)}")
        except Exception as e:
            logger.warning(f"⚠️  압축 저장 실패, 일반 저장 시도: {e}")
            doc.save(pdf_output_path)
            logger.info(f"✅ PDF 저장 완료 (일반 모드)")

        doc.close()

    except Exception as e:
        logger.error(f"❌ PDF 처리 중 치명적 오류 발생: {e}", exc_info=True)
        raise RuntimeError(f"PDF 처리 중 오류 발생: {e}")

    logger.info("=" * 60)
    logger.info("📊 결과 집계 중...")

    all_keys = set(all_keywords_map.keys())
    missing_keys = all_keys - found_keywords
    not_found_count = len(missing_keys)

    logger.info(f"   - 전체 키워드: {total_keywords}개")
    logger.info(f"   - 발견된 키워드: {len(found_keywords)}개")
    logger.info(f"   - 미발견 키워드: {not_found_count}개")
    logger.info(f"   - 총 하이라이트: {total_hits}개")

    if failed_pages:
        logger.warning(f"⚠️  처리 실패 페이지: {failed_pages}")

    if missing_keys:
        logger.info("📝 미발견 키워드 엑셀 생성 중...")
        missing_data_list = []
        for key in missing_keys:
            info = all_keywords_map[key]
            missing_data_list.append({
                'Header': info['header'],
                'Keyword': info['word'],
                'Status': 'Not Found'
            })

        missing_df = pd.DataFrame(missing_data_list).sort_values(by=['Header', 'Keyword'])
        missing_df.to_excel(not_found_xlsx_path, index=False)
        logger.info(f"✅ 미발견 목록 저장: {os.path.basename(not_found_xlsx_path)}")
    else:
        logger.info("🎉 모든 키워드가 발견되었습니다!")

    logger.info("=" * 60)
    logger.info("✅ Full tag 처리 완료")
    logger.info("=" * 60)

    return {
        "pages": num_pages,
        "terms": total_keywords,
        "hits": total_hits,
        "not_found_count": not_found_count,
        "failed_count": len(failed_pages),
    }