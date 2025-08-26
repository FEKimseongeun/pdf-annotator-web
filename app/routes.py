import os
import time
from uuid import uuid4
from flask import (
    Blueprint, current_app, render_template, request,
    redirect, url_for, send_from_directory, flash
)
from werkzeug.utils import secure_filename
from .services.annotate_service import (
    annotate_pdf_with_excel,           # Full tag
    annotate_pdf_restricted_with_excel # Restricted tag
)

bp = Blueprint("main", __name__)

ALLOWED_PDF = {".pdf"}
ALLOWED_XL  = {".xlsx", ".xls"}

def _ext_ok(filename, allow_set):
    _, ext = os.path.splitext(filename.lower())
    return ext in allow_set

# ===== 대문 (홈) =====
@bp.route("/", methods=["GET"])
def home():
    return render_template("home.html")

# ===== 라인리스트 메뉴 (Full/Restricted 진입점) =====
@bp.route("/linelist", methods=["GET"])
def linelist():
    return render_template("linelist.html")

# --- [NEW] Instrument Coming Soon ---
@bp.route("/instrument/comming-soon", methods=["GET"], endpoint="instrument_coming_soon")
def instrument_coming_soon():
    # 공용 coming_soon.html에 feature_name만 넘겨줌
    return render_template("instrument_tag.html", feature_name="Instrument")


# ===== Full tag: 기존 기능 페이지 =====
@bp.route("/linelist/full", methods=["GET"])
def linelist_full():
    default_colors = {
        "A": "#FFFF99",
        "B": "#FF9999",
        "C": "#99BFFF",
        "D": "#99FF99",
    }
    default_opacity = 0.35
    return render_template("linelist_full.html", defaults=default_colors, default_opacity=default_opacity)

# ===== Restricted tag: A/B/C가 같은 '한 줄'에 모두 포함되면 라인 전체 하이라이트 =====
@bp.route("/linelist/restricted", methods=["GET"])
def linelist_restricted():
    default_restricted_color = "#FFD54D"  # 연한 앰버
    default_opacity = 0.35
    return render_template("linelist_restricted.html", default_color=default_restricted_color, default_opacity=default_opacity)

# ===== Full tag 처리 =====
@bp.route("/annotate/full", methods=["POST"])
def annotate_full():
    ignore_case = request.form.get("ignore_case") == "on"
    whole_word  = request.form.get("whole_word") == "on"
    opacity     = float(request.form.get("opacity", "0.35"))

    color_hex = {
        "A": request.form.get("color_A", "#FFFF99"),
        "B": request.form.get("color_B", "#FF9999"),
        "C": request.form.get("color_C", "#99BFFF"),
        "D": request.form.get("color_D", "#99FF99"),
    }

    excel_file = request.files.get("excel_file")
    pdf_file   = request.files.get("pdf_file")

    if not excel_file or excel_file.filename == "":
        flash("엑셀 파일을 업로드하세요.")
        return redirect(url_for("main.linelist_full"))
    if not pdf_file or pdf_file.filename == "":
        flash("PDF 파일을 업로드하세요.")
        return redirect(url_for("main.linelist_full"))

    if not _ext_ok(excel_file.filename, ALLOWED_XL):
        flash("엑셀 파일은 .xlsx 또는 .xls만 지원합니다.")
        return redirect(url_for("main.linelist_full"))
    if not _ext_ok(pdf_file.filename, ALLOWED_PDF):
        flash("PDF 파일만 지원합니다.")
        return redirect(url_for("main.linelist_full"))

    job_id = f"{int(time.time())}_{uuid4().hex[:8]}"
    xlsx_name = secure_filename(f"{job_id}_" + excel_file.filename)
    pdf_in_name = secure_filename(f"{job_id}_" + pdf_file.filename)
    xlsx_path = os.path.join(current_app.config["UPLOAD_XLSX_DIR"], xlsx_name)
    pdf_in_path = os.path.join(current_app.config["UPLOAD_PDF_DIR"], pdf_in_name)
    excel_file.save(xlsx_path)
    pdf_file.save(pdf_in_path)

    base_name, _ = os.path.splitext(pdf_in_name)
    pdf_out_name = base_name + "_annotated_full.pdf"
    not_found_name = f"{base_name}_not_found_full.xlsx"

    pdf_out_path = os.path.join(current_app.config["OUTPUT_DIR"], pdf_out_name)
    not_found_path = os.path.join(current_app.config["OUTPUT_DIR"], not_found_name)

    try:
        stats = annotate_pdf_with_excel(
            excel_path=xlsx_path,
            pdf_input_path=pdf_in_path,
            pdf_output_path=pdf_out_path,
            not_found_xlsx_path=not_found_path,
            color_hex_map=color_hex,
            opacity=opacity,
            ignore_case=ignore_case,
            whole_word=whole_word,
            clean_terms=False
        )
    except Exception as e:
        current_app.logger.exception(e)
        flash(f"작업 중 오류가 발생했습니다: {e}")
        return redirect(url_for("main.linelist_full"))

    return render_template(
        "result.html",
        stats=stats,
        output_pdf=pdf_out_name,
        not_found_xlsx=(not_found_name if os.path.exists(not_found_path) and stats.get("not_found_count", 0) > 0 else None)
    )

# ===== Restricted tag 처리 =====
@bp.route("/annotate/restricted", methods=["POST"])
def annotate_restricted():
    # 옵션
    ignore_case = request.form.get("ignore_case") == "on"
    require_order = request.form.get("require_order") == "on"  # A→B→C 순서 강제 여부
    opacity     = float(request.form.get("opacity", "0.35"))
    color_hex   = request.form.get("color_restricted", "#FFD54D")

    excel_file = request.files.get("excel_file")
    pdf_file   = request.files.get("pdf_file")

    if not excel_file or excel_file.filename == "":
        flash("엑셀 파일을 업로드하세요.")
        return redirect(url_for("main.linelist_restricted"))
    if not pdf_file or pdf_file.filename == "":
        flash("PDF 파일을 업로드하세요.")
        return redirect(url_for("main.linelist_restricted"))

    if not _ext_ok(excel_file.filename, ALLOWED_XL):
        flash("엑셀 파일은 .xlsx 또는 .xls만 지원합니다.")
        return redirect(url_for("main.linelist_restricted"))
    if not _ext_ok(pdf_file.filename, ALLOWED_PDF):
        flash("PDF 파일만 지원합니다.")
        return redirect(url_for("main.linelist_restricted"))

    job_id = f"{int(time.time())}_{uuid4().hex[:8]}"
    xlsx_name = secure_filename(f"{job_id}_" + excel_file.filename)
    pdf_in_name = secure_filename(f"{job_id}_" + pdf_file.filename)
    xlsx_path = os.path.join(current_app.config["UPLOAD_XLSX_DIR"], xlsx_name)
    pdf_in_path = os.path.join(current_app.config["UPLOAD_PDF_DIR"], pdf_in_name)
    excel_file.save(xlsx_path)
    pdf_file.save(pdf_in_path)

    base_name, _ = os.path.splitext(pdf_in_name)
    pdf_out_name = base_name + "_annotated_restricted.pdf"
    not_found_name = f"{base_name}_not_found_restricted.xlsx"

    pdf_out_path = os.path.join(current_app.config["OUTPUT_DIR"], pdf_out_name)
    not_found_path = os.path.join(current_app.config["OUTPUT_DIR"], not_found_name)

    try:
        stats = annotate_pdf_restricted_with_excel(
            excel_path=xlsx_path,
            pdf_input_path=pdf_in_path,
            pdf_output_path=pdf_out_path,
            not_found_xlsx_path=not_found_path,
            color_hex=color_hex,
            opacity=opacity,
            ignore_case=ignore_case,
            require_order=require_order,
            clean_terms=False  # 요청 반영: 전처리 없이 원문 검색
        )
    except Exception as e:
        current_app.logger.exception(e)
        flash(f"작업 중 오류가 발생했습니다: {e}")
        return redirect(url_for("main.linelist_restricted"))

    return render_template(
        "result.html",
        stats=stats,
        output_pdf=pdf_out_name,
        not_found_xlsx=(not_found_name if os.path.exists(not_found_path) and stats.get("not_found_count", 0) > 0 else None)
    )

@bp.route("/download/output/<path:filename>")
def download_output(filename):
    return send_from_directory(current_app.config["OUTPUT_DIR"], filename, as_attachment=True)

@bp.route("/download/upload/<path:filename>")
def download_upload(filename):
    up_root = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
    root = os.path.join(up_root, "uploads")
    return send_from_directory(root, filename, as_attachment=True)
