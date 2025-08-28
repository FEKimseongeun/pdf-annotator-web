import os
import time
from uuid import uuid4
# 파일 상단 flask import 구문에 request 추가
from flask import (
    Blueprint, current_app, render_template, request,
    redirect, url_for, send_from_directory, flash
)

from werkzeug.utils import secure_filename

from .services import (
    annotate_pdf_with_excel,           # Full
    annotate_pdf_restricted_with_excel # Restricted
)

bp = Blueprint("main", __name__)

ALLOWED_PDF = {".pdf"}
ALLOWED_XL  = {".xlsx", ".xls"}

def _ext_ok(filename, allow_set):
    _, ext = os.path.splitext(filename.lower())
    return ext in allow_set

# ===== 홈 =====
@bp.route("/", methods=["GET"])
def home():
    return render_template("home.html")

# ===== 라인리스트 메뉴 =====
@bp.route("/linelist", methods=["GET"])
def linelist():
    return render_template("linelist.html")

# ===== Coming soon =====
@bp.route("/instrument/coming-soon", methods=["GET"], endpoint="instrument_coming_soon")
def instrument_coming_soon():
    return render_template("comming_soon.html", feature_name="Instrument")

# ===== Full tag 페이지 =====
@bp.route("/linelist/full", methods=["GET"])
def linelist_full():
    default_colors = {"A": "#FFFF99","B": "#FF9999","C": "#99BFFF","D": "#99FF99"}
    default_opacity = 0.35
    return render_template("linelist_full.html", defaults=default_colors, default_opacity=default_opacity)

# ===== Restricted tag 페이지 =====
@bp.route("/linelist/restricted", methods=["GET"])
def linelist_restricted():
    default_restricted_color = "#FFD54D"
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
        flash("엑셀 파일을 업로드하세요."); return redirect(url_for("main.linelist_full"))
    if not pdf_file or pdf_file.filename == "":
        flash("PDF 파일을 업로드하세요."); return redirect(url_for("main.linelist_full"))
    if not _ext_ok(excel_file.filename, ALLOWED_XL):
        flash("엑셀 파일은 .xlsx 또는 .xls만 지원합니다."); return redirect(url_for("main.linelist_full"))
    if not _ext_ok(pdf_file.filename, ALLOWED_PDF):
        flash("PDF 파일만 지원합니다."); return redirect(url_for("main.linelist_full"))

    job_id = f"{int(time.time())}_{uuid4().hex[:8]}"
    xlsx_name = secure_filename(f"{job_id}_" + excel_file.filename)
    pdf_in_name = secure_filename(f"{job_id}_" + pdf_file.filename)
    xlsx_path = os.path.join(current_app.config["UPLOAD_XLSX_DIR"], xlsx_name)
    pdf_in_path = os.path.join(current_app.config["UPLOAD_PDF_DIR"], pdf_in_name)
    excel_file.save(xlsx_path); pdf_file.save(pdf_in_path)

    # 출력 파일명: 원본이름 + _ann
    orig_pdf_base = os.path.splitext(secure_filename(pdf_file.filename))[0]
    pdf_out_name   = f"{orig_pdf_base}_ann.pdf"
    not_found_name = f"{orig_pdf_base}_ann.xlsx"
    pdf_out_path   = os.path.join(current_app.config["OUTPUT_DIR"], pdf_out_name)
    not_found_path = os.path.join(current_app.config["OUTPUT_DIR"], not_found_name)

    current_app.logger.info(f"[FULL] job={job_id} START xlsx={xlsx_path}, pdf={pdf_in_path} -> out={pdf_out_path}")
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

    current_app.logger.info(f"[FULL] job={job_id} DONE hits={stats.get('hits')} nf={stats.get('not_found_count')} out={pdf_out_path}")

    return render_template(
        "result.html",
        stats=stats,
        output_pdf=pdf_out_name,
        not_found_xlsx=(not_found_name if os.path.exists(not_found_path) and stats.get("not_found_count", 0) > 0 else None)
    )

# ===== Restricted tag 처리 =====
@bp.route("/annotate/restricted", methods=["POST"])
def annotate_restricted():
    ignore_case   = request.form.get("ignore_case") == "on"
    require_order = request.form.get("require_order") == "on"
    opacity       = float(request.form.get("opacity", "0.35"))
    color_hex     = request.form.get("color_restricted", "#FFD54D")  # (자동색 사용으로 실사용 X)

    excel_file = request.files.get("excel_file")
    pdf_file   = request.files.get("pdf_file")
    if not excel_file or excel_file.filename == "":
        flash("엑셀 파일을 업로드하세요."); return redirect(url_for("main.linelist_restricted"))
    if not pdf_file or pdf_file.filename == "":
        flash("PDF 파일을 업로드하세요."); return redirect(url_for("main.linelist_restricted"))
    if not _ext_ok(excel_file.filename, ALLOWED_XL):
        flash("엑셀 파일은 .xlsx 또는 .xls만 지원합니다."); return redirect(url_for("main.linelist_restricted"))
    if not _ext_ok(pdf_file.filename, ALLOWED_PDF):
        flash("PDF 파일만 지원합니다."); return redirect(url_for("main.linelist_restricted"))

    job_id = f"{int(time.time())}_{uuid4().hex[:8]}"
    xlsx_name = secure_filename(f"{job_id}_" + excel_file.filename)
    pdf_in_name = secure_filename(f"{job_id}_" + pdf_file.filename)
    xlsx_path = os.path.join(current_app.config["UPLOAD_XLSX_DIR"], xlsx_name)
    pdf_in_path = os.path.join(current_app.config["UPLOAD_PDF_DIR"], pdf_in_name)
    excel_file.save(xlsx_path); pdf_file.save(pdf_in_path)

    # 출력 파일명: 원본이름 + _ann
    orig_pdf_base = os.path.splitext(secure_filename(pdf_file.filename))[0]
    pdf_out_name   = f"{orig_pdf_base}_ann.pdf"
    not_found_name = f"{orig_pdf_base}_ann.xlsx"
    pdf_out_path   = os.path.join(current_app.config["OUTPUT_DIR"], pdf_out_name)
    not_found_path = os.path.join(current_app.config["OUTPUT_DIR"], not_found_name)

    current_app.logger.info(f"[RES] job={job_id} START xlsx={xlsx_path}, pdf={pdf_in_path} -> out={pdf_out_path}")
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
            clean_terms=False
        )
    except Exception as e:
        current_app.logger.exception(e)
        flash(f"작업 중 오류가 발생했습니다: {e}")
        return redirect(url_for("main.linelist_restricted"))

    current_app.logger.info(
        f"[RES] job={job_id} DONE pages={stats.get('pages')} total_hits={stats.get('hits')} "
        f"sheets={stats.get('sheets')} rows_before={stats.get('rows_before_total')} rows_after={stats.get('rows_after_total')} "
        f"nf_file={stats.get('not_found_file_written')} out={pdf_out_path}"
    )

    return render_template(
        "result.html",
        stats=stats,
        output_pdf=pdf_out_name,
        not_found_xlsx=(not_found_name if stats.get("not_found_file_written") else None)
    )

@bp.route("/download/output/<path:filename>")
def download_output(filename):
    return send_from_directory(current_app.config["OUTPUT_DIR"], filename, as_attachment=True)

@bp.route("/download/upload/<path:filename>")
def download_upload(filename):
    up_root = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
    root = os.path.join(up_root, "uploads")
    return send_from_directory(root, filename, as_attachment=True)

# 파일 맨 아래에 아래 내용 추가
@bp.route('/shutdown')
def shutdown():
    """
    서버를 정상적으로 종료시키되, 실패 시 강제로 종료합니다.
    """
    # 1. 정상 종료 시도
    shutdown_func = request.environ.get('werkzeug.server.shutdown')
    if shutdown_func:
        print("Graceful shutdown initiated.")
        shutdown_func()
        return "서버 종료 중..."

    # 2. 정상 종료 실패 시 (사용자 환경) 강제 종료 실행
    print("Graceful shutdown not available. Forcing process termination.")
    os._exit(0)
