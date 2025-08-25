from app import create_app

app = create_app()

if __name__ == "__main__":
    # 디버그는 개발 중만 켜세요
    app.run(host="127.0.0.1", port=5000, debug=True)