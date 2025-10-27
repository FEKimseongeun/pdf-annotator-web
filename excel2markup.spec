# -*- mode: python ; coding: utf-8 -*-

block_cipher = None

# ✅ 포함할 데이터 파일들 (템플릿, 스태틱 등)
datas = [
    ('app/templates', 'app/templates'),
    ('app/static', 'app/static'),
]

# ✅ Hidden imports (PyInstaller가 자동으로 찾지 못하는 모듈들)
hiddenimports = [
    'flask',
    'werkzeug',
    'jinja2',
    'pandas',
    'openpyxl',
    'xlrd',
    'fitz',  # PyMuPDF
    'concurrent.futures',
    'multiprocessing',
    'PIL',
    'app',
    'app.routes',
    'app.services',
    'app.services.common',
    'app.services.full_service',
    'app.services.restricted_service',
]

a = Analysis(
    ['launcher_local.py'],
    pathex=[],
    binaries=[],
    datas=datas,
    hiddenimports=hiddenimports,
    hookspath=[],
    hooksconfig={},
    runtime_hooks=[],
    excludes=[
        'matplotlib',
        'tkinter',
        'numpy.tests',
    ],
    win_no_prefer_redirects=False,
    win_private_assemblies=False,
    cipher=block_cipher,
    noarchive=False,
)

pyz = PYZ(
    a.pure,
    a.zipped_data,
    cipher=block_cipher
)

exe = EXE(
    pyz,
    a.scripts,
    a.binaries,
    a.zipfiles,
    a.datas,
    [],
    name='Excel2Markup',
    debug=False,
    bootloader_ignore_signals=False,
    strip=False,
    upx=True,
    upx_exclude=[],
    runtime_tmpdir=None,
    console=True,
    disable_windowed_traceback=False,
    argv_emulation=False,
    target_arch=None,
    codesign_identity=None,
    entitlements_file=None,
    # ✅ icon 라인 제거 또는 None으로 설정
)