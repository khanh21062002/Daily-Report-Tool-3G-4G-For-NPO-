"""
Script để build Daily Report Tool thành file .exe
Chạy script này để tạo file .exe từ source code
"""

import os
import subprocess
import sys
import shutil


def check_requirements():
    """Kiểm tra các requirement cần thiết"""
    print("Checking requirements...")

    required_packages = [
        'PyQt6', 'pandas', 'matplotlib', 'numpy',
        'openpyxl'
    ]

    missing = []
    for package in required_packages:
        try:
            __import__(package.replace('-', '_'))
            print(f"✅ {package}")
        except ImportError:
            missing.append(package)
            print(f"❌ {package}")

    if missing:
        print(f"\nMissing packages: {', '.join(missing)}")
        print("Please install them using: pip install " + " ".join(missing))
        return False

    return True


def create_spec_file():
    """Tạo file .spec cho PyInstaller với cấu hình tối ưu"""

    spec_content = '''
# -*- mode: python ; coding: utf-8 -*-

import sys
import os
from PyInstaller.utils.hooks import collect_data_files

# Collect data files
data_files = []
data_files += collect_data_files('matplotlib')
data_files += collect_data_files('pandas')

# Add UI file
data_files += [('main_window.ui', '.')]

# Add 4G folder with all processors
data_files += [('4G/DataVisualizationFor4G_V2.py', '4G')]
data_files += [('4G/DataVisualizationVoLTEFor4G.py', '4G')]

# Add 3G folder if exists
if os.path.exists('3G'):
    for root, dirs, files in os.walk('3G'):
        for file in files:
            if file.endswith('.py'):
                data_files += [(os.path.join(root, file), root)]

block_cipher = None

a = Analysis(
    ['DailyReport.py'],
    pathex=[],
    binaries=[],
    datas=data_files,
    hiddenimports=[
        'PyQt6.QtCore',
        'PyQt6.QtWidgets', 
        'PyQt6.QtGui',
        'PyQt6.uic',
        'pandas',
        'matplotlib',
        'matplotlib.backends.backend_agg',
        'matplotlib.figure',
        'numpy',
        'openpyxl',
        'PIL',
        'PIL.Image',
        'PIL.ImageDraw',
        'PIL.ImageFont',
        'datetime',
        'os',
        'sys',
        'math',
        're',
        'warnings'
    ],
    hookspath=[],
    hooksconfig={},
    runtime_hooks=[],
    excludes=[
        'tkinter',
        'test',
        'unittest',
        'pdb',
        'doctest',
        'difflib'
    ],
    win_no_prefer_redirects=False,
    win_private_assemblies=False,
    cipher=block_cipher,
    noarchive=False,
)

pyz = PYZ(a.pure, a.zipped_data, cipher=block_cipher)

exe = EXE(
    pyz,
    a.scripts,
    a.binaries,
    a.zipfiles,
    a.datas,
    [],
    name='DailyReportTool',
    debug=False,
    bootloader_ignore_signals=False,
    strip=False,
    upx=True,
    upx_exclude=[],
    runtime_tmpdir=None,
    console=False,  # False để ẩn console window
    disable_windowed_traceback=False,
    argv_emulation=False,
    target_arch=None,
    codesign_identity=None,
    entitlements_file=None,
    icon='icon.ico' if os.path.exists('icon.ico') else None,
    version='version_info.txt' if os.path.exists('version_info.txt') else None
)
'''

    with open('DailyReportTool.spec', 'w', encoding='utf-8') as f:
        f.write(spec_content)

    print("✅ Created DailyReportTool.spec")


def create_version_info():
    """Tạo file version info cho exe"""

    version_content = '''
# UTF-8
#
# For more details about fixed file info 'ffi' see:
# http://msdn.microsoft.com/en-us/library/ms646997.aspx
VSVersionInfo(
  ffi=FixedFileInfo(
    filevers=(1,0,0,0),
    prodvers=(1,0,0,0),
    mask=0x3f,
    flags=0x0,
    OS=0x40004,
    fileType=0x1,
    subtype=0x0,
    date=(0, 0)
    ),
  kids=[
    StringFileInfo(
      [
      StringTable(
        u'040904B0',
        [StringStruct(u'CompanyName', u'Daily Report Tool'),
        StringStruct(u'FileDescription', u'Daily Report Tool for 3G/4G KPI Analysis'),
        StringStruct(u'FileVersion', u'1.0.0.0'),
        StringStruct(u'InternalName', u'DailyReportTool'),
        StringStruct(u'LegalCopyright', u'Copyright © 2024'),
        StringStruct(u'OriginalFilename', u'DailyReportTool.exe'),
        StringStruct(u'ProductName', u'Daily Report Tool'),
        StringStruct(u'ProductVersion', u'1.0.0.0')])
      ]), 
    VarFileInfo([VarStruct(u'Translation', [1033, 1200])])
  ]
)
'''

    with open('version_info.txt', 'w', encoding='utf-8') as f:
        f.write(version_content)

    print("✅ Created version_info.txt")


def build_exe():
    """Build file .exe"""
    print("\nBuilding executable...")

    # Tạo spec file
    create_spec_file()
    create_version_info()

    # Chạy PyInstaller
    cmd = [
        sys.executable, '-m', 'PyInstaller',
        '--clean',
        '--noconfirm',
        'DailyReportTool.spec'
    ]

    print(f"Running: {' '.join(cmd)}")

    try:
        result = subprocess.run(cmd, check=True, capture_output=True, text=True)
        print("✅ Build successful!")
        print("\nBuild output:")
        print(result.stdout)

        # Kiểm tra file exe đã tạo
        exe_path = os.path.join('dist', 'DailyReportTool.exe')
        if os.path.exists(exe_path):
            size_mb = os.path.getsize(exe_path) / (1024 * 1024)
            print(f"\n🎉 Executable created successfully!")
            print(f"📍 Location: {os.path.abspath(exe_path)}")
            print(f"📊 Size: {size_mb:.1f} MB")

            return True
        else:
            print("❌ Executable not found after build")
            return False

    except subprocess.CalledProcessError as e:
        print("❌ Build failed!")
        print(f"Error: {e}")
        print(f"Output: {e.stdout}")
        print(f"Error output: {e.stderr}")
        return False


def cleanup():
    """Dọn dẹp files tạm"""
    print("\nCleaning up temporary files...")

    temp_dirs = ['build', '__pycache__']
    temp_files = ['DailyReportTool.spec', 'version_info.txt']

    for dir_name in temp_dirs:
        if os.path.exists(dir_name):
            shutil.rmtree(dir_name)
            print(f"🗑️ Removed {dir_name}")

    for file_name in temp_files:
        if os.path.exists(file_name):
            os.remove(file_name)
            print(f"🗑️ Removed {file_name}")


def main():
    """Main function"""
    print("=" * 60)
    print("🚀 DAILY REPORT TOOL - BUILD TO EXE")
    print("=" * 60)

    # Kiểm tra requirements
    if not check_requirements():
        print("\n❌ Please install missing requirements first!")
        return False

    # Kiểm tra files cần thiết
    required_files = ['DailyReport.py', 'main_window.ui']
    missing_files = [f for f in required_files if not os.path.exists(f)]

    if missing_files:
        print(f"\n❌ Missing required files: {', '.join(missing_files)}")
        return False

    # Build exe
    success = build_exe()

    if success:
        print("\n" + "=" * 60)
        print("🎉 BUILD COMPLETED SUCCESSFULLY!")
        print("=" * 60)
        print("📦 Your executable is ready:")
        print(f"   📍 dist/DailyReportTool.exe")
        print("\n📋 Distribution instructions:")
        print("   1. Copy the entire 'dist' folder to target computer")
        print("   2. Run DailyReportTool.exe")
        print("   3. No Python installation required on target computer!")

        # Hỏi có muốn cleanup không
        response = input("\n🗑️ Do you want to cleanup temporary files? (y/n): ")
        if response.lower() in ['y', 'yes']:
            cleanup()

        return True
    else:
        print("\n❌ Build failed! Please check the errors above.")
        return False


if __name__ == "__main__":
    main()