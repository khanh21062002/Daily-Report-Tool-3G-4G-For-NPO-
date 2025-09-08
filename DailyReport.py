from PyQt6.QtWidgets import (
    QApplication, QMainWindow, QMessageBox, QFileDialog,
    QWidget, QVBoxLayout, QHBoxLayout, QLabel, QPushButton
)
from PyQt6.QtCore import Qt, QThread, pyqtSignal
from PyQt6 import uic
import sys
import os


class ReportWorkerThread(QThread):
    """Worker thread để xử lý báo cáo không block UI"""
    progress_update = pyqtSignal(str)
    finished_report = pyqtSignal(bool, str)

    def __init__(self, processor_class, file_paths, name, creator, region_name):
        super().__init__()
        self.processor_class = processor_class
        self.file_paths = file_paths
        self.name = name
        self.creator = creator
        self.region_name = region_name

    def run(self):
        try:
            self.progress_update.emit("Đang khởi tạo processor...")
            processor = self.processor_class()

            # Tạo thư mục output riêng cho từng region
            output_dir = f"output_{self.region_name.replace(' ', '_').lower()}"
            os.makedirs(output_dir, exist_ok=True)

            processed_files = []
            chart_paths = []

            # Xử lý từng file
            for i, file_path in enumerate(self.file_paths):
                self.progress_update.emit(
                    f"Đang xử lý file {i + 1}/{len(self.file_paths)}: {os.path.basename(file_path)}")

                base_name = os.path.splitext(os.path.basename(file_path))[0]
                csv_file = os.path.join(output_dir, f"{base_name}_clean.csv")

                # Chuyển đổi Excel sang CSV
                df = processor.clean_excel_to_csv(file_path, csv_file)

                if df is not None:
                    processed_files.append(csv_file)
                else:
                    self.finished_report.emit(False, f"Lỗi khi xử lý file: {os.path.basename(file_path)}")
                    return

            if len(processed_files) >= 2:
                # Nếu có ít nhất 2 file, sử dụng 2 file đầu cho All Day và Busy Hour
                self.progress_update.emit("Đang tạo biểu đồ và dashboard...")
                chart_paths = processor.create_charts_from_csv(
                    processed_files[0], processed_files[1], output_dir
                )
            elif len(processed_files) == 1:
                # Nếu chỉ có 1 file, sử dụng file đó cho cả hai
                import shutil
                csv_copy = processed_files[0].replace('_clean.csv', '_BH_clean.csv')
                shutil.copy2(processed_files[0], csv_copy)
                processed_files.append(csv_copy)

                self.progress_update.emit("Đang tạo biểu đồ và dashboard...")
                chart_paths = processor.create_charts_from_csv(
                    processed_files[0], processed_files[1], output_dir
                )

            # Tạo báo cáo tổng hợp
            if len(processed_files) >= 2:
                self.progress_update.emit("Đang tạo báo cáo tổng hợp...")
                processor.create_summary_table(processed_files[0], processed_files[1], output_dir)

            success_msg = f"""Tạo báo cáo {self.name} thành công!

Thông tin báo cáo:
• Loại: {self.name}
• Người tạo: {self.creator}
• Khu vực: {self.region_name}
• Số file xử lý: {len(self.file_paths)}
• Files đã xử lý: {', '.join([os.path.basename(f) for f in self.file_paths])}
• Thư mục kết quả: {output_dir}
• Số biểu đồ tạo: {len(chart_paths) if chart_paths else 0}

Kết quả đã được lưu vào thư mục: {output_dir}"""

            self.finished_report.emit(True, success_msg)

        except Exception as e:
            self.finished_report.emit(False, f"Lỗi khi tạo báo cáo: {str(e)}")


class FileItemWidget(QWidget):
    """Widget hiển thị thông tin một file với nút xóa"""
    remove_requested = pyqtSignal(str)

    def __init__(self, file_path):
        super().__init__()
        self.file_path = file_path
        self.setup_ui()

    def setup_ui(self):
        layout = QHBoxLayout()
        layout.setContentsMargins(5, 5, 5, 5)

        # Label hiển thị tên file
        self.file_label = QLabel(os.path.basename(self.file_path))
        self.file_label.setStyleSheet("""
            QLabel {
                color: #ffffff;
                font-size: 12px;
                padding: 5px 10px;
                background-color: #4a90e2;
                border: 1px solid #4a90e2;
                border-radius: 4px;
                min-height: 25px;
            }
        """)
        self.file_label.setToolTip(self.file_path)

        # Nút xóa
        btn_remove = QPushButton("✕")
        btn_remove.setFixedSize(30, 30)
        btn_remove.setStyleSheet("""
            QPushButton {
                background-color: #dc3545;
                color: white;
                border: none;
                border-radius: 15px;
                font-weight: bold;
                font-size: 14px;
            }
            QPushButton:hover {
                background-color: #c82333;
            }
        """)
        btn_remove.clicked.connect(lambda: self.remove_requested.emit(self.file_path))

        layout.addWidget(self.file_label)
        layout.addWidget(btn_remove)

        self.setLayout(layout)


class MainWindow(QMainWindow):
    def __init__(self):
        super().__init__()

        # Load UI file
        try:
            uic.loadUi('main_window.ui', self)
        except FileNotFoundError:
            QMessageBox.critical(None, "Lỗi", "Không tìm thấy file main_window.ui!")
            sys.exit(1)

        # Import module 4G
        self.import_4g_module()

        # Biến lưu trữ file paths (giờ là list thay vì single path)
        self.file_paths = {
            'bac': [],
            'trung': [],
            'nam': []
        }

        # Thiết lập giao diện và kết nối signals
        self.setup_ui()
        self.connect_signals()

        # Worker thread
        self.worker_thread = None

    def import_4g_module(self):
        """Import module 4G"""
        try:
            current_dir = os.path.dirname(os.path.abspath(__file__))
            path_4g = os.path.join(current_dir, '4G')
            if path_4g not in sys.path:
                sys.path.insert(0, path_4g)

            from DataVisualizationFor4G_V2 import ExcelCSVProcessor
            self.ExcelCSVProcessor = ExcelCSVProcessor
            self.import_4g_success = True
            print("✅ Import DataVisualizationFor4G_V2 thành công!")

        except ImportError as e:
            print(f"❌ Lỗi import DataVisualizationFor4G_V2: {e}")
            self.import_4g_success = False
            self.ExcelCSVProcessor = None

    def setup_ui(self):
        """Thiết lập giao diện"""
        self.setWindowTitle("Ứng dụng báo cáo hàng ngày - PyQt6 (Multi-File)")
        self.setGeometry(100, 100, 1200, 800)

        # Set main window background
        self.setStyleSheet("""
            QMainWindow {
                background-color: #2b2b2b;
                color: #ffffff;
            }
        """)

        # Thiết lập style cho navigation panel
        self.navWidget.setStyleSheet("""
            QWidget {
                background-color: #3c3c3c;
                border-right: 2px solid #555555;
            }
            QLabel {
                color: #ffffff;
                font-weight: bold;
                font-size: 16px;
                padding: 15px;
                background-color: #3c3c3c;
            }
        """)

        # Style cho navigation buttons
        nav_button_style = """
            QPushButton {
                background-color: #4a90e2;
                color: white;
                border: none;
                padding: 12px 20px;
                font-size: 14px;
                font-weight: bold;
                margin: 8px;
                border-radius: 8px;
                min-height: 40px;
            }
            QPushButton:hover {
                background-color: #357abd;
                transform: translateY(-2px);
            }
            QPushButton:pressed {
                background-color: #2968a3;
            }
        """

        self.btnMienBac.setStyleSheet(nav_button_style)
        self.btnMienTrung.setStyleSheet(nav_button_style)
        self.btnMienNam.setStyleSheet(nav_button_style)

        # Style cho titles
        title_base_style = """
            font-size: 32px; 
            font-weight: bold; 
            margin: 30px;
            padding: 20px;
            border-radius: 10px;
            background-color: #404040;
            border: 2px solid {};
        """

        self.lblTitleBac.setStyleSheet(title_base_style.format("#4a90e2") + "color: #4a90e2;")
        self.lblTitleTrung.setStyleSheet(title_base_style.format("#ff9f43") + "color: #ff9f43;")
        self.lblTitleNam.setStyleSheet(title_base_style.format("#00d2d3") + "color: #00d2d3;")

        # Style cho form containers
        form_style = """
            QWidget {
                background-color: #404040;
                border: 2px solid #606060;
                border-radius: 15px;
                padding: 30px;
                margin: 20px;
            }
            QLabel {
                color: #ffffff;
                font-size: 14px;
                font-weight: bold;
                padding: 8px 12px;
                min-width: 120px;
            }
        """

        self.formWidgetBac.setStyleSheet(form_style)
        self.formWidgetTrung.setStyleSheet(form_style)
        self.formWidgetNam.setStyleSheet(form_style)

        # Style cho input fields
        input_style = """
            QLineEdit {
                border: 2px solid #606060;
                border-radius: 8px;
                padding: 12px 16px;
                font-size: 14px;
                background-color: #505050;
                color: #ffffff;
                margin: 5px 0;
                min-height: 20px;
            }
            QLineEdit:focus {
                border-color: #4a90e2;
                background-color: #454545;
            }
            QLineEdit:hover {
                border-color: #707070;
            }
        """

        for widget in [self.lineEditCreatorBac,
                       self.lineEditCreatorTrung,
                       self.lineEditCreatorNam]:
            widget.setStyleSheet(input_style)

        # ComboBoxes style
        combo_style = """
            QComboBox {
                border: 2px solid #606060;
                border-radius: 8px;
                padding: 8px 12px;
                font-size: 14px;
                background-color: #505050;
                color: #ffffff;
                margin: 5px 0;
                min-height: 20px;
            }
            QComboBox:focus { border-color: #4a90e2; background-color: #454545; }
            QComboBox:hover { border-color: #707070; }
        """

        for widget in [self.lineEditNameBac,
                       self.lineEditNameTrung,
                       self.lineEditNameNam]:
            widget.setStyleSheet(combo_style)

        # Style cho file buttons
        file_btn_style = """
            QPushButton {
                background-color: #4a90e2;
                color: white;
                border: none;
                padding: 8px 16px;
                border-radius: 6px;
                font-weight: bold;
                font-size: 12px;
                margin: 2px;
                min-height: 30px;
            }
            QPushButton:hover {
                background-color: #357abd;
            }
            QPushButton:pressed {
                background-color: #2968a3;
            }
        """

        # Apply file button styles
        for widget in [self.btnAddFileBac, self.btnClearFilesBac,
                       self.btnAddFileTrung, self.btnClearFilesTrung,
                       self.btnAddFileNam, self.btnClearFilesNam]:
            widget.setStyleSheet(file_btn_style)

        # Style cho clear button (màu đỏ)
        clear_btn_style = """
            QPushButton {
                background-color: #dc3545;
                color: white;
                border: none;
                padding: 8px 16px;
                border-radius: 6px;
                font-weight: bold;
                font-size: 12px;
                margin: 2px;
                min-height: 30px;
            }
            QPushButton:hover {
                background-color: #c82333;
            }
            QPushButton:pressed {
                background-color: #bd2130;
            }
        """

        for widget in [self.btnClearFilesBac, self.btnClearFilesTrung, self.btnClearFilesNam]:
            widget.setStyleSheet(clear_btn_style)

        # Style cho scroll areas
        scroll_style = """
            QScrollArea {
                background-color: #505050;
                border: 2px solid #606060;
                border-radius: 8px;
                min-height: 120px;
            }
            QScrollArea QWidget {
                background-color: #505050;
            }
        """

        for widget in [self.scrollFilesBac, self.scrollFilesTrung, self.scrollFilesNam]:
            widget.setStyleSheet(scroll_style)

        # Style cho empty labels
        empty_label_style = """
            QLabel {
                color: #cccccc;
                font-style: italic;
                font-size: 12px;
                padding: 20px;
                text-align: center;
            }
        """

        for widget in [self.lblEmptyBac, self.lblEmptyTrung, self.lblEmptyNam]:
            widget.setStyleSheet(empty_label_style)

        # Style cho create buttons
        create_btn_base = """
            QPushButton {{
                background-color: {};
                color: white;
                border: none;
                padding: 15px 40px;
                font-size: 16px;
                font-weight: bold;
                border-radius: 10px;
                margin: 30px;
                min-width: 150px;
                min-height: 50px;
            }}
            QPushButton:hover {{
                background-color: {};
                transform: translateY(-3px);
                box-shadow: 0px 5px 15px rgba(0,0,0,0.3);
            }}
            QPushButton:pressed {{
                background-color: {};
                transform: translateY(-1px);
            }}
        """

        self.btnCreateBac.setStyleSheet(create_btn_base.format("#4a90e2", "#357abd", "#2968a3"))
        self.btnCreateTrung.setStyleSheet(create_btn_base.format("#ff9f43", "#e8890b", "#d17d00"))
        self.btnCreateNam.setStyleSheet(create_btn_base.format("#00d2d3", "#00a8a9", "#008384"))

        # Set main widget backgrounds
        self.centralwidget.setStyleSheet("QWidget { background-color: #2b2b2b; }")
        self.stackedWidget.setStyleSheet(
            "QStackedWidget { background-color: #2b2b2b; } QWidget { background-color: #2b2b2b; }")

        # Đặt trang mặc định
        self.stackedWidget.setCurrentIndex(0)

    def connect_signals(self):
        """Kết nối signals với slots"""
        # Navigation buttons
        self.btnMienBac.clicked.connect(lambda: self.change_page(0))
        self.btnMienTrung.clicked.connect(lambda: self.change_page(1))
        self.btnMienNam.clicked.connect(lambda: self.change_page(2))

        # File management buttons
        self.btnAddFileBac.clicked.connect(lambda: self.add_files('bac'))
        self.btnAddFileTrung.clicked.connect(lambda: self.add_files('trung'))
        self.btnAddFileNam.clicked.connect(lambda: self.add_files('nam'))

        self.btnClearFilesBac.clicked.connect(lambda: self.clear_files('bac'))
        self.btnClearFilesTrung.clicked.connect(lambda: self.clear_files('trung'))
        self.btnClearFilesNam.clicked.connect(lambda: self.clear_files('nam'))

        # Create report buttons
        self.btnCreateBac.clicked.connect(lambda: self.create_report('bac'))
        self.btnCreateTrung.clicked.connect(lambda: self.create_report('trung'))
        self.btnCreateNam.clicked.connect(lambda: self.create_report('nam'))

    def change_page(self, index):
        """Chuyển đổi trang trong QStackedWidget"""
        self.stackedWidget.setCurrentIndex(index)

        # Update navigation button styles
        buttons = [self.btnMienBac, self.btnMienTrung, self.btnMienNam]
        active_style = """
            QPushButton {
                background-color: #ffffff;
                color: #2b2b2b;
                border: 2px solid #4a90e2;
                padding: 12px 20px;
                font-size: 14px;
                font-weight: bold;
                margin: 8px;
                border-radius: 8px;
                min-height: 40px;
            }
            QPushButton:hover {
                background-color: #f0f0f0;
            }
        """

        inactive_style = """
            QPushButton {
                background-color: #4a90e2;
                color: white;
                border: none;
                padding: 12px 20px;
                font-size: 14px;
                font-weight: bold;
                margin: 8px;
                border-radius: 8px;
                min-height: 40px;
            }
            QPushButton:hover {
                background-color: #357abd;
            }
            QPushButton:pressed {
                background-color: #2968a3;
            }
        """

        for i, btn in enumerate(buttons):
            if i == index:
                btn.setStyleSheet(active_style)
            else:
                btn.setStyleSheet(inactive_style)

    def add_files(self, region):
        """Thêm nhiều file vào danh sách"""
        file_dialog = QFileDialog()
        file_dialog.setStyleSheet("""
            QFileDialog {
                background-color: #404040;
                color: #ffffff;
            }
        """)

        file_paths, _ = file_dialog.getOpenFileNames(
            self,
            f"Chọn file dữ liệu cho miền {region}",
            "",
            "Excel Files (*.xlsx *.xls);;CSV Files (*.csv);;All Files (*)"
        )

        if file_paths:
            # Thêm file paths vào danh sách (tránh trùng lặp)
            for file_path in file_paths:
                if file_path not in self.file_paths[region]:
                    self.file_paths[region].append(file_path)

            # Cập nhật hiển thị danh sách file
            self.update_file_list(region)

            # Thông báo thành công
            self.show_info_message(f"Đã thêm {len(file_paths)} file(s) cho {region}!")

    def clear_files(self, region):
        """Xóa tất cả file trong danh sách"""
        if not self.file_paths[region]:
            self.show_warning_message("Không có file nào để xóa!")
            return

        reply = QMessageBox.question(
            self, "Xác nhận",
            f"Bạn có chắc muốn xóa tất cả {len(self.file_paths[region])} file(s)?",
            QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No
        )

        if reply == QMessageBox.StandardButton.Yes:
            self.file_paths[region] = []
            self.update_file_list(region)
            self.show_info_message("Đã xóa tất cả file!")

    def remove_file(self, region, file_path):
        """Xóa một file khỏi danh sách"""
        if file_path in self.file_paths[region]:
            self.file_paths[region].remove(file_path)
            self.update_file_list(region)

    def update_file_list(self, region):
        """Cập nhật hiển thị danh sách file"""
        # Xác định layout và empty label tương ứng
        if region == 'bac':
            layout = self.fileListBac
            empty_label = self.lblEmptyBac
        elif region == 'trung':
            layout = self.fileListTrung
            empty_label = self.lblEmptyTrung
        else:  # nam
            layout = self.fileListNam
            empty_label = self.lblEmptyNam

        # Xóa tất cả widget hiện tại
        for i in reversed(range(layout.count())):
            child = layout.itemAt(i).widget()
            if child:
                child.setParent(None)

        files = self.file_paths[region]

        if not files:
            # Hiển thị empty message
            empty_label.setText("Chưa có file nào được chọn")
            layout.addWidget(empty_label)
            layout.addStretch()
        else:
            # Hiển thị danh sách file
            for file_path in files:
                file_widget = FileItemWidget(file_path)
                file_widget.remove_requested.connect(
                    lambda path, r=region: self.remove_file(r, path)
                )
                layout.addWidget(file_widget)

            layout.addStretch()

    def create_report(self, region):
        """Xử lý tạo báo cáo"""
        # Lấy thông tin từ form dựa vào region
        if region == 'bac':
            name = self.lineEditNameBac.currentText().strip()
            creator = self.lineEditCreatorBac.text().strip()
            region_name = "Miền Bắc"
        elif region == 'trung':
            name = self.lineEditNameTrung.currentText().strip()
            creator = self.lineEditCreatorTrung.text().strip()
            region_name = "Miền Trung"
        else:  # nam
            name = self.lineEditNameNam.currentText().strip()
            creator = self.lineEditCreatorNam.text().strip()
            region_name = "Miền Nam"

        # Validation
        if not name:
            self.show_warning_message("Vui lòng chọn loại báo cáo!")
            return

        if not creator:
            self.show_warning_message("Vui lòng nhập tên người tạo!")
            return

        if not self.file_paths.get(region):
            self.show_warning_message("Vui lòng chọn ít nhất một file dữ liệu!")
            return

        # Gọi hàm xử lý tương ứng
        if name == "4G":
            self.report_4G_multi_files(name, creator, region_name, self.file_paths[region])
        elif name == "3G":
            self.show_info_message("Chức năng báo cáo 3G đang được phát triển...")
        else:
            self.show_warning_message("Loại báo cáo không được hỗ trợ!")

    def report_4G_multi_files(self, name, creator, region_name, file_paths):
        """Xử lý tạo báo cáo 4G với nhiều file"""
        if not self.import_4g_success:
            self.show_warning_message(
                "Không thể load module xử lý 4G!\nVui lòng kiểm tra file DataVisualizationFor4G_V2.py"
            )
            return

        if self.worker_thread and self.worker_thread.isRunning():
            self.show_warning_message("Đang xử lý báo cáo khác, vui lòng chờ...")
            return

        # Tạo và chạy worker thread
        self.worker_thread = ReportWorkerThread(
            self.ExcelCSVProcessor, file_paths, name, creator, region_name
        )

        # Kết nối signals
        self.worker_thread.progress_update.connect(self.show_progress_message)
        self.worker_thread.finished_report.connect(self.on_report_finished)

        # Bắt đầu xử lý
        self.worker_thread.start()

        # Vô hiệu hóa nút tạo báo cáo
        self.set_create_buttons_enabled(False)

    def on_report_finished(self, success, message):
        """Callback khi worker thread hoàn thành"""
        self.set_create_buttons_enabled(True)

        if success:
            self.show_success_message(message)
            # Hỏi có muốn mở thư mục kết quả không
            reply = QMessageBox.question(
                self, "Mở thư mục kết quả",
                "Bạn có muốn mở thư mục chứa kết quả báo cáo không?",
                QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No
            )

            if reply == QMessageBox.StandardButton.Yes:
                self.open_output_folder(message)
        else:
            self.show_warning_message(message)

    def set_create_buttons_enabled(self, enabled):
        """Bật/tắt các nút tạo báo cáo"""
        self.btnCreateBac.setEnabled(enabled)
        self.btnCreateTrung.setEnabled(enabled)
        self.btnCreateNam.setEnabled(enabled)

    def open_output_folder(self, message):
        """Mở thư mục kết quả"""
        try:
            import subprocess
            import platform
            import re

            # Tìm đường dẫn thư mục từ message
            output_match = re.search(r'output_[^/\s]+', message)
            if output_match:
                output_dir = output_match.group()

                if platform.system() == "Windows":
                    subprocess.Popen(f'explorer "{os.path.abspath(output_dir)}"')
                elif platform.system() == "Darwin":  # macOS
                    subprocess.Popen(["open", output_dir])
                else:  # Linux
                    subprocess.Popen(["xdg-open", output_dir])
        except Exception as e:
            print(f"Không thể mở thư mục: {e}")

    def show_progress_message(self, message):
        """Hiển thị thông báo tiến trình"""
        self.statusbar.showMessage(message, 2000)  # Hiển thị trong 2 giây

    def show_info_message(self, message):
        """Hiển thị thông báo thông tin"""
        msg = QMessageBox(self)
        msg.setIcon(QMessageBox.Icon.Information)
        msg.setWindowTitle("Thông tin")
        msg.setText(message)
        msg.setStyleSheet("""
            QMessageBox {
                background-color: #404040;
                color: #ffffff;
            }
            QMessageBox QPushButton {
                background-color: #4a90e2;
                color: white;
                border: none;
                padding: 8px 20px;
                border-radius: 5px;
                font-weight: bold;
                min-width: 80px;
            }
            QPushButton:hover {
                background-color: #357abd;
            }
        """)
        msg.exec()

    def show_warning_message(self, message):
        """Hiển thị thông báo cảnh báo với dark theme"""
        msg = QMessageBox(self)
        msg.setIcon(QMessageBox.Icon.Warning)
        msg.setWindowTitle("Cảnh báo")
        msg.setText(message)
        msg.setStyleSheet("""
            QMessageBox {
                background-color: #404040;
                color: #ffffff;
            }
            QMessageBox QPushButton {
                background-color: #ff9f43;
                color: white;
                border: none;
                padding: 8px 20px;
                border-radius: 5px;
                font-weight: bold;
                min-width: 80px;
            }
            QPushButton:hover {
                background-color: #e8890b;
            }
        """)
        msg.exec()

    def show_success_message(self, message):
        """Hiển thị thông báo thành công với dark theme"""
        msg = QMessageBox(self)
        msg.setIcon(QMessageBox.Icon.Information)
        msg.setWindowTitle("Thành công")
        msg.setText(message)
        msg.setStyleSheet("""
            QMessageBox {
                background-color: #404040;
                color: #ffffff;
            }
            QMessageBox QPushButton {
                background-color: #00d2d3;
                color: white;
                border: none;
                padding: 8px 20px;
                border-radius: 5px;
                font-weight: bold;
                min-width: 80px;
            }
            QPushButton:hover {
                background-color: #00a8a9;
            }
        """)
        msg.exec()

    def clear_form(self, region):
        """Xóa dữ liệu form sau khi tạo báo cáo"""
        msg = QMessageBox(self)
        msg.setIcon(QMessageBox.Icon.Question)
        msg.setWindowTitle("Xác nhận")
        msg.setText("Bạn có muốn xóa thông tin form để tạo báo cáo mới?")
        msg.setStandardButtons(QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No)
        msg.setStyleSheet("""
            QMessageBox {
                background-color: #404040;
                color: #ffffff;
            }
            QMessageBox QPushButton {
                background-color: #4a90e2;
                color: white;
                border: none;
                padding: 8px 20px;
                border-radius: 5px;
                font-weight: bold;
                min-width: 80px;
                margin: 5px;
            }
            QPushButton:hover {
                background-color: #357abd;
            }
        """)

        reply = msg.exec()

        if reply == QMessageBox.StandardButton.Yes:
            if region == 'bac':
                self.lineEditNameBac.setCurrentIndex(0)
                self.lineEditCreatorBac.clear()
            elif region == 'trung':
                self.lineEditNameTrung.setCurrentIndex(0)
                self.lineEditCreatorTrung.clear()
            else:  # nam
                self.lineEditNameNam.setCurrentIndex(0)
                self.lineEditCreatorNam.clear()

            # Xóa danh sách file
            self.file_paths[region] = []
            self.update_file_list(region)


if __name__ == "__main__":
    app = QApplication(sys.argv)
    app.setStyle('Fusion')

    # Kiểm tra file .ui có tồn tại không
    if not os.path.exists('main_window.ui'):
        msg = QMessageBox()
        msg.setIcon(QMessageBox.Icon.Critical)
        msg.setWindowTitle("Lỗi")
        msg.setText("Không tìm thấy file main_window.ui!\n\nVui lòng tạo file .ui trong Qt Designer theo hướng dẫn.")
        msg.setStyleSheet("""
            QMessageBox {
                background-color: #404040;
                color: #ffffff;
            }
            QMessageBox QPushButton {
                background-color: #dc3545;
                color: white;
                border: none;
                padding: 8px 20px;
                border-radius: 5px;
                font-weight: bold;
                min-width: 80px;
            }
            QPushButton:hover {
                background-color: #c82333;
            }
        """)
        msg.exec()
        sys.exit(1)

    window = MainWindow()
    window.show()
    sys.exit(app.exec())