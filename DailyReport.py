from PyQt6.QtWidgets import (
    QApplication, QMainWindow,
    QMessageBox, QFileDialog
)
from PyQt6.QtCore import Qt
from PyQt6 import uic
import sys
import os


class MainWindow(QMainWindow):
    def __init__(self):
        super().__init__()

        # Load UI file
        try:
            uic.loadUi('main_window.ui', self)
        except FileNotFoundError:
            QMessageBox.critical(None, "Lỗi", "Không tìm thấy file main_window.ui!")
            sys.exit(1)

        # Biến lưu trữ file paths
        self.file_paths = {
            'bac': None,
            'trung': None,
            'nam': None
        }

        # Thiết lập giao diện và kết nối signals
        self.setup_ui()
        self.connect_signals()

    def setup_ui(self):
        """Thiết lập giao diện"""
        self.setWindowTitle("Ứng dụng báo cáo hàng ngày - PyQt6")
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

        # Apply input style
        for widget in [self.lineEditCreatorBac,
                       self.lineEditCreatorTrung,
                       self.lineEditCreatorNam]:
            widget.setStyleSheet(input_style)

        # ComboBoxes (report type)
        combo_style = (
            "QComboBox {"
            " border: 2px solid #606060;"
            " border-radius: 8px;"
            " padding: 8px 12px;"
            " font-size: 14px;"
            " background-color: #505050;"
            " color: #ffffff;"
            " margin: 5px 0;"
            " min-height: 20px;"
            "}"
            "QComboBox:focus { border-color: #4a90e2; background-color: #454545; }"
            "QComboBox:hover { border-color: #707070; }"
        )
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
                padding: 10px 20px;
                border-radius: 8px;
                font-weight: bold;
                font-size: 13px;
                margin: 5px;
                min-width: 100px;
                min-height: 35px;
            }
            QPushButton:hover {
                background-color: #357abd;
            }
            QPushButton:pressed {
                background-color: #2968a3;
            }
        """

        self.btnFileBac.setStyleSheet(file_btn_style)
        self.btnFileTrung.setStyleSheet(file_btn_style)
        self.btnFileNam.setStyleSheet(file_btn_style)

        # Style cho file labels
        file_label_style = """
            QLabel {
                color: #cccccc;
                font-style: italic;
                font-size: 13px;
                padding: 12px 16px;
                margin: 5px;
                background-color: #505050;
                border: 1px solid #606060;
                border-radius: 6px;
                min-height: 20px;
            }
        """

        self.lblFileBac.setStyleSheet(file_label_style)
        self.lblFileTrung.setStyleSheet(file_label_style)
        self.lblFileNam.setStyleSheet(file_label_style)

        # Style cho create buttons - mỗi miền có màu riêng
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

        # Set main widget background
        self.centralwidget.setStyleSheet("""
            QWidget {
                background-color: #2b2b2b;
            }
        """)

        # Set stacked widget background
        self.stackedWidget.setStyleSheet("""
            QStackedWidget {
                background-color: #2b2b2b;
            }
            QWidget {
                background-color: #2b2b2b;
            }
        """)

        # Đặt trang mặc định
        self.stackedWidget.setCurrentIndex(0)

    def connect_signals(self):
        """Kết nối signals với slots"""
        # Navigation buttons
        self.btnMienBac.clicked.connect(lambda: self.change_page(0))
        self.btnMienTrung.clicked.connect(lambda: self.change_page(1))
        self.btnMienNam.clicked.connect(lambda: self.change_page(2))

        # File selection buttons
        self.btnFileBac.clicked.connect(lambda: self.select_file('bac', self.lblFileBac))
        self.btnFileTrung.clicked.connect(lambda: self.select_file('trung', self.lblFileTrung))
        self.btnFileNam.clicked.connect(lambda: self.select_file('nam', self.lblFileNam))

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

    def select_file(self, region, label_widget):
        """Xử lý chọn file"""
        file_dialog = QFileDialog()
        file_dialog.setStyleSheet("""
            QFileDialog {
                background-color: #404040;
                color: #ffffff;
            }
        """)

        file_path, _ = file_dialog.getOpenFileName(
            self,
            f"Chọn file dữ liệu cho miền {region}",
            "",
            "Tất cả các file (*);;Excel Files (*.xlsx *.xls);;CSV Files (*.csv);;Text Files (*.txt)"
        )

        if file_path:
            # Lưu đường dẫn file
            self.file_paths[region] = file_path

            # Hiển thị tên file
            file_name = os.path.basename(file_path)
            label_widget.setText(file_name)
            label_widget.setToolTip(file_path)  # Hiện full path khi hover

            # Cập nhật style để hiển thị file đã được chọn
            label_widget.setStyleSheet("""
                QLabel {
                    color: #ffffff;
                    font-weight: bold;
                    font-size: 13px;
                    padding: 12px 16px;
                    margin: 5px;
                    background-color: #4a90e2;
                    border: 1px solid #4a90e2;
                    border-radius: 6px;
                    min-height: 20px;
                }
            """)

            # Thông báo thành công với dark theme
            msg = QMessageBox(self)
            msg.setIcon(QMessageBox.Icon.Information)
            msg.setWindowTitle("Thành công")
            msg.setText(f"Đã chọn file thành công!\n\nFile: {file_name}")
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
                QMessageBox QPushButton:hover {
                    background-color: #357abd;
                }
            """)
            msg.exec()

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

        # Validation with styled message boxes
        if not name:
            self.show_warning_message("Vui lòng chọn loại báo cáo!")
            return

        if not creator:
            self.show_warning_message("Vui lòng nhập tên người tạo!")
            return

        if not self.file_paths.get(region):
            self.show_warning_message("Vui lòng chọn file dữ liệu!")
            return

        # Tạo thông báo thành công
        file_name = os.path.basename(self.file_paths[region])
        success_message = f"""Đã tạo báo cáo thành công!

Thông tin báo cáo:
• Loại báo cáo: {name}
• Người tạo: {creator}
• Khu vực: {region_name}
• File dữ liệu: {file_name}

Báo cáo đã được lưu vào hệ thống."""

        self.show_success_message(success_message)

        # Optional: Clear form sau khi tạo báo cáo thành công
        self.clear_form(region)

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
            QMessageBox QPushButton:hover {
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
            QMessageBox QPushButton:hover {
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
            QMessageBox QPushButton:hover {
                background-color: #357abd;
            }
        """)

        reply = msg.exec()

        if reply == QMessageBox.StandardButton.Yes:
            if region == 'bac':
                self.lineEditNameBac.setCurrentIndex(0)
                self.lineEditCreatorBac.clear()
                self.lblFileBac.setText("Chưa chọn file")
                self.lblFileBac.setStyleSheet("""
                    QLabel {
                        color: #cccccc;
                        font-style: italic;
                        font-size: 13px;
                        padding: 12px 16px;
                        margin: 5px;
                        background-color: #505050;
                        border: 1px solid #606060;
                        border-radius: 6px;
                        min-height: 20px;
                    }
                """)
            elif region == 'trung':
                self.lineEditNameTrung.setCurrentIndex(0)
                self.lineEditCreatorTrung.clear()
                self.lblFileTrung.setText("Chưa chọn file")
                self.lblFileTrung.setStyleSheet("""
                    QLabel {
                        color: #cccccc;
                        font-style: italic;
                        font-size: 13px;
                        padding: 12px 16px;
                        margin: 5px;
                        background-color: #505050;
                        border: 1px solid #606060;
                        border-radius: 6px;
                        min-height: 20px;
                    }
                """)
            else:  # nam
                self.lineEditNameNam.setCurrentIndex(0)
                self.lineEditCreatorNam.clear()
                self.lblFileNam.setText("Chưa chọn file")
                self.lblFileNam.setStyleSheet("""
                    QLabel {
                        color: #cccccc;
                        font-style: italic;
                        font-size: 13px;
                        padding: 12px 16px;
                        margin: 5px;
                        background-color: #505050;
                        border: 1px solid #606060;
                        border-radius: 6px;
                        min-height: 20px;
                    }
                """)

            # Xóa file path
            self.file_paths[region] = None


if __name__ == "__main__":
    app = QApplication(sys.argv)

    # Set application style
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
            QMessageBox QPushButton:hover {
                background-color: #c82333;
            }
        """)
        msg.exec()
        sys.exit(1)

    window = MainWindow()
    window.show()
    sys.exit(app.exec())