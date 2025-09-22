import os
import sys
import subprocess
import importlib.util
from datetime import datetime
from reportlab.lib.pagesizes import A3
from reportlab.pdfgen import canvas
from PIL import Image
import shutil


class Unified3GReportSystem:
    def __init__(self):
        self.output_dir = "unified_report_output"
        self.temp_dirs = {
            'rtwp': 'temp_rtwp',
            'dashboard': 'temp_dashboard',
            'rnc': 'temp_rnc'
        }
        self.expected_images = {
            'rtwp_summary_table.png': 'RTWP Summary Table',
            'rtwp_trend_chart.png': 'RTWP Trend Analysis',
            'Daily_3G_KPI_Dashboard_of_Ericsson.png': 'Ericsson KPI Dashboard',
            'Daily_3G_KPI_Dashboard_of_ZTE.png': 'ZTE KPI Dashboard'
        }

        # Pattern để tìm file có tên động
        self.dynamic_patterns = {
            '3G_KPI_Dashboard_BH_': '3G KPI Dashboard (BH)',
            '3G_KPI_Dashboard_24h_': '3G KPI Dashboard (24h)'
        }

    def setup_directories(self):
        """Tạo các thư mục cần thiết"""
        os.makedirs(self.output_dir, exist_ok=True)
        for temp_dir in self.temp_dirs.values():
            os.makedirs(temp_dir, exist_ok=True)

    def cleanup_directories(self):
        """Dọn dẹp thư mục tạm"""
        for temp_dir in self.temp_dirs.values():
            if os.path.exists(temp_dir):
                shutil.rmtree(temp_dir)

    def run_rtwp_analysis(self):
        """Chạy CountAbnormalCellFor3G.py"""
        print("=" * 60)
        print("STEP 1: Running RTWP Analysis...")
        print("=" * 60)

        try:
            # Import và chạy CountAbnormalCellFor3G
            spec = importlib.util.spec_from_file_location("CountAbnormalCellFor3G", "CountAbnormalCellFor3G.py")
            rtwp_module = importlib.util.module_from_spec(spec)

            # Chuyển directory để output vào thư mục tạm
            original_cwd = os.getcwd()
            os.chdir(self.temp_dirs['rtwp'])

            # Copy file dữ liệu cần thiết
            required_files = [
                'RTWP_3G.xlsx',
                'History Performance_UMTS _RNO_Avg_Mean_RTWP.xlsx'
            ]

            for file in required_files:
                src_path = os.path.join(original_cwd, file)
                if os.path.exists(src_path):
                    shutil.copy2(src_path, file)

            # Chạy module
            spec.loader.exec_module(rtwp_module)
            rtwp_module.main()

            # Quay lại thư mục gốc
            os.chdir(original_cwd)

            # Copy kết quả về thư mục output
            output_files = ['rtwp_summary_table.png', 'rtwp_trend_chart.png']
            for file in output_files:
                src = os.path.join(self.temp_dirs['rtwp'], file)
                dst = os.path.join(self.output_dir, file)
                if os.path.exists(src):
                    shutil.copy2(src, dst)
                    print(f"✓ Generated: {file}")
                else:
                    print(f"✗ Missing: {file}")

            print("RTWP Analysis completed successfully!")
            return True

        except Exception as e:
            print(f"Error in RTWP Analysis: {e}")
            os.chdir(original_cwd)
            return False

    def run_dashboard_analysis(self):
        """Chạy DataVisualizationFor3G.py"""
        print("\n" + "=" * 60)
        print("STEP 2: Running Dashboard Analysis...")
        print("=" * 60)

        try:
            # Import và chạy DataVisualizationFor3G
            spec = importlib.util.spec_from_file_location("DataVisualizationFor3G", "DataVisualizationFor3G.py")
            dashboard_module = importlib.util.module_from_spec(spec)

            # Chuyển directory
            original_cwd = os.getcwd()
            os.chdir(self.temp_dirs['dashboard'])

            # Copy file dữ liệu cần thiết
            required_files = [
                '3G_RNO_KPIs_BH_ZTE_2025-08-06.xlsx',
                '3G_RNO_KPIs_WD_ZTE_2025-08-06.xlsx',
                '3G_RNO_KPIs_BH_scheduled2025-08-06.xlsx',
                '3G_RNO_KPIs_WD_scheduled2025-08-06.xlsx'
            ]

            for file in required_files:
                src_path = os.path.join(original_cwd, file)
                if os.path.exists(src_path):
                    shutil.copy2(src_path, file)

            # Chạy module
            spec.loader.exec_module(dashboard_module)
            dashboard_module.main()

            # Quay lại thư mục gốc
            os.chdir(original_cwd)

            # Copy kết quả về thư mục output
            output_files = [
                'output_ericsson/Daily_3G_KPI_Dashboard_of_Ericsson.png',
                'output_zte/Daily_3G_KPI_Dashboard_of_ZTE.png'
            ]

            for file_path in output_files:
                src = os.path.join(self.temp_dirs['dashboard'], file_path)
                filename = os.path.basename(file_path)
                dst = os.path.join(self.output_dir, filename)
                if os.path.exists(src):
                    shutil.copy2(src, dst)
                    print(f"✓ Generated: {filename}")
                else:
                    print(f"✗ Missing: {filename}")

            print("Dashboard Analysis completed successfully!")
            return True

        except Exception as e:
            print(f"Error in Dashboard Analysis: {e}")
            os.chdir(original_cwd)
            return False

    def run_rnc_dashboard(self):
        """Chạy 3GKPIDashboardByRNC.py"""
        print("\n" + "=" * 60)
        print("STEP 3: Running RNC Dashboard...")
        print("=" * 60)

        try:
            # Import và chạy 3GKPIDashboardByRNC
            spec = importlib.util.spec_from_file_location("3GKPIDashboardByRNC", "3GKPIDashboardByRNC.py")
            rnc_module = importlib.util.module_from_spec(spec)

            # Chuyển directory
            original_cwd = os.getcwd()
            os.chdir(self.temp_dirs['rnc'])

            # Copy file dữ liệu cần thiết
            required_files = [
                "3G_RNO_KPIs_BH_scheduled2025-08-06.csv",
                "3G_RNO_KPIs_BH_ZTE_2025-08-06.csv",
                "3G_RNO_KPIs_WD_scheduled2025-08-06.csv",
                "3G_RNO_KPIs_WD_ZTE_2025-08-06.csv"
            ]

            for file in required_files:
                src_path = os.path.join(original_cwd, file)
                if os.path.exists(src_path):
                    shutil.copy2(src_path, file)

            # Chạy module
            spec.loader.exec_module(rnc_module)

            # Tạo dashboard theo cách của module
            csv_files_bh = [f for f in required_files if 'BH' in f and os.path.exists(f)]
            csv_files_24h = [f for f in required_files if 'WD' in f and os.path.exists(f)]

            if csv_files_bh:
                rnc_module.create_dashboard_from_files(csv_files_bh, "Daily 3G KPI Dashboard by RNC", "BH",
                                                       save_png=True)

            if csv_files_24h:
                rnc_module.create_dashboard_from_files(csv_files_24h, "Daily 3G KPI Dashboard by RNC", "24h",
                                                       save_png=True)

            # Quay lại thư mục gốc
            os.chdir(original_cwd)

            # Copy kết quả về thư mục output (tìm file có pattern phù hợp)
            import glob

            # Tìm file dashboard được tạo
            pattern_files = [
                f"{self.temp_dirs['rnc']}/3G_KPI_Dashboard_*_*.png"
            ]

            for pattern in pattern_files:
                found_files = glob.glob(pattern)
                for src in found_files:
                    filename = os.path.basename(src)
                    dst = os.path.join(self.output_dir, filename)
                    shutil.copy2(src, dst)
                    print(f"✓ Generated: {filename}")

            print("RNC Dashboard completed successfully!")
            return True

        except Exception as e:
            print(f"Error in RNC Dashboard: {e}")
            os.chdir(original_cwd)
            return False

    def find_generated_images(self):
        """Tìm tất cả ảnh đã được tạo ra, bao gồm cả file có tên động"""
        generated_images = []

        for filename in os.listdir(self.output_dir):
            if filename.endswith('.png'):
                filepath = os.path.join(self.output_dir, filename)
                if os.path.exists(filepath):
                    # Xác định title cho file
                    title = self._get_image_title(filename)

                    generated_images.append({
                        'path': filepath,
                        'filename': filename,
                        'title': title
                    })

        # Sắp xếp theo độ ưu tiên
        def get_priority(image):
            filename = image['filename']

            # Priority cố định
            priority_order = [
                'rtwp_summary_table.png',
                'rtwp_trend_chart.png',
                'Daily_3G_KPI_Dashboard_of_Ericsson.png',
                'Daily_3G_KPI_Dashboard_of_ZTE.png'
            ]

            if filename in priority_order:
                return priority_order.index(filename)

            # Priority cho file động
            if filename.startswith('3G_KPI_Dashboard_BH_'):
                return len(priority_order)  # Đặt sau file cố định
            elif filename.startswith('3G_KPI_Dashboard_24h_'):
                return len(priority_order) + 1

            return len(priority_order) + 2  # File khác

        generated_images.sort(key=get_priority)
        return generated_images

    def _get_image_title(self, filename):
        """Xác định title cho ảnh dựa trên tên file"""
        # Kiểm tra file cố định trước
        if filename in self.expected_images:
            return self.expected_images[filename]

        # Kiểm tra file có pattern động
        for pattern, title in self.dynamic_patterns.items():
            if filename.startswith(pattern):
                return title

        # Fallback: tạo title từ tên file
        return filename.replace('.png', '').replace('_', ' ').title()

    def create_multipage_pdf_report(self, images):
        """Tạo báo cáo PDF nhiều trang"""
        if not images:
            print("No images found to create PDF report!")
            return None

        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        pdf_path = os.path.join(self.output_dir, f"3G_Comprehensive_Report_{timestamp}.pdf")

        try:
            c = canvas.Canvas(pdf_path, pagesize=A3)
            width, height = A3

            print(f"\nCreating comprehensive PDF report: {pdf_path}")
            print(f"Total images to include: {len(images)}")

            for i, image_info in enumerate(images, 1):
                print(f"Processing page {i}: {image_info['filename']}")

                # Tạo trang mới
                if i > 1:
                    c.showPage()

                # Thông tin trang
                current_time = datetime.now()
                date_str = current_time.strftime("%d/%m/%Y")
                time_str = current_time.strftime("%H:%M:%S")

                # Tiêu đề trang
                c.setFont("Helvetica-Bold", 24)
                title = "3G Network Performance Report"
                title_width = c.stringWidth(title, "Helvetica-Bold", 24)
                c.drawString((width - title_width) / 2, height - 40, title)

                # Thông tin ngày giờ
                c.setFont("Helvetica", 16)
                datetime_text = f"{date_str} - {time_str}"
                datetime_width = c.stringWidth(datetime_text, "Helvetica", 16)
                c.drawString((width - datetime_width) / 2, height - 70, datetime_text)

                # Tiêu đề ảnh
                c.setFont("Helvetica-Bold", 18)
                image_title = image_info['title']
                title_width = c.stringWidth(image_title, "Helvetica-Bold", 18)
                c.drawString((width - title_width) / 2, height - 110, image_title)

                # Thông tin trang
                c.setFont("Helvetica", 12)
                page_info = f"Page {i} of {len(images)}"
                c.drawString(width - 100, height - 20, page_info)

                # Vẽ ảnh
                self.draw_image_on_page(c, image_info['path'], width, height, top_margin=130)

            # Lưu PDF
            c.save()

            print(f"\n✓ PDF Report created successfully: {pdf_path}")
            return pdf_path

        except Exception as e:
            print(f"Error creating PDF report: {e}")
            return None

    def draw_image_on_page(self, canvas_obj, image_path, page_width, page_height, top_margin=130):
        """Vẽ ảnh lên trang PDF với tối ưu kích thước"""
        try:
            # Mở ảnh để lấy kích thước
            img = Image.open(image_path)
            img_width, img_height = img.size
            img_aspect = img_width / img_height

            # Tính toán không gian khả dụng
            bottom_margin = 20
            side_margin = 20

            available_width = page_width - (side_margin * 2)
            available_height = page_height - top_margin - bottom_margin

            # Tính toán kích thước ảnh tối ưu
            # Ưu tiên chiều rộng tối đa
            new_width = available_width
            new_height = new_width / img_aspect

            # Nếu chiều cao vượt quá, điều chỉnh theo chiều cao
            if new_height > available_height:
                new_height = available_height
                new_width = new_height * img_aspect

            # Vị trí ảnh (căn giữa)
            x = (page_width - new_width) / 2
            y = page_height - top_margin - new_height

            # Vẽ ảnh
            canvas_obj.drawImage(image_path, x, y, new_width, new_height)

            print(f"   Image size: {img_width}x{img_height} -> {new_width:.0f}x{new_height:.0f}")

        except Exception as e:
            print(f"   Error drawing image {image_path}: {e}")
            # Vẽ thông báo lỗi thay thế
            canvas_obj.setFont("Helvetica", 16)
            canvas_obj.drawString(page_width / 2 - 100, page_height / 2,
                                  f"Error loading image: {os.path.basename(image_path)}")

    def generate_summary_report(self, images, pdf_path):
        """Tạo báo cáo tóm tắt"""
        summary_path = os.path.join(self.output_dir, "generation_summary.txt")

        with open(summary_path, 'w', encoding='utf-8') as f:
            f.write("3G NETWORK PERFORMANCE REPORT - GENERATION SUMMARY\n")
            f.write("=" * 60 + "\n\n")

            f.write(f"Generated on: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
            f.write(f"Total images processed: {len(images)}\n")
            f.write(f"PDF Report: {os.path.basename(pdf_path) if pdf_path else 'Failed to create'}\n\n")

            f.write("GENERATED IMAGES:\n")
            f.write("-" * 30 + "\n")
            for i, img in enumerate(images, 1):
                f.write(f"{i}. {img['title']}\n")
                f.write(f"   File: {img['filename']}\n")
                f.write(f"   Size: {os.path.getsize(img['path']) / 1024:.1f} KB\n\n")

            f.write("PROCESSING MODULES:\n")
            f.write("-" * 30 + "\n")
            f.write("1. CountAbnormalCellFor3G.py - RTWP Analysis\n")
            f.write("2. DataVisualizationFor3G.py - Individual Dashboards\n")
            f.write("3. 3GKPIDashboardByRNC.py - RNC Dashboards\n\n")

            f.write("OUTPUT STRUCTURE:\n")
            f.write("-" * 30 + "\n")
            f.write(f"Main output directory: {self.output_dir}/\n")
            f.write("├── 3G_Comprehensive_Report_YYYYMMDD_HHMMSS.pdf\n")
            f.write("├── generation_summary.txt\n")
            f.write("└── [Generated PNG files]\n")

        print(f"✓ Summary report created: {summary_path}")

    def run_complete_analysis(self):
        """Chạy toàn bộ quy trình phân tích"""
        print("🚀 STARTING 3G COMPREHENSIVE ANALYSIS SYSTEM")
        print("=" * 80)

        # Setup
        self.setup_directories()

        success_count = 0

        # Chạy từng module
        if self.run_rtwp_analysis():
            success_count += 1

        if self.run_dashboard_analysis():
            success_count += 1

        if self.run_rnc_dashboard():
            success_count += 1

        print("\n" + "=" * 80)
        print("STEP 4: Creating Comprehensive PDF Report...")
        print("=" * 80)

        # Tìm tất cả ảnh đã tạo
        generated_images = self.find_generated_images()

        if generated_images:
            print(f"\nFound {len(generated_images)} generated images:")
            for img in generated_images:
                print(f"  • {img['filename']} - {img['title']}")

            # Tạo báo cáo PDF
            pdf_path = self.create_multipage_pdf_report(generated_images)

            # Tạo báo cáo tóm tắt
            self.generate_summary_report(generated_images, pdf_path)

        else:
            print("⚠️  No images found to create PDF report!")

        # Cleanup
        self.cleanup_directories()

        # Kết quả cuối cùng
        print("\n" + "=" * 80)
        print("🎉 ANALYSIS COMPLETED!")
        print("=" * 80)
        print(f"Modules successfully executed: {success_count}/3")
        print(f"Generated images: {len(generated_images)}")
        print(f"Output directory: {os.path.abspath(self.output_dir)}")

        if generated_images:
            print(f"\n📄 Final PDF Report: {os.path.basename(pdf_path) if pdf_path else 'Creation failed'}")

        return success_count, generated_images, pdf_path


def main():
    """Hàm main để chạy hệ thống"""
    try:
        # Khởi tạo hệ thống
        system = Unified3GReportSystem()

        # Chạy toàn bộ quy trình
        success_count, images, pdf_path = system.run_complete_analysis()

        # Kiểm tra kết quả
        if success_count >= 1 and images:
            print(f"\n✅ SUCCESS: Generated comprehensive 3G report with {len(images)} visualizations")
            if pdf_path:
                print(f"📋 PDF Report: {pdf_path}")
        else:
            print(f"\n❌ PARTIAL SUCCESS: Only {success_count}/3 modules completed successfully")

        return success_count >= 1

    except Exception as e:
        print(f"\n💥 CRITICAL ERROR in main execution: {e}")
        import traceback
        traceback.print_exc()
        return False


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)