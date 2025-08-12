import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import os
import numpy as np
from datetime import datetime
from PIL import Image, ImageDraw, ImageFont
import math


class ExcelCSVProcessor:
    def __init__(self):
        self.cleaned_data = {}

    def clean_excel_to_csv(self, excel_path, csv_path, sheet_name=0):
        """
        Chuyển đổi Excel sang CSV với việc làm sạch dữ liệu chặt chẽ
        """
        try:
            print(f"🔄 Đang xử lý file: {excel_path}")

            # Đọc file Excel với nhiều tùy chọn để tránh lỗi
            df = pd.read_excel(excel_path, sheet_name=sheet_name, header=None)

            # Tìm dòng header thực sự (dòng đầu tiên có 'Date')
            header_row = None
            for i in range(min(10, len(df))):  # Tìm trong 10 dòng đầu
                row_values = df.iloc[i].astype(str).str.lower()
                if any('date' in str(val).lower() for val in row_values):
                    header_row = i
                    break

            if header_row is None:
                print("⚠️ Không tìm thấy header chứa 'Date', sử dụng dòng đầu tiên")
                header_row = 0

            # Đọc lại với header đúng
            df = pd.read_excel(excel_path, sheet_name=sheet_name, header=header_row)

            # Làm sạch tên cột
            df.columns = df.columns.astype(str)  # Chuyển tất cả tên cột thành string
            df.columns = [col.strip() for col in df.columns]  # Loại bỏ khoảng trắng

            # Loại bỏ các cột không có tên hoặc tên lạ (Unnamed)
            unnamed_cols = [col for col in df.columns if 'unnamed' in col.lower() or col.startswith('Unnamed')]
            if unnamed_cols:
                print(f"🗑️ Loại bỏ {len(unnamed_cols)} cột không tên: {unnamed_cols[:3]}...")
                df = df.drop(columns=unnamed_cols)

            # Loại bỏ các cột hoàn toàn trống
            df = df.dropna(axis=1, how='all')

            # Loại bỏ các hàng hoàn toàn trống
            df = df.dropna(axis=0, how='all')

            # Làm sạch dữ liệu trong cột Date
            date_col = df.columns[0]  # Giả sử cột đầu tiên là Date

            # Chuyển đổi cột Date
            try:
                # Thử nhiều cách chuyển đổi ngày tháng
                if pd.api.types.is_string_dtype(df[date_col]):
                    df[date_col] = pd.to_datetime(df[date_col], errors='coerce', dayfirst=True)
                elif pd.api.types.is_numeric_dtype(df[date_col]):
                    # Nếu là số (Excel date serial), chuyển đổi
                    df[date_col] = pd.to_datetime(df[date_col], origin='1899-12-30', unit='D', errors='coerce')
                else:
                    df[date_col] = pd.to_datetime(df[date_col], errors='coerce')
            except Exception as e:
                print(f"⚠️ Lỗi chuyển đổi ngày tháng: {e}")

            # Loại bỏ các hàng có ngày không hợp lệ
            df = df.dropna(subset=[date_col])

            # Sắp xếp theo ngày
            df = df.sort_values(by=date_col).reset_index(drop=True)

            # Làm sạch dữ liệu số
            for col in df.columns[1:]:  # Bỏ qua cột Date
                if df[col].dtype == 'object':
                    # Thử chuyển đổi thành số
                    df[col] = pd.to_numeric(df[col], errors='coerce')

            # Loại bỏ các hàng có quá nhiều giá trị NaN
            threshold = len(df.columns) * 0.5  # Nếu hơn 50% cột là NaN thì loại bỏ
            df = df.dropna(thresh=threshold)

            # Lưu thành CSV
            df.to_csv(csv_path, index=False, encoding='utf-8')
            print(f"✅ Đã chuyển đổi thành công: {csv_path}")
            print(f"   📊 Kích thước dữ liệu: {df.shape[0]} hàng x {df.shape[1]} cột")
            print(f"   📅 Khoảng thời gian: {df[date_col].min()} đến {df[date_col].max()}")

            self.cleaned_data[csv_path] = df
            return df

        except Exception as e:
            print(f"❌ Lỗi khi xử lý {excel_path}: {e}")
            return None

    def verify_csv_structure(self, csv_path):
        """
        Kiểm tra cấu trúc file CSV sau khi chuyển đổi
        """
        try:
            df = pd.read_csv(csv_path)
            print(f"\n🔍 Kiểm tra cấu trúc file: {csv_path}")
            print(f"   📏 Kích thước: {df.shape}")
            print(f"   📋 Các cột đầu tiên: {list(df.columns[:5])}")
            print(f"   📅 Cột Date: {df.columns[0]} - Kiểu dữ liệu: {df.dtypes[0]}")
            print(f"   🔢 5 dòng đầu tiên:")
            print(df.head())

            # Kiểm tra xem có cột lạ không
            suspicious_cols = [col for col in df.columns if 'unnamed' in col.lower()]
            if suspicious_cols:
                print(f"   ⚠️ Phát hiện {len(suspicious_cols)} cột lạ: {suspicious_cols}")
                return False

            return True

        except Exception as e:
            print(f"❌ Lỗi khi kiểm tra {csv_path}: {e}")
            return False

    def create_charts_from_csv(self, csv_all_day, csv_busy_hour, output_dir="charts_output"):
        """
        Tạo biểu đồ từ file CSV đã được làm sạch
        """
        try:
            # Đọc dữ liệu
            df_all = pd.read_csv(csv_all_day)
            df_bh = pd.read_csv(csv_busy_hour)

            # Chuyển đổi cột Date
            date_col = df_all.columns[0]
            df_all[date_col] = pd.to_datetime(df_all[date_col])
            df_bh[date_col] = pd.to_datetime(df_bh[date_col])

            # Tạo thư mục output
            os.makedirs(output_dir, exist_ok=True)

            # Lấy danh sách KPI (bỏ qua cột Date và Cell Type)
            skip_cols = [date_col, 'Cell Type', 'RRC Att', 'ERAB Att','S1 Att','ERAB Release','pmHoPrepAttLteIntraF','DC_E_ERBS_UTRANCELLRELATION.pmHoPrepAtt','CSFB Att','CSFB Succ to GSM','PS Traffic UL (GB)','pmHoPrepAttLteInterF','X2 HOSR','X2 HO Att','S1 HOSR','S1 HO Att','RRC Connected User Max','RTWP','RRC Connected User Average','RRC Connected User Max']
            kpi_cols = [col for col in df_all.columns if col not in skip_cols]

            print(f"📈 Tạo biểu đồ cho {len(kpi_cols)} KPI...")

            # Danh sách để lưu đường dẫn các biểu đồ đã tạo
            created_chart_paths = []

            # Tạo biểu đồ cho từng KPI
            created_charts = 0
            for kpi in kpi_cols:
                if kpi not in df_bh.columns:
                    print(f"⚠️ KPI '{kpi}' không có trong dữ liệu Busy Hours. Bỏ qua.")
                    continue

                try:
                    plt.figure(figsize=(12, 6))

                    # Vẽ đường All Day
                    plt.plot(df_all[date_col], df_all[kpi],
                             label='All Day', color='#1f77b4', linewidth=2, marker='o', markersize=4)

                    # Vẽ đường Busy Hours
                    plt.plot(df_bh[date_col], df_bh[kpi],
                             label='Busy Hours', color='#ff7f0e', linewidth=2, marker='s', markersize=4)

                    # Định dạng biểu đồ
                    plt.title(f'{kpi}', fontsize=14, fontweight='bold', pad=20)
                    plt.xlabel('Date', fontsize=12)
                    plt.ylabel(kpi, fontsize=12)
                    plt.grid(True, linestyle='--', alpha=0.7)
                    plt.legend(fontsize=11, loc='best')

                    # Định dạng trục x
                    plt.gca().xaxis.set_major_formatter(mdates.DateFormatter('%m/%d'))
                    plt.gca().xaxis.set_major_locator(mdates.DayLocator(interval=2))
                    plt.xticks(rotation=45)

                    # Màu nền
                    plt.gca().set_facecolor('#f8f9fa')

                    plt.tight_layout()

                    # Lưu biểu đồ với tên file an toàn
                    safe_filename = "".join(c for c in kpi if c.isalnum() or c in (' ', '-', '_')).rstrip()
                    safe_filename = safe_filename.replace(' ', '_')
                    filepath = os.path.join(output_dir, f"{safe_filename}.png")

                    plt.savefig(filepath, dpi=300, bbox_inches='tight')
                    plt.close()

                    created_charts += 1
                    created_chart_paths.append(filepath)

                except Exception as e:
                    print(f"❌ Lỗi khi tạo biểu đồ cho {kpi}: {e}")
                    plt.close()

            print(f"✅ Đã tạo {created_charts} biểu đồ trong thư mục '{output_dir}'")

            # Tạo báo cáo tổng hợp
            if created_chart_paths:
                self.create_comprehensive_report(created_chart_paths, output_dir)

            return created_chart_paths

        except Exception as e:
            print(f"❌ Lỗi khi tạo biểu đồ: {e}")
            return []

    def create_comprehensive_report(self, chart_paths, output_dir):
        """
        Tạo báo cáo tổng hợp gộp tất cả biểu đồ thành một file ảnh duy nhất
        """
        try:
            print("\n📋 Đang tạo báo cáo tổng hợp...")

            if not chart_paths:
                print("❌ Không có biểu đồ nào để tạo báo cáo")
                return None

            # Đọc tất cả ảnh biểu đồ
            images = []
            for path in chart_paths:
                try:
                    img = Image.open(path)
                    images.append(img)
                except Exception as e:
                    print(f"⚠️ Không thể đọc ảnh {path}: {e}")

            if not images:
                print("❌ Không có ảnh hợp lệ để tạo báo cáo")
                return None

            # Tính toán layout tối ưu (số cột x số hàng)
            num_charts = len(images)
            cols = math.ceil(math.sqrt(num_charts))
            rows = math.ceil(num_charts / cols)

            print(f"   📐 Layout: {rows} hàng x {cols} cột cho {num_charts} biểu đồ")

            # Kích thước của mỗi biểu đồ trong báo cáo (resize để phù hợp)
            chart_width = 800
            chart_height = 480

            # Kích thước margin và padding
            margin = 50
            padding = 20
            header_height = 100

            # Tính toán kích thước tổng của báo cáo
            total_width = margin * 2 + cols * chart_width + (cols - 1) * padding
            total_height = margin * 2 + header_height + rows * chart_height + (rows - 1) * padding

            # Tạo canvas trắng cho báo cáo
            report_image = Image.new('RGB', (total_width, total_height), 'white')
            draw = ImageDraw.Draw(report_image)

            # Thêm tiêu đề báo cáo
            try:
                # Thử sử dụng font hệ thống
                title_font = ImageFont.truetype("arial.ttf", 32)
                subtitle_font = ImageFont.truetype("arial.ttf", 18)
            except:
                # Fallback về font mặc định nếu không tìm thấy arial
                title_font = ImageFont.load_default()
                subtitle_font = ImageFont.load_default()

            # Vẽ tiêu đề
            title_text = "4G KPI PERFORMANCE REPORT"
            title_bbox = draw.textbbox((0, 0), title_text, font=title_font)
            title_width = title_bbox[2] - title_bbox[0]
            title_x = (total_width - title_width) // 2

            draw.text((title_x, margin), title_text, fill='black', font=title_font)

            # Vẽ subtitle với thời gian tạo
            current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            subtitle_text = f"Generated on {current_time} | Total KPIs: {num_charts}"
            subtitle_bbox = draw.textbbox((0, 0), subtitle_text, font=subtitle_font)
            subtitle_width = subtitle_bbox[2] - subtitle_bbox[0]
            subtitle_x = (total_width - subtitle_width) // 2

            draw.text((subtitle_x, margin + 40), subtitle_text, fill='gray', font=subtitle_font)

            # Vẽ đường phân cách
            line_y = margin + header_height - 20
            draw.line([(margin, line_y), (total_width - margin, line_y)], fill='lightgray', width=2)

            # Đặt các biểu đồ vào báo cáo
            for idx, img in enumerate(images):
                row = idx // cols
                col = idx % cols

                # Resize ảnh để phù hợp với kích thước đã định
                img_resized = img.resize((chart_width, chart_height), Image.Resampling.LANCZOS)

                # Tính toán vị trí
                x = margin + col * (chart_width + padding)
                y = margin + header_height + row * (chart_height + padding)

                # Dán ảnh vào báo cáo
                report_image.paste(img_resized, (x, y))

                # Thêm border cho mỗi biểu đồ
                draw.rectangle([x - 1, y - 1, x + chart_width + 1, y + chart_height + 1],
                               outline='lightgray', width=1)

            # Thêm footer
            footer_y = total_height - margin + 10
            footer_text = "All Day vs Busy Hours Comparison • Generated by ExcelCSVProcessor"
            try:
                footer_font = ImageFont.truetype("arial.ttf", 12)
            except:
                footer_font = ImageFont.load_default()

            footer_bbox = draw.textbbox((0, 0), footer_text, font=footer_font)
            footer_width = footer_bbox[2] - footer_bbox[0]
            footer_x = (total_width - footer_width) // 2

            draw.text((footer_x, footer_y), footer_text, fill='gray', font=footer_font)

            # Lưu báo cáo
            report_path = os.path.join(output_dir, "4G_KPI_Comprehensive_Report.png")
            report_image.save(report_path, "PNG", quality=95)

            # Tạo thêm phiên bản PDF nếu có thể
            try:
                pdf_path = os.path.join(output_dir, "4G_KPI_Comprehensive_Report.pdf")
                report_image.save(pdf_path, "PDF", quality=95)
                print(f"✅ Đã tạo báo cáo PDF: {pdf_path}")
            except Exception as e:
                print(f"⚠️ Không thể tạo PDF: {e}")

            print(f"✅ Đã tạo báo cáo tổng hợp: {report_path}")
            print(f"   📏 Kích thước: {total_width} x {total_height} pixels")
            print(f"   📊 Chứa {num_charts} biểu đồ KPI")

            return report_path

        except Exception as e:
            print(f"❌ Lỗi khi tạo báo cáo tổng hợp: {e}")
            return None

    def create_summary_table(self, csv_all_day, csv_busy_hour, output_dir):
        """
        Tạo bảng tóm tắt thống kê cho báo cáo
        """
        try:
            print("\n📊 Đang tạo bảng tóm tắt thống kê...")

            # Đọc dữ liệu
            df_all = pd.read_csv(csv_all_day)
            df_bh = pd.read_csv(csv_busy_hour)

            date_col = df_all.columns[0]
            skip_cols = [date_col, 'Cell Type']
            kpi_cols = [col for col in df_all.columns if col not in skip_cols and col in df_bh.columns]

            # Tạo bảng thống kê
            summary_data = []
            for kpi in kpi_cols:
                all_day_avg = df_all[kpi].mean()
                busy_hour_avg = df_bh[kpi].mean()
                difference = busy_hour_avg - all_day_avg
                change_percent = (difference / all_day_avg * 100) if all_day_avg != 0 else 0

                summary_data.append({
                    'KPI': kpi,
                    'All Day Avg': round(all_day_avg, 2),
                    'Busy Hours Avg': round(busy_hour_avg, 2),
                    'Difference': round(difference, 2),
                    'Change (%)': round(change_percent, 2)
                })

            summary_df = pd.DataFrame(summary_data)

            # Lưu thành CSV
            summary_path = os.path.join(output_dir, "KPI_Summary_Statistics.csv")
            summary_df.to_csv(summary_path, index=False)

            print(f"✅ Đã tạo bảng tóm tắt: {summary_path}")
            return summary_path

        except Exception as e:
            print(f"❌ Lỗi khi tạo bảng tóm tắt: {e}")
            return None


def main():
    """
    Hàm main để chạy chương trình
    """
    processor = ExcelCSVProcessor()

    print("=" * 60)
    print("🚀 CHƯƠNG TRÌNH CHUYỂN ĐỔI EXCEL SANG CSV VÀ TẠO BÁO CÁO TỔNG HỢP")
    print("=" * 60)

    # Đường dẫn file Excel
    excel_files = {
        '4G_KPI Cell FDD Data_24h_scheduled.xlsx': '4G_KPI_Cell_FDD_Data_24h_clean.csv',
        '4G_KPI Cell FDD Data_BH_scheduled.xlsx': '4G_KPI_Cell_FDD_Data_BH_clean.csv'
    }

    print("\n📋 BƯỚC 1: CHUYỂN ĐỔI EXCEL SANG CSV")
    print("-" * 40)

    converted_files = {}

    for excel_file, csv_file in excel_files.items():
        if os.path.exists(excel_file):
            df = processor.clean_excel_to_csv(excel_file, csv_file)
            if df is not None:
                converted_files[excel_file] = csv_file
                # Kiểm tra cấu trúc file CSV
                processor.verify_csv_structure(csv_file)
        else:
            print(f"⚠️ File không tồn tại: {excel_file}")

    print("\n📊 BƯỚC 2: TẠO BIỂU ĐỒ VÀ BÁO CÁO TỔNG HỢP")
    print("-" * 40)

    if len(converted_files) >= 2:
        csv_files = list(converted_files.values())
        chart_paths = processor.create_charts_from_csv(csv_files[0], csv_files[1])

        # Tạo bảng tóm tắt thống kê
        processor.create_summary_table(csv_files[0], csv_files[1], "charts_output")

    else:
        print("❌ Cần ít nhất 2 file CSV để tạo biểu đồ so sánh")

    print("\n" + "=" * 60)
    print("🎉 HOÀN THÀNH! Kiểm tra thư mục 'charts_output' để xem:")
    print("   📈 Các biểu đồ riêng lẻ")
    print("   📋 Báo cáo tổng hợp (4G_KPI_Comprehensive_Report.png)")
    print("   📊 Bảng tóm tắt thống kê (KPI_Summary_Statistics.csv)")
    print("=" * 60)


# Hàm tiện ích để sửa file CSV bị lỗi
def fix_corrupted_csv(input_csv, output_csv):
    """
    Sửa file CSV bị lỗi (có dòng Unnamed columns)
    """
    try:
        print(f"🔧 Đang sửa file CSV bị lỗi: {input_csv}")

        # Đọc file với header=None để tránh lỗi
        df = pd.read_csv(input_csv, header=None)

        # Tìm dòng header thực sự
        header_row = None
        for i in range(min(5, len(df))):
            row_values = df.iloc[i].astype(str)
            if any('date' in str(val).lower() for val in row_values):
                header_row = i
                break

        if header_row is not None:
            # Lấy header từ dòng đúng
            new_header = df.iloc[header_row].tolist()
            # Lấy dữ liệu từ dòng sau header
            data_rows = df.iloc[header_row + 1:].values

            # Tạo DataFrame mới với header đúng
            df_clean = pd.DataFrame(data_rows, columns=new_header)

            # Loại bỏ các cột không tên
            df_clean = df_clean.loc[:, ~df_clean.columns.str.contains('^Unnamed')]

            # Lưu file đã sửa
            df_clean.to_csv(output_csv, index=False)
            print(f"✅ Đã sửa và lưu: {output_csv}")

            return df_clean
        else:
            print("❌ Không tìm thấy header hợp lệ")
            return None

    except Exception as e:
        print(f"❌ Lỗi khi sửa file CSV: {e}")
        return None


if __name__ == "__main__":
    # Kiểm tra và cài đặt thư viện cần thiết
    required_packages = ['pandas', 'matplotlib', 'openpyxl', 'pillow']
    print("📦 Kiểm tra các thư viện cần thiết:")
    for package in required_packages:
        try:
            if package == 'pillow':
                __import__('PIL')
                print(f"   ✅ {package}")
            else:
                __import__(package)
                print(f"   ✅ {package}")
        except ImportError:
            if package == 'pillow':
                print(f"   ❌ {package} - Chạy: pip install Pillow")
            else:
                print(f"   ❌ {package} - Chạy: pip install {package}")

    print("\n" + "=" * 60)

    # Chạy chương trình chính
    main()

    # Nếu bạn có file CSV bị lỗi, sử dụng hàm này để sửa:
    # fix_corrupted_csv('4G_KPI_Cell_FDD_Data_BH_error.csv', '4G_KPI_Cell_FDD_Data_BH_fixed.csv')