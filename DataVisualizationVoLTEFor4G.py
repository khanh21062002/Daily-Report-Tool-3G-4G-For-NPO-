import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import numpy as np
import os
import warnings
from datetime import datetime, timedelta
from PIL import Image, ImageDraw, ImageFont
import math

# Tắt warnings để output sạch hơn
warnings.filterwarnings('ignore')


class VoLTEKPIProcessor:
    def __init__(self):
        """
        Khởi tạo class processor với cấu hình matplotlib tiếng Việt
        """
        # Cấu hình matplotlib để hiển thị tiếng Việt
        plt.rcParams['font.family'] = 'DejaVu Sans'
        plt.rcParams['axes.unicode_minus'] = False

        self.cleaned_data = {}
        self.csv_files = {}

        print("VOLTE KPI DATA PROCESSOR")
        print("=" * 70)

    def read_excel_file(self, excel_path):
        """
        Đọc file Excel và xác định các sheets cần xử lý
        """
        try:
            print(f"📖 Đang đọc file Excel: {excel_path}")

            # Đọc tất cả sheet names
            excel_file = pd.ExcelFile(excel_path)
            all_sheets = excel_file.sheet_names
            print(f"📊 Tất cả sheets: {all_sheets}")

            # Xác định các sheet dữ liệu cần xử lý (chỉ 2 sheet đầu tiên)
            target_sheets = ["Net KPI_Daily", "Net KPI_Hourly"]

            # Tìm sheets có sẵn
            available_sheets = []
            for sheet in target_sheets:
                if sheet in all_sheets:
                    available_sheets.append(sheet)
                    print(f"✅ Tìm thấy sheet: {sheet}")
                else:
                    # Tìm sheet tương tự
                    similar_sheet = self._find_similar_sheet(sheet, all_sheets)
                    if similar_sheet:
                        available_sheets.append(similar_sheet)
                        print(f"✅ Tìm thấy sheet tương tự: {similar_sheet}")
                    else:
                        print(f"⚠️ Không tìm thấy sheet: {sheet}")

            if not available_sheets:
                print("❌ Không tìm thấy sheet dữ liệu cần thiết!")
                return None

            # Đọc dữ liệu từ các sheets
            dataframes = {}
            for sheet_name in available_sheets:
                print(f"📖 Đang đọc sheet: {sheet_name}")

                # Đọc với nhiều phương pháp để tránh lỗi
                df = self._read_sheet_robust(excel_file, sheet_name)

                if df is not None and not df.empty:
                    dataframes[sheet_name] = df
                    print(f"   📊 Kích thước raw: {df.shape}")
                else:
                    print(f"   ❌ Không thể đọc dữ liệu từ {sheet_name}")

            return dataframes

        except Exception as e:
            print(f"❌ Lỗi khi đọc file Excel: {e}")
            return None

    def _find_similar_sheet(self, target_sheet, all_sheets):
        """
        Tìm sheet có tên tương tự
        """
        target_lower = target_sheet.lower()
        for sheet in all_sheets:
            sheet_lower = sheet.lower()
            if any(keyword in sheet_lower for keyword in ['daily', 'hourly', 'kpi']):
                if 'daily' in target_lower and 'daily' in sheet_lower:
                    return sheet
                elif 'hourly' in target_lower and ('hourly' in sheet_lower or 'hour' in sheet_lower):
                    return sheet
        return None

    def _read_sheet_robust(self, excel_file, sheet_name):
        """
        Đọc sheet với nhiều phương pháp để đảm bảo thành công
        """
        try:
            # Thử đọc với header mặc định
            df = pd.read_excel(excel_file, sheet_name=sheet_name, header=0)

            # Kiểm tra xem có phải header thực sự không
            if self._is_valid_header(df):
                return df

            # Nếu không, thử tìm header thực sự
            for header_row in range(0, min(10, len(df))):
                try:
                    df_test = pd.read_excel(excel_file, sheet_name=sheet_name, header=header_row)
                    if self._is_valid_header(df_test):
                        print(f"   🎯 Tìm thấy header thực tế ở dòng {header_row}")
                        return df_test
                except:
                    continue

            # Nếu vẫn không tìm được, sử dụng phương pháp cuối cùng
            df = pd.read_excel(excel_file, sheet_name=sheet_name, header=None)
            return df

        except Exception as e:
            print(f"   ❌ Lỗi khi đọc sheet {sheet_name}: {e}")
            return None

    def _is_valid_header(self, df):
        """
        Kiểm tra xem header có hợp lệ không
        """
        if df.empty or len(df.columns) < 2:
            return False

        # Tìm các từ khóa quan trọng trong header
        header_keywords = ['Date', 'Time', 'VoLTE', 'CSSR', 'CDR', 'Traffic',
                           'SRVCC', 'SR', 'HOSR', 'GB', '%', 'Rate']

        header_str = ' '.join([str(col) for col in df.columns])

        # Kiểm tra có ít nhất 2 từ khóa
        keyword_count = sum(1 for keyword in header_keywords if keyword in header_str)

        return keyword_count >= 2

    def clean_dataframe_enhanced(self, df, sheet_name):
        """
        Làm sạch dataframe với xử lý nâng cao và chi tiết hơn
        """
        print(f"🧹 Làm sạch dữ liệu nâng cao cho {sheet_name}...")
        print(f"   📊 Trước khi làm sạch: {df.shape}")

        if df.empty:
            print("   ❌ DataFrame rỗng!")
            return None

        # 1. Xử lý tên cột
        df = self._clean_column_names(df)

        # 2. Tìm và thiết lập header đúng
        df = self._fix_header_row(df, sheet_name)

        if df is None or df.empty:
            return None

        # 3. Xóa các cột và hàng không cần thiết
        df = self._remove_unnecessary_data(df)

        # 4. Xử lý cột Date/Time
        df = self._process_datetime_column(df)

        # 5. Chuyển đổi các cột số
        df = self._convert_numeric_columns(df)

        # 6. Làm sạch dữ liệu cuối cùng
        df = self._final_cleanup(df)

        if df is None or df.empty:
            print(f"   ❌ Không có dữ liệu hợp lệ sau khi làm sạch!")
            return None

        print(f"   ✨ Sau khi làm sạch: {df.shape}")
        print(f"   📋 Các cột cuối cùng: {list(df.columns[:10])}")

        return df

    def _clean_column_names(self, df):
        """
        Làm sạch tên cột
        """
        df.columns = df.columns.astype(str)
        df.columns = [col.strip().replace('\n', ' ').replace('\r', ' ').replace('  ', ' ')
                      for col in df.columns]
        return df

    def _fix_header_row(self, df, sheet_name):
        """
        Tìm và sửa dòng header đúng
        """
        # Tìm dòng chứa từ khóa quan trọng
        header_keywords = ['Date', 'Time', 'VoLTE', 'CSSR', 'CDR', 'Traffic', 'SRVCC']

        for i in range(min(5, len(df))):
            row_str = ' '.join([str(val) for val in df.iloc[i].values if pd.notna(val)])
            keyword_count = sum(1 for keyword in header_keywords if keyword in row_str)

            if keyword_count >= 2:  # Ít nhất 2 từ khóa
                print(f"   🎯 Tìm thấy header thực tế ở dòng {i}")

                # Tạo header mới
                new_header = []
                for val in df.iloc[i].values:
                    if pd.notna(val) and str(val).strip() != '':
                        new_header.append(str(val).strip())
                    else:
                        new_header.append(f'Col_{len(new_header)}')

                # Tạo DataFrame mới
                data_rows = df.iloc[i + 1:].values
                if len(data_rows) == 0:
                    return None

                # Đảm bảo số cột khớp
                min_cols = min(len(new_header), data_rows.shape[1] if len(data_rows) > 0 else 0)
                if min_cols == 0:
                    return None

                new_header = new_header[:min_cols]
                data_rows = data_rows[:, :min_cols]

                df_new = pd.DataFrame(data_rows, columns=new_header)
                return df_new

        return df  # Trả về DataFrame gốc nếu không tìm thấy header tốt hơn

    def _remove_unnecessary_data(self, df):
        """
        Xóa các cột và hàng không cần thiết
        """
        # Xóa các cột Unnamed
        unnamed_cols = [col for col in df.columns if 'Unnamed' in str(col) or 'Col_' in str(col)]
        if unnamed_cols:
            df = df.drop(columns=unnamed_cols, errors='ignore')
            print(f"   🗑️ Đã xóa {len(unnamed_cols)} cột không tên")

        # Xóa các cột hoàn toàn trống
        df = df.dropna(axis=1, how='all')

        # Xóa các hàng hoàn toàn trống
        df = df.dropna(axis=0, how='all')

        # Xóa các hàng có quá ít dữ liệu
        threshold = max(2, len(df.columns) * 0.3)  # Ít nhất 30% cột có dữ liệu
        df = df.dropna(thresh=threshold)

        return df.reset_index(drop=True)

    def _process_datetime_column(self, df):
        """
        Xử lý cột Date/Time
        """
        if len(df.columns) == 0 or len(df) == 0:
            return df

        # Tìm cột Date
        date_col = None
        for col in df.columns[:3]:  # Kiểm tra 3 cột đầu
            col_str = str(col).lower()
            if any(keyword in col_str for keyword in ['date', 'time', 'ngày', 'giờ']):
                date_col = col
                break

        if date_col is None:
            date_col = df.columns[0]  # Mặc định cột đầu tiên

        print(f"   📅 Xử lý cột thời gian: {date_col}")

        try:
            # Thử các phương pháp chuyển đổi khác nhau
            original_data = df[date_col].copy()

            # Phương pháp 1: Chuyển đổi trực tiếp
            df[date_col] = pd.to_datetime(df[date_col], errors='coerce')

            # Nếu có quá nhiều NaT, thử phương pháp khác
            nat_count = df[date_col].isna().sum()
            if nat_count > len(df) * 0.5:  # Hơn 50% là NaT
                print(f"   ⚠️ Quá nhiều ngày không hợp lệ, thử phương pháp khác...")

                # Phương pháp 2: Xử lý số Excel
                try:
                    df[date_col] = pd.to_datetime(original_data, origin='1899-12-30', unit='D', errors='coerce')
                    nat_count = df[date_col].isna().sum()
                except:
                    pass

                # Phương pháp 3: Parsing linh hoạt
                if nat_count > len(df) * 0.5:
                    try:
                        df[date_col] = pd.to_datetime(original_data, infer_datetime_format=True, errors='coerce')
                    except:
                        pass

            # Loại bỏ các hàng có ngày không hợp lệ
            valid_dates = df[date_col].notna()
            df = df[valid_dates].reset_index(drop=True)

            # Sắp xếp theo ngày
            if len(df) > 0:
                df = df.sort_values(by=date_col).reset_index(drop=True)
                print(f"   ✅ Đã chuyển đổi {len(df)} ngày hợp lệ")

        except Exception as e:
            print(f"   ⚠️ Lỗi xử lý ngày tháng: {e}")

        return df

    def _convert_numeric_columns(self, df):
        """
        Chuyển đổi các cột số
        """
        numeric_converted = 0

        # Bỏ qua cột đầu tiên (Date/Time)
        for col in df.columns[1:]:
            try:
                original_count = df[col].count()

                # Xử lý các ký tự đặc biệt trong số
                if df[col].dtype == 'object':
                    df[col] = df[col].astype(str).str.replace(',', '').str.replace('%', '').str.replace(' ', '')
                    df[col] = df[col].replace(['', 'nan', 'NaN', 'null', 'NULL', '-'], np.nan)

                # Chuyển đổi sang số
                df[col] = pd.to_numeric(df[col], errors='coerce')

                new_count = df[col].count()

                if new_count > 0:
                    numeric_converted += 1
                    if new_count < original_count:
                        lost_pct = (original_count - new_count) / original_count * 100
                        if lost_pct > 20:  # Cảnh báo nếu mất quá 20% dữ liệu
                            print(f"   ⚠️ {col}: mất {lost_pct:.1f}% dữ liệu ({original_count} -> {new_count})")

            except Exception as e:
                print(f"   ⚠️ Lỗi chuyển đổi cột {col}: {e}")
                continue

        print(f"   🔢 Đã chuyển đổi {numeric_converted} cột sang kiểu số")
        return df

    def _final_cleanup(self, df):
        """
        Làm sạch cuối cùng
        """
        # Xóa các hàng có quá ít dữ liệu
        min_valid_cols = max(2, len(df.columns) * 0.4)  # Ít nhất 40% cột có dữ liệu
        df = df.dropna(thresh=min_valid_cols)

        # Xóa các cột có quá ít dữ liệu
        min_valid_rows = max(1, len(df) * 0.1)  # Ít nhất 10% hàng có dữ liệu
        df = df.dropna(axis=1, thresh=min_valid_rows)

        return df.reset_index(drop=True)

    def save_to_csv(self, dataframes, output_dir="output_charts"):
        """
        Lưu các DataFrame thành file CSV
        """
        print(f"\n💾 Lưu dữ liệu thành CSV...")
        os.makedirs(output_dir, exist_ok=True)

        for sheet_name, df in dataframes.items():
            # Tạo tên file CSV
            if 'Daily' in sheet_name or 'daily' in sheet_name.lower():
                csv_filename = 'Net_KPI_Daily.csv'
            elif 'Hourly' in sheet_name or 'hourly' in sheet_name.lower() or 'hour' in sheet_name.lower():
                csv_filename = 'Net_KPI_Hourly.csv'
            else:
                safe_name = "".join(c for c in sheet_name if c.isalnum() or c in (' ', '-', '_')).replace(' ', '_')
                csv_filename = f'{safe_name}.csv'

            csv_path = os.path.join(output_dir, csv_filename)

            try:
                df.to_csv(csv_path, index=False, encoding='utf-8-sig')
                print(f"✅ Đã lưu: {csv_filename} ({df.shape[0]} hàng × {df.shape[1]} cột)")

                # Lưu thông tin để tạo biểu đồ sau
                self.csv_files[sheet_name] = csv_path
                self.cleaned_data[sheet_name] = df

            except Exception as e:
                print(f"❌ Lỗi khi lưu {csv_filename}: {e}")

        return self.csv_files

    def create_charts_from_csv(self, output_dir="output_charts"):
        """
        Tạo biểu đồ từ các file CSV
        """
        print(f"\n🎨 Tạo biểu đồ từ dữ liệu CSV...")

        for sheet_name, csv_path in self.csv_files.items():
            # Xác định loại biểu đồ
            if 'Daily' in sheet_name or 'daily' in sheet_name.lower():
                chart_folder = os.path.join(output_dir, "Chart_daily")
                data_type = "Daily"
            elif 'Hourly' in sheet_name or 'hourly' in sheet_name.lower():
                chart_folder = os.path.join(output_dir, "Chart_hourly")
                data_type = "Hourly"
            else:
                chart_folder = os.path.join(output_dir, "Charts")
                data_type = "General"

            # Tạo biểu đồ
            self._generate_charts_for_data(csv_path, chart_folder, data_type)

    def _generate_charts_for_data(self, csv_file, chart_folder, data_type):
        """
        Tạo biểu đồ cho một file CSV cụ thể
        """
        print(f"\n📊 Tạo biểu đồ {data_type}...")

        if not os.path.exists(csv_file):
            print(f"   ❌ Không tìm thấy file: {csv_file}")
            return

        os.makedirs(chart_folder, exist_ok=True)

        try:
            # Đọc dữ liệu
            df = pd.read_csv(csv_file)
            print(f"   📊 Đọc dữ liệu: {df.shape}")

            if df.empty or len(df.columns) < 2:
                print(f"   ⚠️ Dữ liệu không đủ để tạo biểu đồ")
                return

            # Cột thời gian (cột đầu tiên)
            x_column = df.columns[0]
            print(f"   📅 Cột thời gian: {x_column}")

            # Chuyển đổi cột thời gian
            try:
                df[x_column] = pd.to_datetime(df[x_column])
            except:
                print(f"   ⚠️ Không thể chuyển đổi cột thời gian")

            # Lọc các cột số hợp lệ
            numeric_columns = []
            for col in df.columns[1:]:
                if pd.api.types.is_numeric_dtype(df[col]) and df[col].count() > 0:
                    # Kiểm tra có đủ dữ liệu không (ít nhất 20% không phải NaN)
                    valid_ratio = df[col].count() / len(df)
                    if valid_ratio >= 0.2:
                        numeric_columns.append(col)

            print(f"   📈 Tìm thấy {len(numeric_columns)} cột dữ liệu hợp lệ")

            if not numeric_columns:
                print(f"   ❌ Không có cột dữ liệu hợp lệ!")
                return

            chart_count = 0

            # 1. Tạo biểu đồ đường cho từng KPI
            print(f"   📊 Tạo biểu đồ đường riêng lẻ...")
            for col_name in numeric_columns:
                try:
                    chart_path = self._create_line_chart(df, x_column, col_name, chart_folder)
                    if chart_path:
                        chart_count += 1
                except Exception as e:
                    print(f"   ❌ Lỗi tạo biểu đồ đường {col_name}: {e}")

            # 2. Tạo biểu đồ kết hợp (đường + cột)
            print(f"   📊 Tạo biểu đồ kết hợp...")
            for i in range(0, len(numeric_columns) - 1, 2):
                try:
                    col1 = numeric_columns[i]
                    col2 = numeric_columns[i + 1] if i + 1 < len(numeric_columns) else None

                    if col2 and col1 != col2:
                        chart_path = self._create_combo_chart(df, x_column, col1, col2, chart_folder)
                        if chart_path:
                            chart_count += 1
                except Exception as e:
                    print(f"   ❌ Lỗi tạo biểu đồ kết hợp: {e}")

            print(f"   🎉 Đã tạo {chart_count} biểu đồ cho {data_type}")

        except Exception as e:
            print(f"   ❌ Lỗi tạo biểu đồ {data_type}: {e}")

    def _create_line_chart(self, df, x_col, y_col, chart_folder):
        """
        Tạo biểu đồ đường cho một KPI
        """
        try:
            plt.figure(figsize=(12, 6))

            # Lọc dữ liệu hợp lệ
            clean_data = df[[x_col, y_col]].dropna()
            if clean_data.empty:
                plt.close()
                return None

            # Vẽ biểu đồ
            plt.plot(clean_data[x_col], clean_data[y_col],
                     marker='o', linewidth=2.5, markersize=4,
                     color='#1f77b4', alpha=0.8, label=y_col)

            # Định dạng biểu đồ
            plt.title(f'{y_col} Trend Analysis', fontsize=14, fontweight='bold', pad=20)
            plt.xlabel('Date/Time', fontsize=12)
            plt.ylabel(y_col, fontsize=12)
            plt.grid(True, alpha=0.3, linestyle='--')
            plt.legend(fontsize=11, loc='best')

            # Định dạng trục x cho datetime
            if pd.api.types.is_datetime64_any_dtype(clean_data[x_col]):
                plt.gca().xaxis.set_major_formatter(mdates.DateFormatter('%m/%d'))
                plt.gca().xaxis.set_major_locator(mdates.DayLocator(interval=max(1, len(clean_data) // 10)))

            plt.xticks(rotation=45, fontsize=10)
            plt.yticks(fontsize=10)

            # Màu nền
            plt.gca().set_facecolor('#f8f9fa')

            plt.tight_layout()

            # Lưu biểu đồ
            safe_filename = "".join(c for c in y_col if c.isalnum() or c in (' ', '-', '_')).replace(' ', '_')
            chart_path = os.path.join(chart_folder, f"{safe_filename}_line.png")
            plt.savefig(chart_path, dpi=300, bbox_inches='tight', facecolor='white')
            plt.close()

            return chart_path

        except Exception as e:
            plt.close()
            return None

    def _create_combo_chart(self, df, x_col, y_line, y_bar, chart_folder):
        """
        Tạo biểu đồ kết hợp đường và cột
        """
        try:
            # Lọc dữ liệu hợp lệ
            clean_data = df[[x_col, y_line, y_bar]].dropna()
            if clean_data.empty:
                return None

            fig, ax1 = plt.subplots(figsize=(12, 6))

            # Trục Y bên trái (đường)
            color_line = '#1f77b4'
            ax1.set_xlabel('Date/Time', fontsize=12)
            ax1.set_ylabel(y_line, color=color_line, fontsize=12, fontweight='bold')
            ax1.plot(clean_data[x_col], clean_data[y_line],
                     marker='o', color=color_line, linewidth=2.5, markersize=4,
                     label=y_line, alpha=0.8)
            ax1.tick_params(axis='y', labelcolor=color_line, labelsize=10)
            ax1.tick_params(axis='x', labelsize=10)
            ax1.grid(True, alpha=0.3, linestyle='--')

            # Trục Y bên phải (cột)
            ax2 = ax1.twinx()
            color_bar = '#ff7f0e'
            ax2.set_ylabel(y_bar, color=color_bar, fontsize=12, fontweight='bold')

            # Tính độ rộng cột
            bar_width = 0.6 if len(clean_data) > 15 else 0.8

            ax2.bar(clean_data[x_col], clean_data[y_bar],
                    alpha=0.6, color=color_bar, label=y_bar, width=bar_width)
            ax2.tick_params(axis='y', labelcolor=color_bar, labelsize=10)

            # Tiêu đề
            plt.title(f'{y_line} & {y_bar} Combined Analysis',
                      fontsize=14, fontweight='bold', pad=20)

            # Định dạng trục x
            if pd.api.types.is_datetime64_any_dtype(clean_data[x_col]):
                fig.autofmt_xdate()
                ax1.xaxis.set_major_formatter(mdates.DateFormatter('%m/%d'))
            else:
                plt.xticks(rotation=45)

            # Legend kết hợp
            lines1, labels1 = ax1.get_legend_handles_labels()
            lines2, labels2 = ax2.get_legend_handles_labels()
            ax1.legend(lines1 + lines2, labels1 + labels2, loc='upper left', fontsize=10)

            # Màu nền
            ax1.set_facecolor('#f8f9fa')

            fig.tight_layout()

            # Lưu biểu đồ
            safe_filename1 = "".join(c for c in y_line if c.isalnum() or c in (' ', '-', '_')).replace(' ', '_')
            safe_filename2 = "".join(c for c in y_bar if c.isalnum() or c in (' ', '-', '_')).replace(' ', '_')
            chart_path = os.path.join(chart_folder, f"{safe_filename1}_and_{safe_filename2}_combo.png")
            plt.savefig(chart_path, dpi=300, bbox_inches='tight', facecolor='white')
            plt.close()

            return chart_path

        except Exception as e:
            plt.close()
            return None

    def create_comprehensive_report(self, output_dir="output_charts"):
        """
        Tạo báo cáo tổng hợp CHÍNH - không có dashboard, chỉ biểu đồ
        """
        print(f"\n📋 Tạo báo cáo tổng hợp chỉ biểu đồ...")

        try:
            # Thu thập tất cả file ảnh biểu đồ (không bao gồm dashboard)
            image_files = []

            # Biểu đồ Daily
            daily_chart_dir = os.path.join(output_dir, "Chart_daily")
            if os.path.exists(daily_chart_dir):
                for file in sorted(os.listdir(daily_chart_dir)):
                    if file.endswith('.png'):
                        image_files.append(os.path.join(daily_chart_dir, file))

            # Biểu đồ Hourly
            hourly_chart_dir = os.path.join(output_dir, "Chart_hourly")
            if os.path.exists(hourly_chart_dir):
                for file in sorted(os.listdir(hourly_chart_dir)):
                    if file.endswith('.png'):
                        image_files.append(os.path.join(hourly_chart_dir, file))

            if not image_files:
                print("   ❌ Không tìm thấy file ảnh nào để tạo báo cáo")
                return None

            print(f"   📊 Tìm thấy {len(image_files)} biểu đồ")

            # Tạo báo cáo tổng hợp 1 trang duy nhất
            report_path = self._create_single_page_report(image_files, output_dir)

            if report_path:
                print(f"✅ Đã tạo báo cáo tổng hợp: {report_path}")

                # Tạo PDF từ PNG
                try:
                    pdf_path = report_path.replace('.png', '.pdf')
                    img = Image.open(report_path)
                    img.save(pdf_path, "PDF", quality=95)
                    print(f"✅ Đã tạo báo cáo PDF: {pdf_path}")
                except Exception as e:
                    print(f"⚠️ Không thể tạo PDF: {e}")

            return report_path

        except Exception as e:
            print(f"❌ Lỗi tạo báo cáo tổng hợp: {e}")
            return None

    def _create_single_page_report(self, image_files, output_dir):
        """
        Tạo báo cáo 1 trang duy nhất với layout tối ưu
        """
        try:
            if not image_files:
                return None

            # Đọc tất cả ảnh
            images = []
            for img_path in image_files:
                try:
                    img = Image.open(img_path)
                    images.append((img, os.path.basename(img_path)))
                except Exception as e:
                    print(f"   ⚠️ Không thể đọc {img_path}: {e}")

            if not images:
                return None

            # Cấu hình layout cho 1 trang A4 (tỉ lệ 210:297)
            page_width = 2100  # pixels (độ phân giải cao)
            page_height = 2970  # pixels (tỉ lệ A4)

            # Cấu hình layout
            margin = 60
            header_height = 120
            spacing = 40

            # Tính toán số biểu đồ trên mỗi hàng và cột để vừa 1 trang
            total_charts = len(images)

            if total_charts <= 2:
                cols, rows = 1, total_charts
            elif total_charts <= 4:
                cols, rows = 2, 2
            elif total_charts <= 6:
                cols, rows = 2, 3
            elif total_charts <= 9:
                cols, rows = 3, 3
            else:
                # Nếu quá nhiều biểu đồ, chỉ lấy 9 biểu đồ đầu tiên
                cols, rows = 3, 3
                images = images[:9]
                total_charts = 9
                print(f"   ⚠️ Quá nhiều biểu đồ, chỉ hiển thị {total_charts} biểu đồ đầu tiên")

            # Tính kích thước biểu đồ
            available_width = page_width - 2 * margin - (cols - 1) * spacing
            available_height = page_height - header_height - 2 * margin - (rows - 1) * spacing

            chart_width = available_width // cols
            chart_height = available_height // rows

            # Tạo canvas
            report_img = Image.new('RGB', (page_width, page_height), 'white')
            draw = ImageDraw.Draw(report_img)

            # Header
            try:
                title_font = ImageFont.truetype("arial.ttf", 48)
                subtitle_font = ImageFont.truetype("arial.ttf", 24)
            except:
                title_font = ImageFont.load_default()
                subtitle_font = ImageFont.load_default()

            # Tiêu đề chính
            title = "VoLTE KPI ANALYSIS REPORT"
            title_bbox = draw.textbbox((0, 0), title, font=title_font)
            title_width = title_bbox[2] - title_bbox[0]
            draw.text(((page_width - title_width) // 2, margin), title,
                      fill='navy', font=title_font)

            # Phụ đề
            subtitle = f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M')} | Charts: {total_charts}"
            subtitle_bbox = draw.textbbox((0, 0), subtitle, font=subtitle_font)
            subtitle_width = subtitle_bbox[2] - subtitle_bbox[0]
            draw.text(((page_width - subtitle_width) // 2, margin + 60), subtitle,
                      fill='gray', font=subtitle_font)

            # Vẽ đường phân cách
            line_y = header_height + margin - 10
            draw.line([(margin, line_y), (page_width - margin, line_y)], fill='lightgray', width=2)

            # Vẽ các biểu đồ
            start_y = header_height + margin + 20

            for i, (chart_img, filename) in enumerate(images):
                row = i // cols
                col = i % cols

                x = margin + col * (chart_width + spacing)
                y = start_y + row * (chart_height + spacing)

                # Resize biểu đồ giữ nguyên tỉ lệ
                chart_resized = self._resize_image_proportional(chart_img, chart_width, chart_height)

                # Căn giữa biểu đồ trong ô
                chart_w, chart_h = chart_resized.size
                center_x = x + (chart_width - chart_w) // 2
                center_y = y + (chart_height - chart_h) // 2

                report_img.paste(chart_resized, (center_x, center_y))

                # Thêm border nhẹ quanh biểu đồ
                border_rect = [center_x - 2, center_y - 2,
                               center_x + chart_w + 2, center_y + chart_h + 2]
                draw.rectangle(border_rect, outline='lightgray', width=1)

            # Footer
            footer_text = f"Total KPIs Analyzed: {total_charts} | Report Format: Single Page Summary"
            footer_bbox = draw.textbbox((0, 0), footer_text, font=subtitle_font)
            footer_width = footer_bbox[2] - footer_bbox[0]
            draw.text(((page_width - footer_width) // 2, page_height - 60), footer_text,
                      fill='gray', font=subtitle_font)

            # Lưu báo cáo
            report_path = os.path.join(output_dir, "VoLTE_KPI_Single_Page_Report.png")
            report_img.save(report_path, "PNG", quality=95, dpi=(300, 300))

            return report_path

        except Exception as e:
            print(f"❌ Lỗi tạo báo cáo 1 trang: {e}")
            return None

    def _resize_image_proportional(self, img, max_width, max_height):
        """
        Resize ảnh giữ nguyên tỉ lệ và fit vào kích thước cho phép
        """
        original_width, original_height = img.size

        # Tính tỉ lệ resize
        ratio_w = max_width / original_width
        ratio_h = max_height / original_height
        ratio = min(ratio_w, ratio_h)  # Chọn tỉ lệ nhỏ hơn để đảm bảo fit

        # Tính kích thước mới
        new_width = int(original_width * ratio)
        new_height = int(original_height * ratio)

        return img.resize((new_width, new_height), Image.Resampling.LANCZOS)

    def process_complete_workflow(self, excel_path, output_dir="output_charts"):
        """
        Thực hiện quy trình hoàn chỉnh từ Excel đến báo cáo (KHÔNG có dashboard)
        """
        print(f"\n🎯 BẮT ĐẦU QUY TRÌNH XỬ LÝ HOÀN CHỈNH")
        print(f"📁 File đầu vào: {excel_path}")
        print(f"📁 Thư mục đầu ra: {output_dir}")
        print("=" * 70)

        # Bước 1: Đọc Excel
        print("\n📖 BƯỚC 1: ĐỌC VÀ PHÂN TÍCH FILE EXCEL")
        dataframes = self.read_excel_file(excel_path)

        if not dataframes:
            print("❌ Không thể đọc file Excel!")
            return False

        # Bước 2: Làm sạch dữ liệu
        print("\n🧹 BƯỚC 2: LÀM SẠCH DỮ LIỆU")
        cleaned_dataframes = {}

        for sheet_name, df in dataframes.items():
            cleaned_df = self.clean_dataframe_enhanced(df, sheet_name)
            if cleaned_df is not None and not cleaned_df.empty:
                cleaned_dataframes[sheet_name] = cleaned_df
            else:
                print(f"❌ Không thể làm sạch dữ liệu từ {sheet_name}")

        if not cleaned_dataframes:
            print("❌ Không có dữ liệu hợp lệ sau khi làm sạch!")
            return False

        # Bước 3: Lưu CSV
        print("\n💾 BƯỚC 3: LƯU DỮ LIỆU THÀNH CSV")
        csv_files = self.save_to_csv(cleaned_dataframes, output_dir)

        if not csv_files:
            print("❌ Không thể lưu file CSV!")
            return False

        # Bước 4: Tạo biểu đồ
        print("\n🎨 BƯỚC 4: TẠO BIỂU ĐỒ")
        self.create_charts_from_csv(output_dir)

        # Bước 5: Tạo báo cáo tổng hợp (KHÔNG có dashboard)
        print("\n📋 BƯỚC 5: TẠO BÁO CÁO TỔNG HỢP")
        report_path = self.create_comprehensive_report(output_dir)

        # Tổng kết
        print("\n" + "=" * 70)
        print("🎉 HOÀN TẤT QUY TRÌNH XỬ LÝ!")
        print("=" * 70)
        print(f"📁 Kết quả lưu tại: {output_dir}")
        print("\n📊 Cấu trúc kết quả:")
        print("📂 output_charts/")

        for sheet_name, csv_path in csv_files.items():
            print(f"   📄 {os.path.basename(csv_path)}")

        chart_folders = ['Chart_daily', 'Chart_hourly']
        for folder in chart_folders:
            folder_path = os.path.join(output_dir, folder)
            if os.path.exists(folder_path):
                chart_count = len([f for f in os.listdir(folder_path) if f.endswith('.png')])
                print(f"   📂 {folder}/ ({chart_count} biểu đồ)")

        if report_path and os.path.exists(report_path):
            print(f"   📊 VoLTE_KPI_Single_Page_Report.png")

        pdf_path = os.path.join(output_dir, "VoLTE_KPI_Single_Page_Report.pdf")
        if os.path.exists(pdf_path):
            print(f"   📊 VoLTE_KPI_Single_Page_Report.pdf")

        print("=" * 70)
        return True


def main():
    """
    Hàm main để chạy chương trình
    """
    print("🚀 VOLTE KPI DATA PROCESSING SYSTEM")
    print("=" * 70)
    print("📋 Chức năng:")
    print("   ✅ Chuyển đổi Excel sang CSV (chỉ 2 sheet: Net KPI_Daily, Net KPI_Hourly)")
    print("   ✅ Làm sạch dữ liệu chuyên sâu")
    print("   ✅ Tạo biểu đồ đường và biểu đồ kết hợp")
    print("   ✅ Tạo báo cáo tổng hợp 1 trang PNG/PDF (KHÔNG có dashboard)")
    print("=" * 70)

    # Khởi tạo processor
    processor = VoLTEKPIProcessor()

    # Đường dẫn file Excel (thay đổi theo file thực tế của bạn)
    excel_file = "4G_KPI Cell VoLTE_20250807.xlsx"

    # Kiểm tra file tồn tại
    if not os.path.exists(excel_file):
        print(f"❌ Không tìm thấy file: {excel_file}")
        print("💡 Hãy đảm bảo file Excel ở cùng thư mục với script này")
        print("💡 Hoặc thay đổi đường dẫn trong biến excel_file")
        return

    # Chạy quy trình hoàn chỉnh
    success = processor.process_complete_workflow(excel_file)

    if success:
        print("\n🎊 THÀNH CÔNG! Hãy kiểm tra thư mục 'output_charts'")
        print("📊 Báo cáo tổng hợp được lưu dạng PNG và PDF 1 trang duy nhất")
    else:
        print("\n❌ CÓ LỖI XẢY RA! Vui lòng kiểm tra lại dữ liệu đầu vào")


# Utility function để fix file CSV bị lỗi (nếu cần)
def fix_csv_file(input_csv, output_csv):
    """
    Hàm tiện ích để sửa file CSV bị lỗi
    """
    processor = VoLTEKPIProcessor()

    try:
        print(f"🔧 Đang sửa file CSV: {input_csv}")

        # Đọc file với header=None
        df = pd.read_csv(input_csv, header=None)

        # Sử dụng hàm làm sạch của processor
        df_cleaned = processor.clean_dataframe_enhanced(df, "CSV_Fix")

        if df_cleaned is not None:
            df_cleaned.to_csv(output_csv, index=False, encoding='utf-8-sig')
            print(f"✅ Đã sửa và lưu: {output_csv}")
            return True
        else:
            print("❌ Không thể sửa file CSV")
            return False

    except Exception as e:
        print(f"❌ Lỗi khi sửa file: {e}")
        return False


if __name__ == "__main__":
    # Kiểm tra các thư viện cần thiết
    required_packages = {
        'pandas': 'pandas',
        'matplotlib': 'matplotlib',
        'numpy': 'numpy',
        'PIL': 'Pillow',
        'openpyxl': 'openpyxl'
    }

    print("📦 Kiểm tra thư viện cần thiết:")
    missing_packages = []

    for package, install_name in required_packages.items():
        try:
            __import__(package)
            print(f"   ✅ {package}")
        except ImportError:
            print(f"   ❌ {package} - Cần cài đặt: pip install {install_name}")
            missing_packages.append(install_name)

    if missing_packages:
        print(f"\n⚠️ Vui lòng cài đặt các package còn thiếu:")
        print(f"pip install {' '.join(missing_packages)}")
        exit()

    print("\n")

    # Chạy chương trình chính
    main()