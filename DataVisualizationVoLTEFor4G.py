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

    def create_dashboard_report(self, output_dir="output_charts"):
        """
        Tạo báo cáo dashboard tổng hợp
        """
        print(f"\n📋 Tạo báo cáo dashboard tổng hợp...")

        if len(self.csv_files) < 2:
            print("   ⚠️ Cần ít nhất 2 file CSV để tạo dashboard so sánh")
            return None

        try:
            # Tìm file Daily và Hourly
            daily_csv = None
            hourly_csv = None

            for sheet_name, csv_path in self.csv_files.items():
                if 'Daily' in sheet_name or 'daily' in sheet_name.lower():
                    daily_csv = csv_path
                elif 'Hourly' in sheet_name or 'hourly' in sheet_name.lower():
                    hourly_csv = csv_path

            if not daily_csv or not hourly_csv:
                print("   ❌ Không tìm thấy cả file Daily và Hourly")
                return None

            # Tạo dashboard table
            dashboard_path = self._create_kpi_dashboard_table(daily_csv, hourly_csv, output_dir)

            if dashboard_path:
                # Tạo comprehensive report
                self._create_comprehensive_report(output_dir)

            return dashboard_path

        except Exception as e:
            print(f"   ❌ Lỗi tạo dashboard: {e}")
            return None

    def _create_kpi_dashboard_table(self, csv_daily, csv_hourly, output_dir):
        """
        Tạo bảng dashboard KPI theo phong cách như DataVisualizationFor4G_V2.py
        """
        try:
            print("📊 Đang tạo bảng KPI Dashboard...")

            # Đọc dữ liệu
            df_daily = pd.read_csv(csv_daily)
            df_hourly = pd.read_csv(csv_hourly)

            # Chuyển đổi cột Date
            date_col = df_daily.columns[0]
            df_daily[date_col] = pd.to_datetime(df_daily[date_col])
            df_hourly[date_col] = pd.to_datetime(df_hourly[date_col])

            # KPI mapping cho VoLTE
            kpi_mapping = {
                # VoLTE Success Rates
                'VoLTE CSSR': ['VoLTE CSSR', 'Call Setup Success Rate', 'CSSR'],
                'VoLTE CDR': ['VoLTE CDR', 'Call Drop Rate', 'CDR'],
                'SRVCC SR': ['SRVCC SR', 'SRVCC Success Rate'],
                'VoLTE Traffic': ['VoLTE Traffic', 'Traffic', 'Call Volume'],

                # Handover Rates
                'Intra HO SR': ['Intra HO SR', 'IntraF HOSR', 'Intra Handover'],
                'Inter HO SR': ['Inter HO SR', 'InterF HOSR', 'Inter Handover'],
                'SRVCC HO SR': ['SRVCC HO SR', 'SRVCC Handover'],
                'Voice Quality': ['Voice Quality', 'MOS', 'Quality Score']
            }

            # Tạo figure với 2 subplots
            fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(16, 14))
            fig.suptitle('Daily VoLTE KPI Dashboard', fontsize=18, fontweight='bold', y=0.98)

            # Lấy các ngày gần nhất
            latest = df_daily[date_col].max()
            prev = df_daily[df_daily[date_col] < latest][date_col].max() if pd.notna(latest) else pd.NaT
            week_candidate = latest - timedelta(days=7) if pd.notna(latest) else pd.NaT
            week_date = df_daily[df_daily[date_col] <= week_candidate][date_col].max() if pd.notna(
                week_candidate) else pd.NaT

            latest_dates = []
            for date in [latest, prev, week_date]:
                if pd.notna(date) and date not in latest_dates:
                    latest_dates.append(date)

            # Tạo dashboard cho Daily
            self._create_dashboard_subplot(ax1, df_daily, latest_dates, date_col, kpi_mapping,
                                           "Daily VoLTE KPI Dashboard (24h)", "#FF6B35")

            # Tạo dashboard cho Hourly
            self._create_dashboard_subplot(ax2, df_hourly, latest_dates, date_col, kpi_mapping,
                                           "Daily VoLTE KPI Dashboard (Peak Hours)", "#FFA500")

            plt.tight_layout()

            # Lưu dashboard
            dashboard_path = os.path.join(output_dir, "VoLTE_KPI_Dashboard.png")
            plt.savefig(dashboard_path, dpi=300, bbox_inches='tight', facecolor='white')
            plt.close()

            print(f"✅ Đã tạo VoLTE KPI Dashboard: {dashboard_path}")
            return dashboard_path

        except Exception as e:
            print(f"❌ Lỗi khi tạo KPI Dashboard: {e}")
            return None

    def _create_dashboard_subplot(self, ax, df, latest_dates, date_col, kpi_mapping, title, header_color):
        """
        Tạo một subplot dashboard
        """
        ax.clear()
        ax.set_xlim(0, 12)
        ax.set_ylim(0, 10)
        ax.axis('off')

        # Tiêu đề
        ax.text(6, 9.5, title, ha='center', va='center', fontsize=14, fontweight='bold')

        # Tìm các KPI có sẵn trong dữ liệu
        available_kpis = []
        for kpi_name, possible_cols in kpi_mapping.items():
            for col_name in possible_cols:
                if col_name in df.columns:
                    available_kpis.append((kpi_name, col_name))
                    break

        if not available_kpis:
            ax.text(6, 5, 'No KPI data available', ha='center', va='center', fontsize=12)
            return

        # Tạo bảng với KPI có sẵn (tối đa 4 KPI)
        display_kpis = available_kpis[:4]
        header = ['Date'] + [kpi[0] for kpi in display_kpis]

        # Chuẩn bị dữ liệu bảng
        table_data = []
        date_rows_data = []

        for date in latest_dates:
            date_str = date.strftime('%d-%b-%y')
            row_data = [date_str]

            for kpi_name, col_name in display_kpis:
                day_data = df[df[date_col].dt.date == date.date()]
                if not day_data.empty and col_name in df.columns:
                    val = day_data[col_name].iloc[0]
                    if pd.notna(val) and str(val).strip() != '':
                        try:
                            row_data.append(f"{float(val):.2f}")
                        except:
                            row_data.append('-')
                    else:
                        row_data.append('-')
                else:
                    row_data.append('-')

            table_data.append(row_data)
            date_rows_data.append(row_data)

        # Thêm hàng so sánh nếu có đủ dữ liệu
        if len(date_rows_data) >= 2:
            comp_d1 = ['Compare (D-1)']
            for j in range(1, len(header)):
                try:
                    curr_str = date_rows_data[0][j]
                    prev_str = date_rows_data[1][j]

                    if curr_str != '-' and prev_str != '-':
                        curr_val = float(curr_str)
                        prev_val = float(prev_str)
                        diff = curr_val - prev_val
                        comp_d1.append(f"{diff:+.2f}")
                    else:
                        comp_d1.append('-')
                except:
                    comp_d1.append('-')
            table_data.append(comp_d1)

        # Vẽ bảng
        self._draw_dashboard_table(ax, header, table_data, header_color)

    def _draw_dashboard_table(self, ax, header, data, header_color):
        """
        Vẽ bảng dashboard
        """
        num_cols = len(header)
        num_rows = len(data) + 1  # +1 cho header

        col_width = 10 / num_cols
        row_height = 0.6

        x_start = 1
        y_start = 7

        # Vẽ header
        for i, col_name in enumerate(header):
            x = x_start + i * col_width
            rect = plt.Rectangle((x, y_start), col_width, row_height,
                                 facecolor=header_color, edgecolor='black', linewidth=1)
            ax.add_patch(rect)
            ax.text(x + col_width / 2, y_start + row_height / 2, col_name,
                    ha='center', va='center', fontsize=10, fontweight='bold', color='white')

        # Vẽ dữ liệu
        for row_idx, row_data in enumerate(data):
            y = y_start - (row_idx + 1) * row_height
            for col_idx, value in enumerate(row_data):
                x = x_start + col_idx * col_width

                # Màu nền
                if 'Compare' in str(row_data[0]):
                    bg_color = '#E6E6FA'  # Lavender cho hàng so sánh
                else:
                    bg_color = 'white'

                rect = plt.Rectangle((x, y), col_width, row_height,
                                     facecolor=bg_color, edgecolor='black', linewidth=1)
                ax.add_patch(rect)

                # Màu chữ cho hàng so sánh
                text_color = 'black'
                font_weight = 'normal'

                if 'Compare' in str(row_data[0]) and col_idx > 0:
                    try:
                        val = float(str(value).replace('+', '').replace('-', ''))
                        if '+' in str(value):
                            text_color = 'green'
                            font_weight = 'bold'
                        elif '-' in str(value) and val > 0:
                            text_color = 'red'
                            font_weight = 'bold'
                    except:
                        pass

                font_size = 9 if len(str(value)) > 8 else 10
                ax.text(x + col_width / 2, y + row_height / 2, str(value),
                        ha='center', va='center', fontsize=font_size,
                        color=text_color, weight=font_weight)

    def _create_comprehensive_report(self, output_dir):
        """
        Tạo báo cáo tổng hợp chứa dashboard và tất cả biểu đồ
        """
        try:
            print("\n📋 Đang tạo báo cáo tổng hợp...")

            # Thu thập tất cả file ảnh
            image_files = []

            # Dashboard
            dashboard_file = os.path.join(output_dir, "VoLTE_KPI_Dashboard.png")
            if os.path.exists(dashboard_file):
                image_files.append(dashboard_file)

            # Biểu đồ Daily
            daily_chart_dir = os.path.join(output_dir, "Chart_daily")
            if os.path.exists(daily_chart_dir):
                for file in os.listdir(daily_chart_dir):
                    if file.endswith('.png'):
                        image_files.append(os.path.join(daily_chart_dir, file))

            # Biểu đồ Hourly
            hourly_chart_dir = os.path.join(output_dir, "Chart_hourly")
            if os.path.exists(hourly_chart_dir):
                for file in os.listdir(hourly_chart_dir):
                    if file.endswith('.png'):
                        image_files.append(os.path.join(hourly_chart_dir, file))

            if not image_files:
                print("   ❌ Không tìm thấy file ảnh nào để tạo báo cáo")
                return None

            # Tạo báo cáo PDF/PNG tổng hợp
            self._create_combined_report(image_files, output_dir)

        except Exception as e:
            print(f"❌ Lỗi tạo báo cáo tổng hợp: {e}")

    def _create_combined_report(self, image_files, output_dir):
        """
        Tạo báo cáo kết hợp tất cả ảnh
        """
        try:
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

            # Tính toán layout
            dashboard_img = None
            chart_images = []

            for img, filename in images:
                if 'Dashboard' in filename:
                    dashboard_img = img
                else:
                    chart_images.append(img)

            # Layout calculation
            charts_per_row = 2
            chart_rows = math.ceil(len(chart_images) / charts_per_row)

            # Kích thước
            page_width = 2100
            dashboard_height = 800 if dashboard_img else 0
            chart_width = 900
            chart_height = 600
            margin = 50
            spacing = 30
            header_height = 100

            total_height = (header_height + margin * 2 + dashboard_height +
                            spacing + chart_rows * (chart_height + spacing))

            # Tạo canvas
            report_img = Image.new('RGB', (page_width, total_height), 'white')
            draw = ImageDraw.Draw(report_img)

            # Header
            try:
                title_font = ImageFont.truetype("arial.ttf", 36)
                subtitle_font = ImageFont.truetype("arial.ttf", 20)
            except:
                title_font = ImageFont.load_default()
                subtitle_font = ImageFont.load_default()

            title = "VoLTE KPI COMPREHENSIVE ANALYSIS REPORT"
            title_bbox = draw.textbbox((0, 0), title, font=title_font)
            title_width = title_bbox[2] - title_bbox[0]
            draw.text(((page_width - title_width) // 2, margin), title,
                      fill='navy', font=title_font)

            subtitle = f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} | Charts: {len(chart_images)}"
            subtitle_bbox = draw.textbbox((0, 0), subtitle, font=subtitle_font)
            subtitle_width = subtitle_bbox[2] - subtitle_bbox[0]
            draw.text(((page_width - subtitle_width) // 2, margin + 50), subtitle,
                      fill='gray', font=subtitle_font)

            current_y = header_height + margin

            # Dashboard
            if dashboard_img:
                dashboard_resized = dashboard_img.resize((page_width - 2 * margin, dashboard_height),
                                                         Image.Resampling.LANCZOS)
                report_img.paste(dashboard_resized, (margin, current_y))
                current_y += dashboard_height + spacing

            # Charts
            for i, chart_img in enumerate(chart_images):
                row = i // charts_per_row
                col = i % charts_per_row

                x = margin + col * (chart_width + spacing)
                y = current_y + row * (chart_height + spacing)

                chart_resized = chart_img.resize((chart_width, chart_height),
                                                 Image.Resampling.LANCZOS)
                report_img.paste(chart_resized, (x, y))

            # Lưu report
            report_path = os.path.join(output_dir, "VoLTE_KPI_Comprehensive_Report.png")
            report_img.save(report_path, "PNG", quality=95)

            # Lưu PDF
            try:
                pdf_path = os.path.join(output_dir, "VoLTE_KPI_Comprehensive_Report.pdf")
                report_img.save(pdf_path, "PDF", quality=95)
                print(f"✅ Đã tạo báo cáo PDF: {pdf_path}")
            except:
                print("⚠️ Không thể tạo PDF")

            print(f"✅ Đã tạo báo cáo tổng hợp: {report_path}")
            return report_path

        except Exception as e:
            print(f"❌ Lỗi tạo báo cáo kết hợp: {e}")
            return None

    def process_complete_workflow(self, excel_path, output_dir="output_charts"):
        """
        Thực hiện quy trình hoàn chỉnh từ Excel đến báo cáo
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

        # Bước 5: Tạo dashboard và báo cáo
        print("\n📋 BƯỚC 5: TẠO DASHBOARD VÀ BÁO CÁO TỔNG HỢP")
        dashboard_path = self.create_dashboard_report(output_dir)

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

        if dashboard_path:
            print(f"   📊 VoLTE_KPI_Dashboard.png")

        if os.path.exists(os.path.join(output_dir, "VoLTE_KPI_Comprehensive_Report.png")):
            print(f"   📋 VoLTE_KPI_Comprehensive_Report.png")

        if os.path.exists(os.path.join(output_dir, "VoLTE_KPI_Comprehensive_Report.pdf")):
            print(f"   📋 VoLTE_KPI_Comprehensive_Report.pdf")

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
    print("   ✅ Tạo Dashboard KPI")
    print("   ✅ Tạo báo cáo tổng hợp PNG/PDF")
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