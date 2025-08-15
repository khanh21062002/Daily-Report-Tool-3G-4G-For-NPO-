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

        # Định nghĩa các biểu đồ cụ thể cần tạo
        self.required_charts = {
            'hourly': [
                {
                    'type': 'line',
                    'y_col': 'VoLTE Traffic (Erl)',
                    'title': 'VoLTE Traffic (Erl) by Date and Hour'
                },
                {
                    'type': 'combo',
                    'y_line': 'SRVCC HOSR UTRAN',
                    'y_bar': 'SRVCC HO Att UTRAN',
                    'title': 'SRVCC HOSR UTRAN and SRVCC HO Att UTRAN by Date and Hour'
                },
                {
                    'type': 'combo',
                    'y_line': 'VoLTE CSSR QCI1',
                    'y_bar': 'VoLTE RAB Att QCI1',
                    'title': 'VoLTE CSSR QCI1 and VoLTE RAB Att QCI1 by Date and Hour'
                },
                {
                    'type': 'combo',
                    'y_line': 'VoLTE CSSR QCI5',
                    'y_bar': 'VoLTE RAB Att QCI5',
                    'title': 'VoLTE CSSR QCI5 and VoLTE RAB Att QCI5 by Date and Hour'
                },
                {
                    'type': 'combo',
                    'y_line': 'VoLTE CDR QCI1',
                    'y_bar': 'VoLTE Call Drop QCI1',
                    'title': 'VoLTE CDR QCI1 and VoLTE Call Drop QCI1 by Date and Hour'
                },
                {
                    'type': 'combo',
                    'y_line': 'VoLTE CDR QCI5',
                    'y_bar': 'VoLTE Call Drop QCI5',
                    'title': 'VoLTE CDR QCI5 and VoLTE Call Drop QCI5 by Date and Hour'
                },
                {
                    'type': 'combo',
                    'y_line': 'VOLTE UL Packet Loss',
                    'y_bar': 'VOLTE UL Packet Loss_Mau so',
                    'title': 'VOLTE UL Packet Loss and VOLTE UL Packet Loss_Mau so by Date and Hour'
                },
                {
                    'type': 'combo',
                    'y_line': 'VOLTE DL Packet Loss',
                    'y_bar': 'VOLTE DL Packet Loss_Mau so',
                    'title': 'VOLTE DL Packet Loss and VOLTE DL Packet Loss_Mau so by Date and Hour'
                }
            ],
            'daily': [
                {
                    'type': 'line',
                    'y_col': 'VoLTE Traffic (Erl)',
                    'title': 'VoLTE Traffic (Erl) by Date'
                },
                {
                    'type': 'combo',
                    'y_line': 'SRVCC HOSR UTRAN',
                    'y_bar': 'SRVCC HO Att UTRAN',
                    'title': 'SRVCC HOSR UTRAN and SRVCC HO Att UTRAN by Date'
                },
                {
                    'type': 'combo',
                    'y_line': 'VoLTE CSSR QCI1',
                    'y_bar': 'VoLTE RAB Att QCI1',
                    'title': 'VoLTE CSSR QCI1 and VoLTE RAB Att QCI1 by Date'
                },
                {
                    'type': 'combo',
                    'y_line': 'VoLTE CSSR QCI5',
                    'y_bar': 'VoLTE RAB Att QCI5',
                    'title': 'VoLTE CSSR QCI5 and VoLTE RAB Att QCI5 by Date'
                },
                {
                    'type': 'combo',
                    'y_line': 'VoLTE CDR QCI1',
                    'y_bar': 'VoLTE Call Drop QCI1',
                    'title': 'VoLTE CDR QCI1 and VoLTE Call Drop QCI1 by Date'
                },
                {
                    'type': 'combo',
                    'y_line': 'VoLTE CDR QCI5',
                    'y_bar': 'VoLTE Call Drop QCI5',
                    'title': 'VoLTE CDR QCI5 and VoLTE Call Drop QCI5 by Date'
                },
                {
                    'type': 'combo',
                    'y_line': 'pmErabRelAbnormalEnbActHprQci',
                    'y_bar': 'VoLTE Call Drop QCI1',
                    'title': 'pmErabRelAbnormalEnbActHprQci and VoLTE Call Drop QCI1 by Date'
                }
            ]
        }

        print("VOLTE KPI DATA PROCESSOR - ENHANCED VERSION WITH FIXED HOURLY CHARTS")
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
                           'SRVCC', 'SR', 'HOSR', 'GB', '%', 'Rate', 'QCI', 'Att', 'Drop']

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
        df = self._process_datetime_column(df, sheet_name)

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
        header_keywords = ['Date', 'Time', 'VoLTE', 'CSSR', 'CDR', 'Traffic', 'SRVCC', 'QCI', 'Att', 'Drop']

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

    def _process_datetime_column(self, df, sheet_name):
        """
        Xử lý cột Date/Time với đặc biệt cho dữ liệu Hourly
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
            # Xử lý đặc biệt cho dữ liệu Hourly
            if 'hourly' in sheet_name.lower() or 'hour' in sheet_name.lower():
                df = self._process_hourly_datetime(df, date_col)
            else:
                # Xử lý bình thường cho Daily
                original_data = df[date_col].copy()
                df[date_col] = pd.to_datetime(df[date_col], errors='coerce')

                # Nếu có quá nhiều NaT, thử phương pháp khác
                nat_count = df[date_col].isna().sum()
                if nat_count > len(df) * 0.5:  # Hơn 50% là NaT
                    try:
                        df[date_col] = pd.to_datetime(original_data, origin='1899-12-30', unit='D', errors='coerce')
                    except:
                        df[date_col] = pd.to_datetime(original_data, infer_datetime_format=True, errors='coerce')

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

    def _process_hourly_datetime(self, df, date_col):
        """
        Xử lý đặc biệt cho dữ liệu hourly để tạo datetime chính xác
        """
        print("   🕐 Xử lý dữ liệu hourly với datetime đầy đủ...")

        # Kiểm tra xem có cột Hour riêng biệt không
        hour_col = None
        for col in df.columns:
            if 'hour' in str(col).lower() or 'time' in str(col).lower():
                if col != date_col:
                    hour_col = col
                    break

        if hour_col is not None:
            print(f"   🕐 Tìm thấy cột giờ riêng biệt: {hour_col}")
            # Kết hợp Date và Hour thành datetime đầy đủ
            try:
                df[date_col] = pd.to_datetime(df[date_col], errors='coerce')
                df[hour_col] = pd.to_numeric(df[hour_col], errors='coerce')

                # Tạo datetime đầy đủ
                df['datetime_full'] = df.apply(lambda row:
                                               row[date_col] + pd.Timedelta(hours=row[hour_col])
                                               if pd.notna(row[date_col]) and pd.notna(row[hour_col])
                                               else pd.NaT, axis=1)

                # Thay thế cột date bằng datetime đầy đủ
                df[date_col] = df['datetime_full']
                df = df.drop(['datetime_full'], axis=1, errors='ignore')

                # Xóa cột hour nếu không cần thiết cho chart
                if hour_col not in [col for chart in self.required_charts.get('hourly', [])
                                    for col in [chart.get('y_col'), chart.get('y_line'), chart.get('y_bar')] if col]:
                    df = df.drop([hour_col], axis=1, errors='ignore')

            except Exception as e:
                print(f"   ⚠️ Lỗi kết hợp Date-Hour: {e}")
                # Fall back về xử lý thông thường
                df[date_col] = pd.to_datetime(df[date_col], errors='coerce')
        else:
            # Thử phân tích datetime từ cột duy nhất
            original_data = df[date_col].copy()

            # Thử các format khác nhau cho hourly data
            formats_to_try = [
                '%Y-%m-%d %H:%M:%S',
                '%Y-%m-%d %H:%M',
                '%m/%d/%Y %H:%M',
                '%d/%m/%Y %H:%M',
                '%Y-%m-%d %H',
                None  # Let pandas infer
            ]

            for fmt in formats_to_try:
                try:
                    if fmt is None:
                        df[date_col] = pd.to_datetime(original_data, errors='coerce', infer_datetime_format=True)
                    else:
                        df[date_col] = pd.to_datetime(original_data, format=fmt, errors='coerce')

                    # Kiểm tra thành công
                    valid_count = df[date_col].notna().sum()
                    if valid_count > len(df) * 0.7:  # Ít nhất 70% thành công
                        print(f"   ✅ Thành công với format: {fmt if fmt else 'auto-detect'}")
                        break
                except:
                    continue

            # Nếu vẫn không thành công, thử xử lý số Excel
            if df[date_col].notna().sum() < len(df) * 0.5:
                try:
                    df[date_col] = pd.to_datetime(original_data, origin='1899-12-30', unit='D', errors='coerce')
                    print("   ✅ Đã sử dụng origin Excel để convert")
                except:
                    print("   ⚠️ Không thể convert datetime, giữ nguyên dữ liệu gốc")

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

        csv_files = {}

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
                csv_files[sheet_name] = csv_path
                self.cleaned_data[sheet_name] = df

            except Exception as e:
                print(f"❌ Lỗi khi lưu {csv_filename}: {e}")

        self.csv_files = csv_files
        return csv_files

    def _find_matching_column(self, df, target_cols):
        """
        Tìm cột phù hợp nhất từ danh sách tên cột mục tiêu
        """
        if isinstance(target_cols, str):
            target_cols = [target_cols]

        df_columns_lower = {col.lower(): col for col in df.columns}

        for target in target_cols:
            target_lower = target.lower()
            # Tìm khớp chính xác
            if target_lower in df_columns_lower:
                return df_columns_lower[target_lower]

            # Tìm khớp một phần
            for col_lower, col_original in df_columns_lower.items():
                if target_lower in col_lower or any(word in col_lower for word in target_lower.split()):
                    return col_original

        return None

    def create_specific_charts(self, output_dir="output_charts"):
        """
        Tạo các biểu đồ cụ thể theo yêu cầu với cải thiện cho Hourly
        """
        print(f"\n🎨 Tạo các biểu đồ cụ thể theo yêu cầu...")

        created_charts = []

        for sheet_name, df in self.cleaned_data.items():
            print(f"\n📊 Xử lý dữ liệu từ {sheet_name}...")

            # Xác định loại dữ liệu (daily hoặc hourly)
            data_type = 'daily' if 'daily' in sheet_name.lower() else 'hourly'

            # Tạo thư mục cho biểu đồ
            chart_folder = os.path.join(output_dir, f"Chart_{data_type}")
            os.makedirs(chart_folder, exist_ok=True)

            # Lấy danh sách biểu đồ cần tạo
            charts_config = self.required_charts.get(data_type, [])

            if not charts_config:
                print(f"   ⚠️ Không có cấu hình biểu đồ cho {data_type}")
                continue

            # Tìm cột thời gian
            time_col = df.columns[0]  # Giả định cột đầu tiên là thời gian

            print(f"   📅 Sử dụng cột thời gian: {time_col}")
            print(f"   📋 Tất cả các cột có sẵn: {list(df.columns)}")

            # Tạo từng biểu đồ
            for chart_config in charts_config:
                try:
                    if chart_config['type'] == 'line':
                        # Biểu đồ đường đơn
                        y_col = self._find_matching_column(df, chart_config['y_col'])
                        if y_col:
                            if data_type == 'hourly':
                                chart_path = self._create_enhanced_hourly_line_chart(
                                    df, time_col, y_col, chart_folder, chart_config['title']
                                )
                            else:
                                chart_path = self._create_enhanced_line_chart(
                                    df, time_col, y_col, chart_folder, chart_config['title']
                                )

                            if chart_path:
                                created_charts.append(chart_path)
                                print(f"   ✅ Đã tạo biểu đồ đường: {chart_config['title']}")
                            else:
                                print(f"   ❌ Không thể tạo biểu đồ đường: {chart_config['title']}")
                        else:
                            print(f"   ⚠️ Không tìm thấy cột: {chart_config['y_col']}")

                    elif chart_config['type'] == 'combo':
                        # Biểu đồ kết hợp
                        y_line = self._find_matching_column(df, chart_config['y_line'])
                        y_bar = self._find_matching_column(df, chart_config['y_bar'])

                        if y_line and y_bar:
                            if data_type == 'hourly':
                                chart_path = self._create_enhanced_hourly_combo_chart(
                                    df, time_col, y_line, y_bar, chart_folder, chart_config['title']
                                )
                            else:
                                chart_path = self._create_enhanced_combo_chart(
                                    df, time_col, y_line, y_bar, chart_folder, chart_config['title']
                                )

                            if chart_path:
                                created_charts.append(chart_path)
                                print(f"   ✅ Đã tạo biểu đồ kết hợp: {chart_config['title']}")
                            else:
                                print(f"   ❌ Không thể tạo biểu đồ kết hợp: {chart_config['title']}")
                        else:
                            missing_cols = []
                            if not y_line:
                                missing_cols.append(chart_config['y_line'])
                            if not y_bar:
                                missing_cols.append(chart_config['y_bar'])
                            print(f"   ⚠️ Không tìm thấy cột: {', '.join(missing_cols)}")

                except Exception as e:
                    print(f"   ❌ Lỗi tạo biểu đồ '{chart_config['title']}': {e}")
                    continue

        print(f"\n🎉 Đã tạo tổng cộng {len(created_charts)} biểu đồ cụ thể!")
        return created_charts

    def _create_enhanced_hourly_line_chart(self, df, x_col, y_col, chart_folder, title):
        """
        Tạo biểu đồ đường cho dữ liệu hourly với format đặc biệt
        """
        try:
            # Lọc dữ liệu hợp lệ
            clean_data = df[[x_col, y_col]].dropna()
            if clean_data.empty:
                return None

            plt.figure(figsize=(16, 8))

            # Vẽ biểu đồ đường với style đẹp hơn cho hourly data
            plt.plot(clean_data[x_col], clean_data[y_col],
                     marker='o', linewidth=2, markersize=3,
                     color='#2E86AB', alpha=0.8, markerfacecolor='#A23B72',
                     markeredgecolor='white', markeredgewidth=0.5)

            # Định dạng tiêu đề và labels
            plt.title(title, fontsize=16, fontweight='bold', pad=25, color='#2C3E50')
            plt.xlabel('Date and Hour', fontsize=12, fontweight='bold', color='#34495E')
            plt.ylabel(y_col, fontsize=12, fontweight='bold', color='#34495E')

            # Grid và styling
            plt.grid(True, alpha=0.4, linestyle='--', linewidth=0.8)
            plt.gca().set_facecolor('#F8F9FA')

            # Định dạng trục x đặc biệt cho hourly data
            if pd.api.types.is_datetime64_any_dtype(clean_data[x_col]):
                # Lấy số ngày duy nhất
                dates = pd.to_datetime(clean_data[x_col].dt.date).unique()
                num_days = len(dates)

                if num_days <= 7:  # Ít hơn 1 tuần - hiện từng giờ
                    plt.gca().xaxis.set_major_formatter(mdates.DateFormatter('%m/%d\n%H:%M'))
                    plt.gca().xaxis.set_major_locator(mdates.HourLocator(interval=max(1, len(clean_data) // 20)))
                elif num_days <= 31:  # Ít hơn 1 tháng - hiện một số giờ
                    plt.gca().xaxis.set_major_formatter(mdates.DateFormatter('%m/%d\n%H:00'))
                    plt.gca().xaxis.set_major_locator(mdates.HourLocator(interval=max(6, len(clean_data) // 30)))
                else:  # Nhiều hơn - hiện theo ngày với một số sample giờ
                    plt.gca().xaxis.set_major_formatter(mdates.DateFormatter('%m/%d'))
                    plt.gca().xaxis.set_major_locator(mdates.DayLocator(interval=max(1, num_days // 15)))

                plt.xticks(rotation=45)

            # Styling cho axes
            ax = plt.gca()
            ax.spines['top'].set_visible(False)
            ax.spines['right'].set_visible(False)
            ax.spines['left'].set_color('#BDC3C7')
            ax.spines['bottom'].set_color('#BDC3C7')

            plt.tight_layout()

            # Tạo tên file an toàn
            safe_filename = "".join(c for c in title if c.isalnum() or c in (' ', '-', '_')).replace(' ', '_')
            chart_path = os.path.join(chart_folder, f"{safe_filename}_line.png")
            plt.savefig(chart_path, dpi=300, bbox_inches='tight', facecolor='white', edgecolor='none')
            plt.close()

            return chart_path

        except Exception as e:
            plt.close()
            print(f"      ❌ Lỗi tạo biểu đồ đường hourly: {e}")
            return None

    def _create_enhanced_hourly_combo_chart(self, df, x_col, y_line, y_bar, chart_folder, title):
        """
        Tạo biểu đồ kết hợp (đường + cột) cho dữ liệu hourly
        """
        try:
            # Lọc dữ liệu hợp lệ
            clean_data = df[[x_col, y_line, y_bar]].dropna()
            if clean_data.empty:
                return None

            fig, ax1 = plt.subplots(figsize=(16, 8))
            fig.patch.set_facecolor('white')

            # Trục Y bên trái (đường) - sử dụng gradient color
            color_line = '#E74C3C'
            ax1.set_xlabel('Date and Hour', fontsize=12, fontweight='bold', color='#34495E')
            ax1.set_ylabel(y_line, color=color_line, fontsize=12, fontweight='bold')

            line_plot = ax1.plot(clean_data[x_col], clean_data[y_line],
                                 marker='o', color=color_line, linewidth=2, markersize=3,
                                 label=y_line, alpha=0.9, markerfacecolor='white',
                                 markeredgecolor=color_line, markeredgewidth=1)

            ax1.tick_params(axis='y', labelcolor=color_line, labelsize=10)
            ax1.tick_params(axis='x', labelsize=9, rotation=45)
            ax1.grid(True, alpha=0.3, linestyle='--', linewidth=0.5)

            # Trục Y bên phải (cột) - sử dụng gradient color
            ax2 = ax1.twinx()
            color_bar = '#3498DB'
            ax2.set_ylabel(y_bar, color=color_bar, fontsize=12, fontweight='bold')

            # Tính độ rộng cột dựa trên số lượng dữ liệu và hourly spacing
            if len(clean_data) > 200:  # Nhiều dữ liệu hourly
                bar_width = 0.8
                alpha_val = 0.6
            elif len(clean_data) > 100:
                bar_width = 0.9
                alpha_val = 0.65
            else:
                bar_width = 1.0
                alpha_val = 0.7

            # Tạo bar width dựa trên time difference
            if pd.api.types.is_datetime64_any_dtype(clean_data[x_col]) and len(clean_data) > 1:
                time_diff = (clean_data[x_col].iloc[1] - clean_data[x_col].iloc[0]).total_seconds() / 3600  # hours
                if time_diff <= 1:  # hourly data
                    bar_width = pd.Timedelta(hours=0.8)
                else:
                    bar_width = pd.Timedelta(hours=time_diff * 0.8)

            bars = ax2.bar(clean_data[x_col], clean_data[y_bar],
                           alpha=alpha_val, color=color_bar, label=y_bar,
                           width=bar_width, edgecolor='white', linewidth=0.5)

            ax2.tick_params(axis='y', labelcolor=color_bar, labelsize=10)

            # Tiêu đề với styling đẹp
            plt.title(title, fontsize=16, fontweight='bold', pad=25, color='#2C3E50')

            # Định dạng trục x đặc biệt cho hourly data
            if pd.api.types.is_datetime64_any_dtype(clean_data[x_col]):
                # Lấy số ngày duy nhất
                dates = pd.to_datetime(clean_data[x_col].dt.date).unique()
                num_days = len(dates)

                if num_days <= 7:  # Ít hơn 1 tuần - hiện từng giờ
                    ax1.xaxis.set_major_formatter(mdates.DateFormatter('%m/%d\n%H:%M'))
                    ax1.xaxis.set_major_locator(mdates.HourLocator(interval=max(1, len(clean_data) // 20)))
                elif num_days <= 31:  # Ít hơn 1 tháng - hiện một số giờ
                    ax1.xaxis.set_major_formatter(mdates.DateFormatter('%m/%d\n%H:00'))
                    ax1.xaxis.set_major_locator(mdates.HourLocator(interval=max(6, len(clean_data) // 30)))
                else:  # Nhiều hơn - hiện theo ngày với một số sample giờ
                    ax1.xaxis.set_major_formatter(mdates.DateFormatter('%m/%d'))
                    ax1.xaxis.set_major_locator(mdates.DayLocator(interval=max(1, num_days // 15)))

                fig.autofmt_xdate()

            # Legend kết hợp với styling
            lines1, labels1 = ax1.get_legend_handles_labels()
            lines2, labels2 = ax2.get_legend_handles_labels()
            legend = ax1.legend(lines1 + lines2, labels1 + labels2,
                                loc='upper left', fontsize=10,
                                frameon=True, fancybox=True, shadow=True,
                                facecolor='white', edgecolor='#BDC3C7')

            # Styling cho background
            ax1.set_facecolor('#F8F9FA')

            # Loại bỏ spines không cần thiết
            ax1.spines['top'].set_visible(False)
            ax2.spines['top'].set_visible(False)
            ax1.spines['right'].set_visible(False)
            ax2.spines['left'].set_visible(False)

            fig.tight_layout()

            # Tạo tên file an toàn
            safe_filename = "".join(c for c in title if c.isalnum() or c in (' ', '-', '_')).replace(' ', '_')
            chart_path = os.path.join(chart_folder, f"{safe_filename}_combo.png")
            plt.savefig(chart_path, dpi=300, bbox_inches='tight', facecolor='white', edgecolor='none')
            plt.close()

            return chart_path

        except Exception as e:
            plt.close()
            print(f"      ❌ Lỗi tạo biểu đồ kết hợp hourly: {e}")
            return None

    def _create_enhanced_line_chart(self, df, x_col, y_col, chart_folder, title):
        """
        Tạo biểu đồ đường cho dữ liệu daily
        """
        try:
            # Lọc dữ liệu hợp lệ
            clean_data = df[[x_col, y_col]].dropna()
            if clean_data.empty:
                return None

            plt.figure(figsize=(14, 8))

            # Vẽ biểu đồ đường với style đẹp hơn
            plt.plot(clean_data[x_col], clean_data[y_col],
                     marker='o', linewidth=3, markersize=5,
                     color='#2E86AB', alpha=0.8, markerfacecolor='#A23B72',
                     markeredgecolor='white', markeredgewidth=1)

            # Định dạng tiêu đề và labels
            plt.title(title, fontsize=16, fontweight='bold', pad=25, color='#2C3E50')
            plt.xlabel('Date', fontsize=12, fontweight='bold', color='#34495E')
            plt.ylabel(y_col, fontsize=12, fontweight='bold', color='#34495E')

            # Grid và styling
            plt.grid(True, alpha=0.4, linestyle='--', linewidth=0.8)
            plt.gca().set_facecolor('#F8F9FA')

            # Định dạng trục x cho datetime
            if pd.api.types.is_datetime64_any_dtype(clean_data[x_col]):
                plt.gca().xaxis.set_major_formatter(mdates.DateFormatter('%m/%d'))
                plt.gca().xaxis.set_major_locator(mdates.DayLocator(interval=max(1, len(clean_data) // 10)))
                plt.xticks(rotation=45)

            # Styling cho axes
            ax = plt.gca()
            ax.spines['top'].set_visible(False)
            ax.spines['right'].set_visible(False)
            ax.spines['left'].set_color('#BDC3C7')
            ax.spines['bottom'].set_color('#BDC3C7')

            plt.tight_layout()

            # Tạo tên file an toàn
            safe_filename = "".join(c for c in title if c.isalnum() or c in (' ', '-', '_')).replace(' ', '_')
            chart_path = os.path.join(chart_folder, f"{safe_filename}_line.png")
            plt.savefig(chart_path, dpi=300, bbox_inches='tight', facecolor='white', edgecolor='none')
            plt.close()

            return chart_path

        except Exception as e:
            plt.close()
            print(f"      ❌ Lỗi tạo biểu đồ đường: {e}")
            return None

    def _create_enhanced_combo_chart(self, df, x_col, y_line, y_bar, chart_folder, title):
        """
        Tạo biểu đồ kết hợp (đường + cột) cho dữ liệu daily
        """
        try:
            # Lọc dữ liệu hợp lệ
            clean_data = df[[x_col, y_line, y_bar]].dropna()
            if clean_data.empty:
                return None

            fig, ax1 = plt.subplots(figsize=(14, 8))
            fig.patch.set_facecolor('white')

            # Trục Y bên trái (đường) - sử dụng gradient color
            color_line = '#E74C3C'
            ax1.set_xlabel('Date', fontsize=12, fontweight='bold', color='#34495E')
            ax1.set_ylabel(y_line, color=color_line, fontsize=12, fontweight='bold')

            line_plot = ax1.plot(clean_data[x_col], clean_data[y_line],
                                 marker='o', color=color_line, linewidth=3, markersize=5,
                                 label=y_line, alpha=0.9, markerfacecolor='white',
                                 markeredgecolor=color_line, markeredgewidth=2)

            ax1.tick_params(axis='y', labelcolor=color_line, labelsize=10)
            ax1.tick_params(axis='x', labelsize=10, rotation=45)
            ax1.grid(True, alpha=0.3, linestyle='--', linewidth=0.5)

            # Trục Y bên phải (cột) - sử dụng gradient color
            ax2 = ax1.twinx()
            color_bar = '#3498DB'
            ax2.set_ylabel(y_bar, color=color_bar, fontsize=12, fontweight='bold')

            # Tính độ rộng cột dựa trên số lượng dữ liệu
            if len(clean_data) > 30:
                bar_width = 0.4
            elif len(clean_data) > 15:
                bar_width = 0.6
            else:
                bar_width = 0.8

            bars = ax2.bar(clean_data[x_col], clean_data[y_bar],
                           alpha=0.7, color=color_bar, label=y_bar,
                           width=bar_width, edgecolor='white', linewidth=0.5)

            ax2.tick_params(axis='y', labelcolor=color_bar, labelsize=10)

            # Tiêu đề với styling đẹp
            plt.title(title, fontsize=16, fontweight='bold', pad=25, color='#2C3E50')

            # Định dạng trục x
            if pd.api.types.is_datetime64_any_dtype(clean_data[x_col]):
                fig.autofmt_xdate()
                ax1.xaxis.set_major_formatter(mdates.DateFormatter('%m/%d'))
                ax1.xaxis.set_major_locator(mdates.DayLocator(interval=max(1, len(clean_data) // 10)))

            # Legend kết hợp với styling
            lines1, labels1 = ax1.get_legend_handles_labels()
            lines2, labels2 = ax2.get_legend_handles_labels()
            legend = ax1.legend(lines1 + lines2, labels1 + labels2,
                                loc='upper left', fontsize=11,
                                frameon=True, fancybox=True, shadow=True,
                                facecolor='white', edgecolor='#BDC3C7')

            # Styling cho background
            ax1.set_facecolor('#F8F9FA')

            # Loại bỏ spines không cần thiết
            ax1.spines['top'].set_visible(False)
            ax2.spines['top'].set_visible(False)
            ax1.spines['right'].set_visible(False)
            ax2.spines['left'].set_visible(False)

            fig.tight_layout()

            # Tạo tên file an toàn
            safe_filename = "".join(c for c in title if c.isalnum() or c in (' ', '-', '_')).replace(' ', '_')
            chart_path = os.path.join(chart_folder, f"{safe_filename}_combo.png")
            plt.savefig(chart_path, dpi=300, bbox_inches='tight', facecolor='white', edgecolor='none')
            plt.close()

            return chart_path

        except Exception as e:
            plt.close()
            print(f"      ❌ Lỗi tạo biểu đồ kết hợp: {e}")
            return None

    def create_comprehensive_report(self, output_dir="output_charts"):
        """
        Tạo báo cáo tổng hợp với tất cả các biểu đồ cụ thể
        """
        print(f"\n📋 Tạo báo cáo tổng hợp với các biểu đồ cụ thể...")

        try:
            # Thu thập tất cả file ảnh biểu đồ
            image_files = []

            # Biểu đồ Daily
            daily_chart_dir = os.path.join(output_dir, "Chart_daily")
            if os.path.exists(daily_chart_dir):
                daily_files = []
                for file in sorted(os.listdir(daily_chart_dir)):
                    if file.endswith('.png'):
                        daily_files.append(os.path.join(daily_chart_dir, file))
                image_files.extend(daily_files)
                print(f"   📊 Tìm thấy {len(daily_files)} biểu đồ Daily")

            # Biểu đồ Hourly
            hourly_chart_dir = os.path.join(output_dir, "Chart_hourly")
            if os.path.exists(hourly_chart_dir):
                hourly_files = []
                for file in sorted(os.listdir(hourly_chart_dir)):
                    if file.endswith('.png'):
                        hourly_files.append(os.path.join(hourly_chart_dir, file))
                image_files.extend(hourly_files)
                print(f"   📊 Tìm thấy {len(hourly_files)} biểu đồ Hourly")

            if not image_files:
                print("   ❌ Không tìm thấy file ảnh nào để tạo báo cáo")
                return None

            print(f"   📊 Tổng cộng {len(image_files)} biểu đồ sẽ được đưa vào báo cáo")

            # Tạo báo cáo đa trang nếu có nhiều biểu đồ
            if len(image_files) <= 9:
                report_path = self._create_single_page_report(image_files, output_dir)
            else:
                report_path = self._create_multi_page_report(image_files, output_dir)

            if report_path:
                print(f"✅ Đã tạo báo cáo tổng hợp: {report_path}")

                # Tạo PDF từ PNG
                try:
                    if isinstance(report_path, list):
                        # Multi-page report
                        pdf_path = os.path.join(output_dir, "VoLTE_KPI_Multi_Page_Report.pdf")
                        self._create_multi_page_pdf(report_path, pdf_path)
                    else:
                        # Single-page report
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
        Tạo báo cáo single page cho ít biểu đồ
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

            # Cấu hình layout cho trang A4
            page_width = 2100
            page_height = 2970
            margin = 60
            header_height = 150
            footer_height = 80
            spacing = 40

            # Layout dựa trên số lượng biểu đồ
            if len(images) <= 4:
                cols, rows = 2, 2
            elif len(images) <= 6:
                cols, rows = 2, 3
            else:  # <= 9
                cols, rows = 3, 3

            # Chỉ lấy số biểu đồ vừa đủ
            images = images[:cols * rows]

            # Tính kích thước biểu đồ
            available_width = page_width - 2 * margin - (cols - 1) * spacing
            available_height = page_height - header_height - footer_height - 2 * margin - (rows - 1) * spacing

            chart_width = available_width // cols
            chart_height = available_height // rows

            # Tạo canvas
            report_img = Image.new('RGB', (page_width, page_height), 'white')
            draw = ImageDraw.Draw(report_img)

            # Font setup
            try:
                title_font = ImageFont.truetype("arial.ttf", 42)
                subtitle_font = ImageFont.truetype("arial.ttf", 22)
                page_font = ImageFont.truetype("arial.ttf", 18)
            except:
                title_font = ImageFont.load_default()
                subtitle_font = ImageFont.load_default()
                page_font = ImageFont.load_default()

            # Header
            title = "VoLTE KPI COMPREHENSIVE ANALYSIS REPORT"
            title_bbox = draw.textbbox((0, 0), title, font=title_font)
            title_width = title_bbox[2] - title_bbox[0]
            draw.text(((page_width - title_width) // 2, margin), title,
                      fill='#2C3E50', font=title_font)

            # Subtitle
            subtitle = f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M')} | Charts: {len(images)}"
            subtitle_bbox = draw.textbbox((0, 0), subtitle, font=subtitle_font)
            subtitle_width = subtitle_bbox[2] - subtitle_bbox[0]
            draw.text(((page_width - subtitle_width) // 2, margin + 55), subtitle,
                      fill='#7F8C8D', font=subtitle_font)

            # Separator line
            line_y = header_height + margin - 20
            draw.line([(margin, line_y), (page_width - margin, line_y)], fill='#BDC3C7', width=3)

            # Vẽ các biểu đồ
            start_y = header_height + margin

            for i, (chart_img, filename) in enumerate(images):
                row = i // cols
                col = i % cols

                x = margin + col * (chart_width + spacing)
                y = start_y + row * (chart_height + spacing)

                # Resize biểu đồ
                chart_resized = self._resize_image_proportional(chart_img, chart_width - 20, chart_height - 40)

                # Căn giữa
                chart_w, chart_h = chart_resized.size
                center_x = x + (chart_width - chart_w) // 2
                center_y = y + (chart_height - chart_h) // 2

                # Vẽ background cho biểu đồ
                bg_rect = [x + 5, y + 5, x + chart_width - 5, y + chart_height - 5]
                draw.rectangle(bg_rect, fill='#F8F9FA', outline='#E5E5E5', width=2)

                # Paste biểu đồ
                report_img.paste(chart_resized, (center_x, center_y))

                # Title cho biểu đồ (truncate nếu quá dài)
                chart_title = filename.replace('.png', '').replace('_', ' ')
                if len(chart_title) > 40:
                    chart_title = chart_title[:37] + "..."

                title_bbox = draw.textbbox((0, 0), chart_title, font=page_font)
                title_width = title_bbox[2] - title_bbox[0]
                title_x = x + (chart_width - title_width) // 2
                draw.text((title_x, y + chart_height - 35), chart_title,
                          fill='#34495E', font=page_font)

            # Footer
            footer_text = f"VoLTE KPI Analysis | Total Charts: {len(images)}"
            footer_bbox = draw.textbbox((0, 0), footer_text, font=subtitle_font)
            footer_width = footer_bbox[2] - footer_bbox[0]
            draw.text(((page_width - footer_width) // 2, page_height - footer_height), footer_text,
                      fill='#95A5A6', font=subtitle_font)

            # Lưu báo cáo
            report_path = os.path.join(output_dir, "VoLTE_KPI_Single_Page_Report.png")
            report_img.save(report_path, "PNG", quality=95, dpi=(300, 300))

            return report_path

        except Exception as e:
            print(f"❌ Lỗi tạo báo cáo single page: {e}")
            return None

    def _create_multi_page_report(self, image_files, output_dir):
        """
        Tạo báo cáo nhiều trang cho nhiều biểu đồ
        """
        try:
            charts_per_page = 6  # Số biểu đồ tối đa mỗi trang
            total_pages = math.ceil(len(image_files) / charts_per_page)

            print(f"   📄 Tạo báo cáo {total_pages} trang với {len(image_files)} biểu đồ")

            page_files = []

            for page_num in range(total_pages):
                start_idx = page_num * charts_per_page
                end_idx = min(start_idx + charts_per_page, len(image_files))
                page_images = image_files[start_idx:end_idx]

                page_path = self._create_report_page(page_images, output_dir, page_num + 1, total_pages)
                if page_path:
                    page_files.append(page_path)

            return page_files

        except Exception as e:
            print(f"❌ Lỗi tạo báo cáo nhiều trang: {e}")
            return None

    def _create_report_page(self, image_files, output_dir, page_num, total_pages):
        """
        Tạo một trang báo cáo
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

            # Cấu hình layout cho trang A4
            page_width = 2100
            page_height = 2970
            margin = 60
            header_height = 150
            footer_height = 80
            spacing = 40

            # Layout cho 6 biểu đồ: 2 cột x 3 hàng
            cols = 2
            rows = 3

            # Chỉ lấy tối đa 6 biểu đồ
            images = images[:6]

            # Tính kích thước biểu đồ
            available_width = page_width - 2 * margin - (cols - 1) * spacing
            available_height = page_height - header_height - footer_height - 2 * margin - (rows - 1) * spacing

            chart_width = available_width // cols
            chart_height = available_height // rows

            # Tạo canvas
            report_img = Image.new('RGB', (page_width, page_height), 'white')
            draw = ImageDraw.Draw(report_img)

            # Font setup
            try:
                title_font = ImageFont.truetype("arial.ttf", 42)
                subtitle_font = ImageFont.truetype("arial.ttf", 22)
                page_font = ImageFont.truetype("arial.ttf", 18)
            except:
                title_font = ImageFont.load_default()
                subtitle_font = ImageFont.load_default()
                page_font = ImageFont.load_default()

            # Header
            title = "VoLTE KPI COMPREHENSIVE ANALYSIS REPORT"
            title_bbox = draw.textbbox((0, 0), title, font=title_font)
            title_width = title_bbox[2] - title_bbox[0]
            draw.text(((page_width - title_width) // 2, margin), title,
                      fill='#2C3E50', font=title_font)

            # Subtitle
            subtitle = f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M')} | Page {page_num}/{total_pages}"
            subtitle_bbox = draw.textbbox((0, 0), subtitle, font=subtitle_font)
            subtitle_width = subtitle_bbox[2] - subtitle_bbox[0]
            draw.text(((page_width - subtitle_width) // 2, margin + 55), subtitle,
                      fill='#7F8C8D', font=subtitle_font)

            # Separator line
            line_y = header_height + margin - 20
            draw.line([(margin, line_y), (page_width - margin, line_y)], fill='#BDC3C7', width=3)

            # Vẽ các biểu đồ
            start_y = header_height + margin

            for i, (chart_img, filename) in enumerate(images):
                row = i // cols
                col = i % cols

                x = margin + col * (chart_width + spacing)
                y = start_y + row * (chart_height + spacing)

                # Resize biểu đồ
                chart_resized = self._resize_image_proportional(chart_img, chart_width - 20, chart_height - 40)

                # Căn giữa
                chart_w, chart_h = chart_resized.size
                center_x = x + (chart_width - chart_w) // 2
                center_y = y + (chart_height - chart_h) // 2

                # Vẽ background cho biểu đồ
                bg_rect = [x + 5, y + 5, x + chart_width - 5, y + chart_height - 5]
                draw.rectangle(bg_rect, fill='#F8F9FA', outline='#E5E5E5', width=2)

                # Paste biểu đồ
                report_img.paste(chart_resized, (center_x, center_y))

                # Title cho biểu đồ (truncate nếu quá dài)
                chart_title = filename.replace('.png', '').replace('_', ' ')
                if len(chart_title) > 40:
                    chart_title = chart_title[:37] + "..."

                title_bbox = draw.textbbox((0, 0), chart_title, font=page_font)
                title_width = title_bbox[2] - title_bbox[0]
                title_x = x + (chart_width - title_width) // 2
                draw.text((title_x, y + chart_height - 35), chart_title,
                          fill='#34495E', font=page_font)

            # Footer
            footer_text = f"VoLTE KPI Analysis | Charts on this page: {len(images)}"
            footer_bbox = draw.textbbox((0, 0), footer_text, font=subtitle_font)
            footer_width = footer_bbox[2] - footer_bbox[0]
            draw.text(((page_width - footer_width) // 2, page_height - footer_height), footer_text,
                      fill='#95A5A6', font=subtitle_font)

            # Lưu trang
            page_path = os.path.join(output_dir, f"VoLTE_KPI_Report_Page_{page_num}.png")
            report_img.save(page_path, "PNG", quality=95, dpi=(300, 300))

            return page_path

        except Exception as e:
            print(f"❌ Lỗi tạo trang báo cáo {page_num}: {e}")
            return None

    def _create_multi_page_pdf(self, page_files, pdf_path):
        """
        Tạo PDF từ nhiều trang PNG
        """
        try:
            if not page_files:
                return False

            images = []
            for page_file in page_files:
                img = Image.open(page_file)
                images.append(img)

            # Lưu PDF
            images[0].save(pdf_path, "PDF", save_all=True, append_images=images[1:], quality=95)
            return True

        except Exception as e:
            print(f"❌ Lỗi tạo PDF nhiều trang: {e}")
            return False

    def _resize_image_proportional(self, img, max_width, max_height):
        """
        Resize ảnh giữ nguyên tỉ lệ
        """
        original_width, original_height = img.size
        ratio_w = max_width / original_width
        ratio_h = max_height / original_height
        ratio = min(ratio_w, ratio_h)

        new_width = int(original_width * ratio)
        new_height = int(original_height * ratio)

        return img.resize((new_width, new_height), Image.Resampling.LANCZOS)

    def process_complete_workflow(self, excel_path, output_dir="output_charts"):
        """
        Thực hiện quy trình hoàn chỉnh với biểu đồ cụ thể
        """
        print(f"\n🎯 BẮT ĐẦU QUY TRÌNH XỬ LÝ HOÀN CHỈNH - ENHANCED VERSION WITH FIXED HOURLY CHARTS")
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
        print("\n🧹 BƯỚC 2: LÀM SẠCH DỮ LIỆU VỚI CẢI THIỆN CHO HOURLY")
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

        # Bước 4: Tạo biểu đồ cụ thể với cải thiện hourly
        print("\n🎨 BƯỚC 4: TẠO CÁC BIỂU ĐỒ CỤ THỂ VỚI HOURLY CHARTS CẢI THIỆN")
        created_charts = self.create_specific_charts(output_dir)

        if not created_charts:
            print("⚠️ Không tạo được biểu đồ nào!")
            return False

        # Bước 5: Tạo báo cáo tổng hợp
        print("\n📋 BƯỚC 5: TẠO BÁO CÁO TỔNG HỢP")
        report_path = self.create_comprehensive_report(output_dir)

        # Tổng kết
        print("\n" + "=" * 70)
        print("🎉 HOÀN TẤT QUY TRÌNH XỬ LÝ NÂNG CAO VỚI HOURLY CHARTS CẢI THIỆN!")
        print("=" * 70)
        print(f"📁 Kết quả lưu tại: {output_dir}")
        print(f"📊 Đã tạo {len(created_charts)} biểu đồ cụ thể")

        print("\n📊 Cấu trúc kết quả:")
        print("📂 output_charts/")

        # Hiển thị CSV files
        for sheet_name, csv_path in csv_files.items():
            print(f"   📄 {os.path.basename(csv_path)}")

        # Hiển thị biểu đồ
        chart_folders = ['Chart_daily', 'Chart_hourly']
        for folder in chart_folders:
            folder_path = os.path.join(output_dir, folder)
            if os.path.exists(folder_path):
                chart_count = len([f for f in os.listdir(folder_path) if f.endswith('.png')])
                print(f"   📂 {folder}/ ({chart_count} biểu đồ cụ thể)")

        # Hiển thị báo cáo
        if report_path:
            if isinstance(report_path, list):
                print(f"   📊 VoLTE_KPI_Report_Page_*.png ({len(report_path)} trang)")
                print(f"   📊 VoLTE_KPI_Multi_Page_Report.pdf")
            else:
                print(f"   📊 {os.path.basename(report_path)}")
                pdf_path = report_path.replace('.png', '.pdf')
                if os.path.exists(pdf_path):
                    print(f"   📊 {os.path.basename(pdf_path)}")

        print("\n✨ Cải thiện đặc biệt cho Hourly Charts:")
        print("   🕐 Xử lý datetime chính xác cho dữ liệu hourly")
        print("   📅 Format trục thời gian phù hợp với hourly data")
        print("   📊 Bar width tự động điều chỉnh theo density dữ liệu")
        print("   🎨 Styling tối ưu cho visualization hourly data")

        print("\n✨ Các biểu đồ được tạo theo đúng yêu cầu:")
        print("   📈 VoLTE Traffic (Erl) - Line charts (Daily & Hourly)")
        print("   📊 SRVCC HOSR & Att - Combo charts (Daily & Hourly)")
        print("   📊 VoLTE CSSR & RAB Att QCI1/QCI5 - Combo charts (Daily & Hourly)")
        print("   📊 VoLTE CDR & Call Drop QCI1/QCI5 - Combo charts (Daily & Hourly)")
        print("   📊 VOLTE UL/DL Packet Loss - Combo charts (Daily & Hourly)")
        print("=" * 70)

        return True


def main():
    """
    Hàm main để chạy chương trình với các biểu đồ cụ thể và cải thiện hourly
    """
    print("🚀 VOLTE KPI DATA PROCESSING SYSTEM - ENHANCED VERSION WITH FIXED HOURLY CHARTS")
    print("=" * 70)
    print("📋 Chức năng nâng cao:")
    print("   ✅ Chuyển đổi Excel sang CSV")
    print("   ✅ Làm sạch dữ liệu chuyên sâu")
    print("   ✅ Xử lý datetime chính xác cho hourly data")
    print("   ✅ Tạo các biểu đồ CỤ THỂ theo yêu cầu:")
    print("      📈 VoLTE Traffic (Erl) - Biểu đồ đường")
    print("      📊 SRVCC HOSR & HO Att - Biểu đồ kết hợp")
    print("      📊 VoLTE CSSR & RAB Att QCI1/QCI5 - Biểu đồ kết hợp")
    print("      📊 VoLTE CDR & Call Drop QCI1/QCI5 - Biểu đồ kết hợp")
    print("      📊 VOLTE UL/DL Packet Loss - Biểu đồ kết hợp")
    print("   ✅ Format đặc biệt cho biểu đồ hourly")
    print("   ✅ Tạo báo cáo tổng hợp PNG/PDF (đơn trang hoặc nhiều trang)")
    print("=" * 70)

    # Khởi tạo processor
    processor = VoLTEKPIProcessor()

    # Đường dẫn file Excel
    excel_file = "4G_KPI Cell VoLTE_ThanhTT.xlsx"

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
        print("📊 Tất cả các biểu đồ cụ thể đã được tạo theo đúng yêu cầu")
        print("🕐 Biểu đồ hourly đã được cải thiện với format thời gian chính xác")
        print("📄 Báo cáo tổng hợp được lưu dạng PNG và PDF")
    else:
        print("\n❌ CÓ LỖI XẢY RA! Vui lòng kiểm tra lại dữ liệu đầu vào")


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
    main()