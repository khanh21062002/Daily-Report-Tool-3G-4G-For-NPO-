import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import os
import numpy as np
from datetime import datetime, timedelta
from PIL import Image, ImageDraw, ImageFont
import math
from matplotlib.backends.backend_agg import FigureCanvasAgg as FigureCanvas
from matplotlib.figure import Figure


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

    def create_daily_dashboard_table(self, csv_all_day, csv_busy_hour, output_dir):
        """
        Tạo bảng Daily Dashboard (24h và BH) theo định dạng như hình mẫu với 2 bảng riêng biệt
        """
        try:
            print("\n📊 Đang tạo bảng Daily Dashboard...")

            # Đọc dữ liệu
            df_all = pd.read_csv(csv_all_day)
            df_bh = pd.read_csv(csv_busy_hour)

            # Chuyển đổi cột Date
            date_col = df_all.columns[0]
            df_all[date_col] = pd.to_datetime(df_all[date_col])
            df_bh[date_col] = pd.to_datetime(df_bh[date_col])

            # Mapping KPI names to dashboard columns
            kpi_mapping = {
                # Group 1: Success Rates
                'ePS CSSR': ['ePS CSSR', 'ePS Call Setup Success Rate'],
                'ePS CDR': ['ePS CDR', 'ePS Call Drop Rate'],
                'CSFB SR': ['CSFB SR', 'CSFB Success Rate'],
                'PS Traffic (GB)': ['PS Traffic (GB)', 'PS Traffic UL (GB)'],

                # Group 2: Handover rates
                'IntraF HOSR': ['IntraF HOSR', 'Intra-Freq Handover Success Rate'],
                'InterF HOSR': ['InterF HOSR', 'Inter-Freq Handover Success Rate'],
                'InterRAT HOSR (L2W)': ['InterRAT HOSR L2W','InterRAT HOSR (L2W)', 'InterRAT HOSR (2G)', 'InterRAT HOSR'],
                'MIMO Rate': ['MIMO Rate','% MIMO']
            }

            # Tạo figure với 2 subplots
            fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(16, 14))
            fig.suptitle('Daily 4G KPI Dashboard', fontsize=18, fontweight='bold', y=0.98)

            # Lấy các ngày: latest, previous (day before), rồi ngày <= latest - 7 ngày (nearest)
            latest = df_all[date_col].max()
            prev = df_all[df_all[date_col] < latest][date_col].max() if pd.notna(latest) else pd.NaT
            week_candidate = latest - timedelta(days=7) if pd.notna(latest) else pd.NaT
            week_date = df_all[df_all[date_col] <= week_candidate][date_col].max() if pd.notna(
                week_candidate) else pd.NaT

            latest_dates = []
            if pd.notna(latest):
                latest_dates.append(latest)
            if pd.notna(prev):
                latest_dates.append(prev)
            if pd.notna(week_date) and (week_date not in latest_dates):
                latest_dates.append(week_date)

            # Tạo dashboard cho 24h
            self._create_dashboard_subplot_fixed(ax1, df_all, latest_dates, date_col, kpi_mapping,
                                                 "Daily 4G KPI Dashboard (24h)", "#FFA500")

            # Tạo dashboard cho BH
            self._create_dashboard_subplot_fixed(ax2, df_bh, latest_dates, date_col, kpi_mapping,
                                                 "Daily 4G KPI Dashboard (BH)", "#FF6B35")

            plt.tight_layout()

            # Lưu dashboard
            dashboard_path = os.path.join(output_dir, "Daily_4G_KPI_Dashboard.png")
            plt.savefig(dashboard_path, dpi=300, bbox_inches='tight', facecolor='white')
            plt.close()

            print(f"✅ Đã tạo bảng Daily Dashboard: {dashboard_path}")
            return dashboard_path

        except Exception as e:
            print(f"❌ Lỗi khi tạo Daily Dashboard: {e}")
            return None

    def _create_dashboard_subplot_fixed(self, ax, df, latest_dates, date_col, kpi_mapping, title, header_color):
        """
        Tạo một subplot dashboard với 2 bảng riêng biệt
        """
        ax.clear()
        ax.set_xlim(0, 10)
        ax.set_ylim(0, 12)
        ax.axis('off')

        # Tiêu đề
        ax.text(5, 11.5, title, ha='center', va='center', fontsize=14, fontweight='bold')

        # ========== BẢNG 1: Success Rates ==========
        table1_data = []

        # Header row cho bảng 1
        header1 = ['Item', 'ePS CSSR', 'ePS CDR', 'CSFB SR', 'PS Traffic (GB)']

        # Target row cho bảng 1
        targets1 = ['Target (%)', '99.00', '1.20', '99.00', '-']
        table1_data.append(targets1)

        # Data rows cho bảng 1
        date_rows_data1 = []
        for i, date in enumerate(latest_dates):
            date_str = date.strftime('%d-%b-%y')
            row_data = [date_str]

            # Lấy dữ liệu cho từng KPI của bảng 1
            for kpi_name in ['ePS CSSR', 'ePS CDR', 'CSFB SR', 'PS Traffic (GB)']:
                possible_cols = kpi_mapping.get(kpi_name, [kpi_name])
                value = None

                for col_name in possible_cols:
                    if col_name in df.columns:
                        day_data = df[df[date_col].dt.date == date.date()]
                        if not day_data.empty:
                            val = day_data[col_name].iloc[0]
                            if pd.notna(val) and val != '' and str(val).strip() != '':
                                value = val
                                break

                if value is not None and not pd.isna(value):
                    try:
                        if kpi_name == 'PS Traffic (GB)':
                            row_data.append(f"{float(value):,.0f}")
                        else:
                            row_data.append(f"{float(value):.2f}")
                    except:
                        row_data.append('-')
                else:
                    row_data.append('-')

            table1_data.append(row_data)
            date_rows_data1.append(row_data)

        # Compare rows cho bảng 1
        if len(date_rows_data1) >= 2:  # Có ít nhất 2 ngày để so sánh
            # Compare with D-1
            comp_d1 = ['Compare with (D-1)']
            for j in range(1, 5):  # Skip Item column
                try:
                    curr_str = date_rows_data1[0][j]  # Latest
                    prev_str = date_rows_data1[1][j]  # Previous

                    if curr_str != '-' and prev_str != '-':
                        curr_val = float(curr_str.replace(',', ''))
                        prev_val = float(prev_str.replace(',', ''))
                        if j == 2:  # ePS CDR
                            diff = prev_val - curr_val
                        else:
                            diff = curr_val - prev_val
                        if j == 4:  # PS Traffic
                            diff = (curr_val - prev_val) / prev_val * 100
                            comp_d1.append(f"{diff:+,.0f}%")
                        else:
                            comp_d1.append(f"{diff:+.2f}%")
                    else:
                        comp_d1.append('-')
                except:
                    comp_d1.append('-')
            table1_data.append(comp_d1)

            # Compare with D-7 (nếu có đủ 3 ngày)
            if len(date_rows_data1) >= 3:
                comp_d7 = ['Compare with (D-7)']
                for j in range(1, 5):
                    try:
                        curr_str = date_rows_data1[0][j]  # Latest
                        week_str = date_rows_data1[2][j]  # Week ago

                        if curr_str != '-' and week_str != '-':
                            curr_val = float(curr_str.replace(',', ''))
                            week_val = float(week_str.replace(',', ''))
                            if j == 2:  # ePS CDR
                                diff = week_val - curr_val
                            else:
                                diff = curr_val - week_val
                            if j == 4:  # PS Traffic
                                diff = (curr_val - week_val) / week_val * 100
                                comp_d7.append(f"{diff:+,.0f}%")
                            else:
                                comp_d7.append(f"{diff:+.2f}%")
                        else:
                            comp_d7.append('-')
                    except:
                        comp_d7.append('-')
                table1_data.append(comp_d7)

        # Vẽ bảng 1
        self._draw_table_fixed(ax, header1, table1_data, header_color, x_start=0.2, y_start=9.5,
                               col_width=1.9, row_height=0.5)

        # ========== BẢNG 2: Handover rates ==========
        table2_data = []

        # Header row cho bảng 2
        header2 = ['Item', 'IntraF HOSR', 'InterF HOSR', 'InterRAT HOSR (L2W)', 'MIMO Rate']

        # Target row cho bảng 2
        targets2 = ['Target (%)', '98.00', '96.00', '96.00', '20.00']
        table2_data.append(targets2)

        # Data rows cho bảng 2
        date_rows_data2 = []
        for date in latest_dates:
            date_str = date.strftime('%d-%b-%y')
            row_data = [date_str]

            for kpi_name in ['IntraF HOSR', 'InterF HOSR', 'InterRAT HOSR (L2W)', 'MIMO Rate']:
                possible_cols = kpi_mapping.get(kpi_name, [kpi_name])
                value = None

                for col_name in possible_cols:
                    if col_name in df.columns:
                        day_data = df[df[date_col].dt.date == date.date()]
                        if not day_data.empty:
                            val = day_data[col_name].iloc[0]
                            if pd.notna(val) and val != '' and str(val).strip() != '':
                                value = val
                                break

                if value is not None and not pd.isna(value):
                    try:
                        row_data.append(f"{float(value):.2f}")
                    except:
                        row_data.append('-')
                else:
                    row_data.append('-')

            table2_data.append(row_data)
            date_rows_data2.append(row_data)

        # Compare rows cho bảng 2
        if len(date_rows_data2) >= 2:
            # Compare with D-1
            comp_d1 = ['Compare with (D-1)']
            for j in range(1, 5):
                try:
                    curr_str = date_rows_data2[0][j]
                    prev_str = date_rows_data2[1][j]

                    if curr_str != '-' and prev_str != '-':
                        curr_val = float(curr_str)
                        prev_val = float(prev_str)
                        if j == 2:  # InterF HOSR
                            diff = prev_val - curr_val
                        else:
                            diff = curr_val - prev_val
                        comp_d1.append(f"{diff:+.2f}%")
                    else:
                        comp_d1.append('-')
                except:
                    comp_d1.append('-')
            table2_data.append(comp_d1)

            # Compare with D-7
            if len(date_rows_data2) >= 3:
                comp_d7 = ['Compare with (D-7)']
                for j in range(1, 5):
                    try:
                        curr_str = date_rows_data2[0][j]
                        week_str = date_rows_data2[2][j]

                        if curr_str != '-' and week_str != '-':
                            curr_val = float(curr_str)
                            week_val = float(week_str)
                            if j == 2:  # InterF HOSR
                                diff = week_val - curr_val
                            else:
                                diff = curr_val - week_val
                            comp_d7.append(f"{diff:+.2f}%")
                        else:
                            comp_d7.append('-')
                    except:
                        comp_d7.append('-')
                table2_data.append(comp_d7)

        # Vẽ bảng 2
        self._draw_table_fixed(ax, header2, table2_data, header_color, x_start=0.2, y_start=5.5,
                               col_width=1.9, row_height=0.5)

    def _draw_table_fixed(self, ax, header, data, header_color, x_start, y_start, col_width, row_height):
        """
        Vẽ bảng lên subplot với xử lý màu sắc đúng
        """
        # Vẽ header
        for i, col_name in enumerate(header):
            x = x_start + i * col_width
            # Header background
            rect = plt.Rectangle((x, y_start), col_width, row_height,
                                 facecolor=header_color, edgecolor='black', linewidth=1)
            ax.add_patch(rect)
            # Header text
            ax.text(x + col_width / 2, y_start + row_height / 2, col_name,
                    ha='center', va='center', fontsize=9, fontweight='bold', color='white')

        # Vẽ data rows
        for row_idx, row_data in enumerate(data):
            y = y_start - (row_idx + 1) * row_height
            for col_idx, value in enumerate(row_data):
                x = x_start + col_idx * col_width

                # Xác định màu nền
                if row_idx == 0:  # Target row
                    bg_color = '#FFFACD'  # Light yellow
                elif 'Compare' in str(row_data[0]):  # Compare rows
                    bg_color = '#E6E6FA'  # Lavender
                else:  # Data rows
                    bg_color = 'white'

                # Cell background
                rect = plt.Rectangle((x, y), col_width, row_height,
                                     facecolor=bg_color, edgecolor='black', linewidth=1)
                ax.add_patch(rect)

                # Cell text với màu sắc theo giá trị
                text_color = 'black'
                font_weight = 'normal'
                arrow = "" #mũi tên

                # Nếu là compare row và có giá trị số
                if 'Compare' in str(row_data[0]) and col_idx > 0:
                    value_str = str(value)
                    value_float = value_str.strip().replace("%", "")
                    vf = float(value_float)
                    if value_str != '-':
                        try:
                            # Kiểm tra nếu là số dương/âm
                            if '+' in value_str:
                                if vf >= 0 and vf <= 1 :
                                    text_color = 'black'
                                    font_weight = 'bold'
                                    arrow = "→"
                                elif vf > 1:
                                    text_color = 'green'
                                    font_weight = 'bold'
                                    arrow = "↑"
                            elif value_str.startswith('-'):
                                if vf < 0 and vf >= -1:
                                    text_color = 'black'
                                    font_weight = 'bold'
                                    arrow = "→"
                                elif vf < -1:
                                    text_color = 'red'
                                    font_weight = 'bold'
                                    arrow = "↓"
                        except:
                            pass
                display_value = f"{value} {arrow}" if arrow else str(value)
                font_size = 8 if len(display_value) > 10 else 9
                ax.text(x + col_width / 2, y + row_height / 2, display_value,
                        ha='center', va='center', fontsize=font_size,
                        color=text_color, weight=font_weight)

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
            skip_cols = [date_col, 'Cell Type', 'RRC Att', 'ERAB Att', 'S1 Att', 'ERAB Release', 'pmHoPrepAttLteIntraF',
                         'DC_E_ERBS_UTRANCELLRELATION.pmHoPrepAtt', 'CSFB Att', 'CSFB Succ to GSM',
                         'PS Traffic UL (GB)', 'pmHoPrepAttLteInterF', 'X2 HOSR', 'X2 HO Att', 'S1 HOSR', 'S1 HO Att',
                         'RRC Connected User Max', 'RTWP', 'RRC Connected User Average', 'RRC Connected User Max','RRC Connected User Max.1']
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

            # Tạo Daily Dashboard
            dashboard_path = self.create_daily_dashboard_table(csv_all_day, csv_busy_hour, output_dir)
            if dashboard_path:
                created_chart_paths.insert(0, dashboard_path)  # Đặt dashboard ở đầu

            # Tạo báo cáo tổng hợp
            if created_chart_paths:
                self.create_comprehensive_report(created_chart_paths, output_dir)

            return created_chart_paths

        except Exception as e:
            print(f"❌ Lỗi khi tạo biểu đồ: {e}")
            return []

    def create_comprehensive_report(self, chart_paths, output_dir):
        """
        Tạo báo cáo tổng hợp gộp tất cả biểu đồ và dashboard thành một file ảnh duy nhất
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

            # Dashboard sẽ được đặt ở trang đầu với kích thước lớn hơn
            dashboard_img = images[0] if chart_paths[0].endswith('Dashboard.png') else None
            chart_images = images[1:] if dashboard_img else images

            # Tính toán layout cho charts (không bao gồm dashboard)
            num_charts = len(chart_images)
            if num_charts > 0:
                cols = min(2, num_charts)  # Tối đa 2 cột cho charts
                rows = math.ceil(num_charts / cols)
            else:
                cols = rows = 0

            print(f"   📐 Layout: Dashboard + {rows} hàng x {cols} cột cho {num_charts} biểu đồ")

            # Kích thước components
            dashboard_width = 1600
            dashboard_height = 1000
            chart_width = 800
            chart_height = 480
            margin = 50
            padding = 30
            header_height = 100
            section_spacing = 50

            # Tính toán kích thước tổng
            total_width = max(dashboard_width, cols * chart_width + (cols - 1) * padding) + 2 * margin
            total_height = (margin * 2 + header_height + dashboard_height +
                            section_spacing + rows * chart_height + (rows - 1) * padding + 100)

            # Tạo canvas
            report_image = Image.new('RGB', (total_width, total_height), 'white')
            draw = ImageDraw.Draw(report_image)

            # Font setup
            try:
                title_font = ImageFont.truetype("arial.ttf", 36)
                subtitle_font = ImageFont.truetype("arial.ttf", 20)
                section_font = ImageFont.truetype("arial.ttf", 24)
                footer_font = ImageFont.truetype("arial.ttf", 14)
            except:
                title_font = ImageFont.load_default()
                subtitle_font = ImageFont.load_default()
                section_font = ImageFont.load_default()
                footer_font = ImageFont.load_default()

            # Header
            title_text = "4G KPI COMPREHENSIVE REPORT"
            title_bbox = draw.textbbox((0, 0), title_text, font=title_font)
            title_width = title_bbox[2] - title_bbox[0]
            title_x = (total_width - title_width) // 2
            draw.text((title_x, margin), title_text, fill='black', font=title_font)

            # Subtitle
            current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            subtitle_text = f"Generated on {current_time} | Dashboard + {num_charts} KPI Charts"
            subtitle_bbox = draw.textbbox((0, 0), subtitle_text, font=subtitle_font)
            subtitle_width = subtitle_bbox[2] - subtitle_bbox[0]
            subtitle_x = (total_width - subtitle_width) // 2
            draw.text((subtitle_x, margin + 50), subtitle_text, fill='gray', font=subtitle_font)

            current_y = margin + header_height

            # Dashboard section
            if dashboard_img:
                # Section title
                dashboard_title = "DAILY DASHBOARD"
                dashboard_title_bbox = draw.textbbox((0, 0), dashboard_title, font=section_font)
                dashboard_title_width = dashboard_title_bbox[2] - dashboard_title_bbox[0]
                dashboard_title_x = (total_width - dashboard_title_width) // 2
                draw.text((dashboard_title_x, current_y), dashboard_title, fill='navy', font=section_font)

                current_y += 40

                # Resize và đặt dashboard
                dashboard_resized = dashboard_img.resize((dashboard_width, dashboard_height), Image.Resampling.LANCZOS)
                dashboard_x = (total_width - dashboard_width) // 2
                report_image.paste(dashboard_resized, (dashboard_x, current_y))

                # Border cho dashboard
                draw.rectangle([dashboard_x - 2, current_y - 2, dashboard_x + dashboard_width + 2,
                                current_y + dashboard_height + 2],
                               outline='navy', width=3)

                current_y += dashboard_height + section_spacing

            # Charts section
            if chart_images:
                # Section title
                charts_title = "KPI TREND CHARTS"
                charts_title_bbox = draw.textbbox((0, 0), charts_title, font=section_font)
                charts_title_width = charts_title_bbox[2] - charts_title_bbox[0]
                charts_title_x = (total_width - charts_title_width) // 2
                draw.text((charts_title_x, current_y), charts_title, fill='navy', font=section_font)

                current_y += 40

                # Đặt các biểu đồ
                charts_start_x = (total_width - (cols * chart_width + (cols - 1) * padding)) // 2

                for idx, img in enumerate(chart_images):
                    row = idx // cols
                    col = idx % cols

                    img_resized = img.resize((chart_width, chart_height), Image.Resampling.LANCZOS)

                    x = charts_start_x + col * (chart_width + padding)
                    y = current_y + row * (chart_height + padding)

                    report_image.paste(img_resized, (x, y))

                    # Border cho chart
                    draw.rectangle([x - 1, y - 1, x + chart_width + 1, y + chart_height + 1],
                                   outline='lightgray', width=1)

            # Footer
            footer_y = total_height - 60
            draw.line([(margin, footer_y - 20), (total_width - margin, footer_y - 20)], fill='lightgray', width=2)

            footer_text = "Daily Dashboard & KPI Trends Analysis • All Day vs Busy Hours Comparison"
            footer_bbox = draw.textbbox((0, 0), footer_text, font=footer_font)
            footer_width = footer_bbox[2] - footer_bbox[0]
            footer_x = (total_width - footer_width) // 2
            draw.text((footer_x, footer_y), footer_text, fill='gray', font=footer_font)

            # Lưu báo cáo PNG
            report_path = os.path.join(output_dir, "4G_KPI_Comprehensive_Report.png")
            report_image.save(report_path, "PNG", quality=95)

            # Tạo phiên bản PDF
            try:
                pdf_path = os.path.join(output_dir, "4G_KPI_Comprehensive_Report.pdf")
                report_image.save(pdf_path, "PDF", quality=95)
                print(f"✅ Đã tạo báo cáo PDF: {pdf_path}")
            except Exception as e:
                print(f"⚠️ Không thể tạo PDF: {e}")

            print(f"✅ Đã tạo báo cáo tổng hợp: {report_path}")
            print(f"   📏 Kích thước: {total_width} x {total_height} pixels")
            print(f"   📊 Chứa: Dashboard + {num_charts} biểu đồ KPI")

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

    print("=" * 70)
    print("🚀 CHƯƠNG TRÌNH CHUYỂN ĐỔI EXCEL SANG CSV VÀ TẠO BÁO CÁO TỔNG HỢP")
    print("=" * 70)

    # Đường dẫn file Excel
    excel_files = {
        '4G_KPI Cell FDD Data_24h_scheduled.xlsx': '4G_KPI_Cell_FDD_Data_24h_clean.csv',
        '4G_KPI Cell FDD Data_BH_scheduled.xlsx': '4G_KPI_Cell_FDD_Data_BH_clean.csv'
    }

    print("\n📋 BƯỚC 1: CHUYỂN ĐỔI EXCEL SANG CSV")
    print("-" * 50)

    converted_files = {}

    for excel_file, csv_file in excel_files.items():
        if os.path.exists(excel_file):
            df = processor.clean_excel_to_csv(excel_file, csv_file)
            if df is not None:
                converted_files[excel_file] = csv_file
                # Kiểm tra cấu trúc file CSV xem có bị lỗi không
                processor.verify_csv_structure(csv_file)
        else:
            print(f"⚠️ File không tồn tại: {excel_file}")

    print("\n📊 BƯỚC 2: TẠO DASHBOARD, BIỂU ĐỒ VÀ BÁO CÁO TỔNG HỢP")
    print("-" * 50)

    if len(converted_files) >= 2:
        csv_files = list(converted_files.values())
        chart_paths = processor.create_charts_from_csv(csv_files[0], csv_files[1])

        # Tạo bảng tóm tắt thống kê
        processor.create_summary_table(csv_files[0], csv_files[1], "charts_output")

    else:
        print("❌ Cần ít nhất 2 file CSV để tạo biểu đồ so sánh")

    print("\n" + "=" * 70)
    print("🎉 HOÀN THÀNH! Kiểm tra thư mục 'charts_output' để xem:")
    print("   📊 Daily Dashboard (Daily_4G_KPI_Dashboard.png)")
    print("   📈 Các biểu đồ riêng lẻ (KPI trend charts)")
    print("   📋 Báo cáo tổng hợp (4G_KPI_Comprehensive_Report.png)")
    print("   📄 Báo cáo PDF (4G_KPI_Comprehensive_Report.pdf)")
    print("   📊 Bảng tóm tắt thống kê (KPI_Summary_Statistics.csv)")
    print("=" * 70)


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

    print("\n" + "=" * 70)

    # Chạy chương trình chính
    main()

    # Nếu bạn có file CSV bị lỗi, sử dụng hàm này để sửa:
    # fix_corrupted_csv('4G_KPI_Cell_FDD_Data_BH_error.csv', '4G_KPI_Cell_FDD_Data_BH_fixed.csv')