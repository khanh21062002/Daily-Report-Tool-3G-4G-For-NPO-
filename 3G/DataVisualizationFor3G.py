import os
from datetime import timedelta
import numpy as np
import pandas as pd
import csv
from matplotlib import pyplot as plt
import matplotlib.dates as mdates
from matplotlib.patches import Rectangle
import seaborn as sns

# Set style for better looking plots
plt.style.use('default')
sns.set_palette("husl")


class ExcelCSVProcessorFor3G:
    def __init__(self):
        self.cleaned_data = {}
        # RNC mapping for different vendors
        self.ericsson_rncs = ['HLRE01', 'HLRE02', 'HLRE03', 'HLRE04', 'HNRZ01']
        self.zte_rncs = ['HNRZ01(101)', 'HNRZ01(102)']

    # ---------- Header detection helpers ----------
    def _find_header_row_generic(self, df, keywords, max_rows=30):
        for i in range(min(max_rows, len(df))):
            row = df.iloc[i].astype(str).str.strip().str.lower().tolist()
            if any(kw.lower() in row for kw in keywords):
                return i
        return 0

    def _find_header_row_ericsson(self, df, max_rows=30):
        return self._find_header_row_generic(df, ["date"], max_rows)

    def _find_header_row_zte(self, df, max_rows=80):
        for i in range(min(max_rows, len(df))):
            row = df.iloc[i].astype(str).str.strip()
            first_cell = str(row.iloc[0]).strip().lower()
            values_lower = [str(v).strip().lower() for v in row.tolist()]
            if first_cell == "index" and "start time" in values_lower and "end time" in values_lower:
                return i
        for i in range(min(max_rows, len(df))):
            first_cell = str(df.iloc[i, 0]).strip().lower()
            if first_cell == "index":
                return i
        return 0

    # ---------- Row classifiers ----------
    @staticmethod
    def _is_type_row(series):
        tokens = {"int", "float", "double", "number", "str", "string", "date", "datetime", "timestamp"}
        vals = [str(x).strip().lower() for x in series.tolist()]
        nonempty = [v for v in vals if v not in ("", "nan")]
        if not nonempty:
            return False
        match_ratio = sum(v in tokens for v in nonempty) / len(nonempty)
        return match_ratio >= 0.6

    @staticmethod
    def _is_unit_row(series):
        unit_tokens = {"%", "ms", "s", "gb", "mb", "kbps", "erlang", "dbm", "num", "den"}
        vals = [str(x).strip().lower() for x in series.tolist()]
        nonempty = [v for v in vals if v not in ("", "nan")]
        if not nonempty:
            return False
        shortish = sum(len(v) <= 6 for v in nonempty) / len(nonempty) >= 0.7
        contains_units = sum(any(t in v for t in unit_tokens) for v in nonempty) / len(nonempty) >= 0.4
        return shortish and contains_units

    # ---------- Converters ----------
    def clean_excel_to_csv_ericsson(self, excel_path, csv_path, sheet_name=0):
        try:
            print(f"🔄 Đang xử lý file Ericsson: {excel_path}")
            preview = pd.read_excel(excel_path, sheet_name=sheet_name, nrows=30, header=None)
            header_row = self._find_header_row_ericsson(preview, max_rows=30)

            df = pd.read_excel(excel_path, sheet_name=sheet_name, header=header_row)
            df = df.loc[:, ~df.columns.astype(str).str.contains(r"^Unnamed", na=False)]
            df = df.dropna(how="all")

            date_col = df.columns[0]
            df[date_col] = pd.to_datetime(df[date_col], errors="coerce", dayfirst=True)
            df = df.dropna(subset=[date_col]).sort_values(by=date_col).reset_index(drop=True)

            df.to_csv(csv_path, index=False, encoding="utf-8", date_format="%Y-%m-%d %H:%M:%S", lineterminator="",
                      quoting=csv.QUOTE_MINIMAL)
            print(f"✅ Đã lưu Ericsson CSV: {csv_path}, kích thước {df.shape}")
            self.cleaned_data[csv_path] = df
            return df
        except Exception as e:
            print(f"❌ Ericsson lỗi: {e}")
            return None

    def clean_excel_to_csv_ZTE(self, excel_path, csv_path, sheet_name=0):
        try:
            print(f"🔄 Đang xử lý file ZTE: {excel_path}")
            preview = pd.read_excel(excel_path, sheet_name=sheet_name, nrows=80, header=None)
            header_row = self._find_header_row_zte(preview, max_rows=80)

            df = pd.read_excel(excel_path, sheet_name=sheet_name, header=header_row)
            df = df.dropna(how="all")
            df = df.loc[:, ~df.columns.astype(str).str.match(r"^Unnamed", na=False)]

            if len(df) > 0 and self._is_type_row(df.iloc[0]):
                df = df.iloc[1:].reset_index(drop=True)

            if len(df) > 0 and self._is_unit_row(df.iloc[0]):
                df = df.iloc[1:].reset_index(drop=True)

            def _normalize_datetime_column(df_, logical_name):
                for c in df_.columns:
                    if str(c).strip().lower() == logical_name:
                        df_[c] = pd.to_datetime(df_[c], errors="coerce", dayfirst=True)
                        return c
                return None

            start_col = _normalize_datetime_column(df, "start time")
            end_col = _normalize_datetime_column(df, "end time")

            if "Index" in df.columns:
                idx_numeric = pd.to_numeric(df["Index"], errors="coerce")
                df = df[idx_numeric.notna()].copy()
                df["Index"] = idx_numeric.astype(int)

            if start_col is not None:
                df = df.sort_values(by=start_col).reset_index(drop=True)

            df.to_csv(csv_path, index=False, encoding="utf-8", date_format="%Y-%m-%d %H:%M:%S", lineterminator="",
                      quoting=csv.QUOTE_MINIMAL)
            print(f"✅ Đã lưu ZTE CSV: {csv_path}, kích thước {df.shape}")
            self.cleaned_data[csv_path] = df
            return df
        except Exception as e:
            print(f"❌ ZTE lỗi: {e}")
            return None

    # ---------- NEW: RNC Dashboard Creation ----------
    def create_daily_rnc_dashboard(self, csv_all_day_ericsson, csv_bh_ericsson,
                                   csv_all_day_zte, csv_bh_zte, output_dir):
        """
        Tạo bảng Daily 3G KPI Dashboard By RNC cho cả Ericsson và ZTE
        """
        try:
            print("\n📊 Đang tạo Daily 3G KPI Dashboard By RNC...")

            # Load data
            df_ericsson_24h = pd.read_csv(csv_all_day_ericsson)
            df_ericsson_bh = pd.read_csv(csv_bh_ericsson)
            df_zte_24h = pd.read_csv(csv_all_day_zte)
            df_zte_bh = pd.read_csv(csv_bh_zte)

            # Process dates
            date_col_ericsson = df_ericsson_24h.columns[0]
            date_col_zte = df_zte_24h.columns[1]  # ZTE usually has different structure

            df_ericsson_24h[date_col_ericsson] = pd.to_datetime(df_ericsson_24h[date_col_ericsson])
            df_ericsson_bh[date_col_ericsson] = pd.to_datetime(df_ericsson_bh[date_col_ericsson])
            df_zte_24h[date_col_zte] = pd.to_datetime(df_zte_24h[date_col_zte])
            df_zte_bh[date_col_zte] = pd.to_datetime(df_zte_bh[date_col_zte])

            # Create RNC dashboard tables
            self._create_rnc_dashboard_table(df_ericsson_24h, df_ericsson_bh,
                                             date_col_ericsson, "Ericsson", output_dir)
            self._create_rnc_dashboard_table(df_zte_24h, df_zte_bh,
                                             date_col_zte, "ZTE", output_dir)

            # Create trend charts
            self._create_rnc_trend_charts(df_ericsson_24h, df_ericsson_bh,
                                          date_col_ericsson, "Ericsson", output_dir)
            self._create_rnc_trend_charts(df_zte_24h, df_zte_bh,
                                          date_col_zte, "ZTE", output_dir)

            print("✅ Đã tạo xong tất cả Dashboard và biểu đồ By RNC")

        except Exception as e:
            print(f"❌ Lỗi khi tạo RNC Dashboard: {e}")

    def _create_rnc_dashboard_table(self, df_24h, df_bh, date_col, vendor, output_dir):
        """
        Tạo bảng dashboard theo RNC như trong hình
        """
        try:
            print(f"📊 Đang tạo bảng Daily {vendor} KPI Dashboard By RNC...")

            # Get RNC list based on vendor
            if vendor == "Ericsson":
                rnc_col = 'RNC Id'
                rncs = self.ericsson_rncs
                kpi_mapping = self._get_ericsson_kpi_mapping()
            else:
                rnc_col = 'RNC Managed NE Name'
                rncs = self.zte_rncs
                kpi_mapping = self._get_zte_kpi_mapping()

            # Filter data for target RNCs
            df_24h_filtered = df_24h[df_24h[rnc_col].isin(rncs)].copy()
            df_bh_filtered = df_bh[df_bh[rnc_col].isin(rncs)].copy()

            for col in df_24h_filtered.columns:
                if col not in [date_col, 'RNC Id']:
                    df_24h_filtered[col] = pd.to_numeric(df_24h_filtered[col], errors='coerce')
            for col in df_bh_filtered.columns:
                if col not in [date_col, 'RNC Id']:
                    df_bh_filtered[col] = pd.to_numeric(df_bh_filtered[col], errors='coerce')

            for col in df_24h_filtered.columns:
                if col not in [date_col, 'RNC Managed NE Name']:
                    df_24h_filtered[col] = pd.to_numeric(df_24h_filtered[col], errors='coerce')
            for col in df_bh_filtered.columns:
                if col not in [date_col, 'RNC Managed NE Name']:
                    df_bh_filtered[col] = pd.to_numeric(df_bh_filtered[col], errors='coerce')

            # Get latest 3 dates
            latest_dates = self._get_latest_dates(df_24h_filtered, date_col, 3)

            # Create figure
            fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(16, 14))
            fig.suptitle(f'Daily 3G KPI Dashboard of {vendor} (BH & 24h)',
                         fontsize=16, fontweight='bold', y=0.95)

            # Create BH dashboard
            self._draw_rnc_dashboard(ax1, df_bh_filtered, latest_dates, date_col,
                                     rnc_col, rncs, kpi_mapping,
                                     f"Daily 3G KPI Dashboard of {vendor} (BH)", "#FFA500")

            # Create 24h dashboard
            self._draw_rnc_dashboard(ax2, df_24h_filtered, latest_dates, date_col,
                                     rnc_col, rncs, kpi_mapping,
                                     f"Daily 3G KPI Dashboard of {vendor} (24h)", "#FF6B35")

            plt.tight_layout()
            plt.subplots_adjust(top=0.93)

            # Save
            dashboard_path = os.path.join(output_dir, f"Daily_3G_KPI_Dashboard_{vendor}_By_RNC.png")
            plt.savefig(dashboard_path, dpi=300, bbox_inches='tight', facecolor='white')
            plt.close()

            print(f"✅ Đã tạo Dashboard By RNC cho {vendor}: {dashboard_path}")

        except Exception as e:
            print(f"❌ Lỗi tạo dashboard table {vendor}: {e}")

    def _draw_rnc_dashboard(self, ax, df, latest_dates, date_col, rnc_col, rncs, kpi_mapping, title, header_color):
        """
        Vẽ bảng dashboard theo RNC
        """
        ax.clear()
        ax.set_xlim(0, 12)
        ax.set_ylim(0, 12)
        ax.axis('off')

        # Title
        ax.text(6, 11.5, title, ha='center', va='center', fontsize=12, fontweight='bold')

        # Prepare data for each KPI
        kpi_groups = [
            (['CS Traffic (Erl)', 'PS Traffic (GB)', 'CS CSSR', 'HSDPA CSSR'],
             [None, None, 99.00, 98.00], 10.5),
            (['CS CDR', 'HSDPA CDR', 'CS Soft HOSR', 'HSDPA Soft HOSR'],
             [0.80, 1.50, 99.00, 98.00], 7.5),
            (['CS IRAT HOSR', 'PS IRAT HOSR'],
             [97.00, 92.00], 4.5)
        ]

        for kpis, targets, y_pos in kpi_groups:
            self._draw_rnc_kpi_table(ax, df, latest_dates, date_col, rnc_col, rncs,
                                     kpi_mapping, kpis, targets, header_color, y_pos)

    def _draw_rnc_kpi_table(self, ax, df, latest_dates, date_col, rnc_col, rncs,
                            kpi_mapping, kpis, targets, header_color, y_start):
        """
        Vẽ bảng KPI theo RNC
        """
        # Table dimensions
        col_width = 1.8
        row_height = 0.3
        x_start = 0.5

        # Headers
        headers = ['Item'] + rncs + ['KPI']

        # Draw headers
        for i, header in enumerate(headers):
            x = x_start + i * col_width
            rect = Rectangle((x, y_start), col_width, row_height,
                             facecolor=header_color, edgecolor='black', linewidth=1)
            ax.add_patch(rect)

            font_size = 8 if len(header) > 10 else 9
            ax.text(x + col_width / 2, y_start + row_height / 2, header,
                    ha='center', va='center', fontsize=font_size,
                    fontweight='bold', color='white')

        # Draw data for each KPI
        for kpi_idx, kpi_name in enumerate(kpis):
            y_kpi_start = y_start - (kpi_idx + 1) * (len(latest_dates) + 3) * row_height

            # KPI name and target
            self._draw_kpi_section(ax, df, latest_dates, date_col, rnc_col, rncs,
                                   kpi_mapping, kpi_name, targets[kpi_idx],
                                   x_start, y_kpi_start, col_width, row_height)

    def _draw_kpi_section(self, ax, df, latest_dates, date_col, rnc_col, rncs,
                          kpi_mapping, kpi_name, target, x_start, y_start, col_width, row_height):
        """
        Vẽ section cho một KPI
        """
        # Draw KPI rows
        row_data = []

        # Target row
        target_row = ['Target (%)'] + ([f"{target:.2f}" if target else '-'] * len(rncs)) + [kpi_name]
        row_data.append(target_row)

        # Date rows
        for date_idx, date in enumerate(latest_dates):
            date_str = date.strftime('%d-%b-%y')
            row = [date_str]

            for rnc in rncs:
                value = self._get_rnc_kpi_value(df, date, date_col, rnc_col, rnc, kpi_name, kpi_mapping)
                if value is not None:
                    if kpi_name in ['CS Traffic (Erl)', 'PS Traffic (GB)']:
                        row.append(f"{value:,.0f}")
                    else:
                        row.append(f"{value:.2f}")
                else:
                    row.append('-')
            row.append('')
            row_data.append(row)

        # Add comparison rows
        if len(latest_dates) >= 2:
            # D-1 comparison
            comp_d1 = ['Delta (D-1)']
            for rnc_idx, rnc in enumerate(rncs):
                if len(row_data) >= 3:
                    try:
                        curr = row_data[1][rnc_idx + 1]
                        prev = row_data[2][rnc_idx + 1]

                        if curr != '-' and prev != '-':
                            curr_val = float(curr.replace(',', ''))
                            prev_val = float(prev.replace(',', ''))

                            if kpi_name in ['CS CDR', 'HSDPA CDR']:
                                diff = prev_val - curr_val
                            else:
                                diff = curr_val - prev_val

                            symbol = '↑' if diff > 1 else ('↓' if diff < -1 else '→')
                            comp_d1.append(f"{diff:+.0f}% {symbol}")
                        else:
                            comp_d1.append('-')
                    except:
                        comp_d1.append('-')
                else:
                    comp_d1.append('-')
            comp_d1.append('')
            row_data.append(comp_d1)

        # Draw all rows
        for row_idx, row in enumerate(row_data):
            y = y_start - row_idx * row_height

            for col_idx, cell_value in enumerate(row):
                x = x_start + col_idx * col_width

                # Determine colors
                if row_idx == 0:  # Target row
                    bg_color = '#FFFACD'
                    text_color = 'black'
                    font_weight = 'bold'
                elif 'Delta' in str(row[0]):  # Comparison row
                    bg_color = '#E6E6FA'
                    text_color = 'green' if '↑' in str(cell_value) else ('red' if '↓' in str(cell_value) else 'black')
                    font_weight = 'bold'
                else:  # Data row
                    bg_color = 'white'
                    text_color = 'black'
                    font_weight = 'normal'

                    # Check against target
                    if col_idx > 0 and col_idx <= len(rncs) and target is not None:
                        try:
                            val = float(str(cell_value).replace(',', ''))
                            if ((kpi_name in ['CS CDR', 'HSDPA CDR'] and val > target) or
                                    (kpi_name not in ['CS CDR', 'HSDPA CDR'] and val < target)):
                                bg_color = '#FFB3B3'
                                text_color = '#B22222'
                                font_weight = 'bold'
                        except:
                            pass

                # Draw cell
                rect = Rectangle((x, y), col_width, row_height,
                                 facecolor=bg_color, edgecolor='black', linewidth=1)
                ax.add_patch(rect)

                # Draw text
                font_size = 7 if len(str(cell_value)) > 10 else 8
                ax.text(x + col_width / 2, y + row_height / 2, str(cell_value),
                        ha='center', va='center', fontsize=font_size,
                        color=text_color, weight=font_weight)

    def _create_rnc_trend_charts(self, df_24h, df_bh, date_col, vendor, output_dir):
        """
        Tạo biểu đồ trend cho các KPI theo RNC
        """
        try:
            print(f"📈 Đang tạo biểu đồ trend cho {vendor}...")

            # Get RNC info
            if vendor == "Ericsson":
                rnc_col = 'RNC Id'
                rncs = self.ericsson_rncs
                kpi_mapping = self._get_ericsson_kpi_mapping()
            else:
                rnc_col = 'RNC Managed NE Name'
                rncs = self.zte_rncs
                kpi_mapping = self._get_zte_kpi_mapping()

            # Create BH charts
            self._create_vendor_charts(df_bh, date_col, rnc_col, rncs, kpi_mapping,
                                       vendor, "BH", output_dir)

            # Create 24h charts
            self._create_vendor_charts(df_24h, date_col, rnc_col, rncs, kpi_mapping,
                                       vendor, "24h", output_dir)

        except Exception as e:
            print(f"❌ Lỗi tạo trend charts {vendor}: {e}")

    def _create_vendor_charts(self, df, date_col, rnc_col, rncs, kpi_mapping, vendor, period, output_dir):
        """
        Tạo biểu đồ cho một vendor và period
        """
        # Define chart groups similar to the images
        chart_groups = [
            # Group 1: Traffic and Success Rates
            {
                'kpis': ['CS Traffic (Erl)', 'PS Traffic (GB)', 'CS CSSR', 'HSDPA CSSR'],
                'targets': [None, None, 99.0, 98.0],
                'filename': f'Chart_{vendor}_{period}_Traffic_Success.png'
            },
            # Group 2: Drop Rates
            {
                'kpis': ['CS CDR', 'HSDPA CDR'],
                'targets': [0.8, 1.5],
                'filename': f'Chart_{vendor}_{period}_Drop_Rates.png'
            },
            # Group 3: Handover Success Rates
            {
                'kpis': ['CS Soft HOSR', 'PS Soft HOSR', 'CS Hard HOSR', 'PS Hard HOSR'],
                'targets': [99.0, 98.0, None, None],
                'filename': f'Chart_{vendor}_{period}_Handover.png'
            },
            # Group 4: Inter-RAT
            {
                'kpis': ['CS IRAT HOSR', 'PS IRAT HOSR'],
                'targets': [97.0, 92.0],
                'filename': f'Chart_{vendor}_{period}_IRAT.png'
            },
            # Group 5: HSDPA Performance
            {
                'kpis': ['HSDPA User', 'HSDPA Throughput'],
                'targets': [None, None],
                'filename': f'Chart_{vendor}_{period}_HSDPA.png'
            }
        ]

        for group in chart_groups:
            self._create_kpi_group_chart(df, date_col, rnc_col, rncs, kpi_mapping,
                                         group['kpis'], group['targets'],
                                         f"Chart {period}", group['filename'], output_dir)

    def _create_kpi_group_chart(self, df, date_col, rnc_col, rncs, kpi_mapping,
                                kpis, targets, title, filename, output_dir):
        """
        Tạo biểu đồ cho một nhóm KPI
        """
        try:
            # Filter available KPIs
            available_kpis = []
            available_targets = []
            for i, kpi in enumerate(kpis):
                if self._check_kpi_available(df, kpi, kpi_mapping):
                    available_kpis.append(kpi)
                    available_targets.append(targets[i])

            if not available_kpis:
                print(f"⚠️ Không có KPI nào khả dụng cho {filename}")
                return

            # Create subplot layout
            n_kpis = len(available_kpis)
            if n_kpis <= 2:
                fig, axes = plt.subplots(1, n_kpis, figsize=(14, 6))
            elif n_kpis <= 4:
                fig, axes = plt.subplots(2, 2, figsize=(14, 10))
            else:
                fig, axes = plt.subplots(3, 2, figsize=(14, 12))

            if n_kpis == 1:
                axes = [axes]
            else:
                axes = axes.flatten() if hasattr(axes, 'flatten') else axes

            fig.suptitle(title, fontsize=16, fontweight='bold', y=0.95)

            # Plot each KPI
            for i, (kpi, target) in enumerate(zip(available_kpis, available_targets)):
                if i < len(axes):
                    self._plot_single_kpi(axes[i], df, date_col, rnc_col, rncs,
                                          kpi_mapping, kpi, target)

            # Hide unused subplots
            for i in range(len(available_kpis), len(axes)):
                axes[i].set_visible(False)

            plt.tight_layout()
            plt.subplots_adjust(top=0.93)

            # Save chart
            chart_path = os.path.join(output_dir, filename)
            plt.savefig(chart_path, dpi=300, bbox_inches='tight', facecolor='white')
            plt.close()

            print(f"✅ Đã tạo biểu đồ: {filename}")

        except Exception as e:
            print(f"❌ Lỗi tạo biểu đồ {filename}: {e}")

    def _plot_single_kpi(self, ax, df, date_col, rnc_col, rncs, kpi_mapping, kpi_name, target):
        """
        Vẽ biểu đồ cho một KPI
        """
        try:
            # Prepare data
            plot_data = {}
            dates = sorted(df[date_col].dt.date.unique())

            for rnc in rncs:
                rnc_data = df[df[rnc_col] == rnc]
                values = []

                for date in dates:
                    day_data = rnc_data[rnc_data[date_col].dt.date == date]
                    if not day_data.empty:
                        value = self._get_rnc_kpi_value_from_data(day_data, kpi_name, kpi_mapping)
                        values.append(value if value is not None else np.nan)
                    else:
                        values.append(np.nan)

                plot_data[rnc] = values

            # Plot lines for each RNC
            colors = ['blue', 'orange', 'green', 'red', 'purple']
            for i, (rnc, values) in enumerate(plot_data.items()):
                color = colors[i % len(colors)]
                ax.plot(dates, values, marker='o', label=rnc, color=color, linewidth=2)

            # Add target line
            if target is not None:
                ax.axhline(y=target, color='red', linestyle='--', linewidth=1, label='Target')

            # Format chart
            ax.set_title(f"{kpi_name}", fontsize=12, fontweight='bold')
            ax.set_xlabel('Date')
            ax.set_ylabel('Value')
            ax.legend(bbox_to_anchor=(1.05, 1), loc='upper left')
            ax.grid(True, alpha=0.3)

            # Format x-axis
            if len(dates) > 10:
                ax.xaxis.set_major_locator(mdates.WeekdayLocator())
                ax.xaxis.set_major_formatter(mdates.DateFormatter('%d-%b'))
            else:
                ax.xaxis.set_major_formatter(mdates.DateFormatter('%d-%b'))

            plt.setp(ax.xaxis.get_majorticklabels(), rotation=45)

        except Exception as e:
            print(f"❌ Lỗi vẽ biểu đồ KPI {kpi_name}: {e}")

    # ---------- Helper Methods ----------
    def _get_latest_dates(self, df, date_col, n_dates=3):
        """
        Lấy n ngày gần nhất từ dataframe
        """
        unique_dates = sorted(df[date_col].dt.date.unique(), reverse=True)
        return [pd.Timestamp(date) for date in unique_dates[:n_dates]]

    def _get_rnc_kpi_value(self, df, date, date_col, rnc_col, rnc, kpi_name, kpi_mapping):
        """
        Lấy giá trị KPI cho một RNC và ngày cụ thể
        """
        try:
            # Filter data for specific RNC and date
            rnc_day_data = df[(df[rnc_col] == rnc) & (df[date_col].dt.date == date.date())]

            if rnc_day_data.empty:
                return None

            return self._get_rnc_kpi_value_from_data(rnc_day_data, kpi_name, kpi_mapping)

        except Exception as e:
            print(f"❌ Lỗi lấy KPI {kpi_name} cho {rnc}: {e}")
            return None

    def _get_rnc_kpi_value_from_data(self, day_data, kpi_name, kpi_mapping):
        """
        Lấy giá trị KPI từ dữ liệu đã lọc
        """
        possible_cols = kpi_mapping.get(kpi_name, [kpi_name])

        for col_name in possible_cols:
            if col_name in day_data.columns:
                val = day_data[col_name].mean()
                if pd.notna(val):
                    return float(val)

            # Try fuzzy matching
            for actual_col in day_data.columns:
                if col_name.lower().replace(' ', '').replace('(', '').replace(')', '') in \
                        actual_col.lower().replace(' ', '').replace('(', '').replace(')', ''):
                    val = day_data[actual_col].mean()
                    if pd.notna(val):
                        return float(val)

        return None

    def _check_kpi_available(self, df, kpi_name, kpi_mapping):
        """
        Kiểm tra xem KPI có khả dụng trong dataframe không
        """
        possible_cols = kpi_mapping.get(kpi_name, [kpi_name])

        for col_name in possible_cols:
            if col_name in df.columns:
                return True

            for actual_col in df.columns:
                if col_name.lower().replace(' ', '').replace('(', '').replace(')', '') in \
                        actual_col.lower().replace(' ', '').replace('(', '').replace(')', ''):
                    return True

        return False

    def _get_ericsson_kpi_mapping(self):
        """
        KPI mapping cho Ericsson
        """
        return {
            'CS CSSR': ['CS CSSR (%)', 'CS CSSR', 'CS Call Setup Success Rate', 'CS Call Setup Success Rate (%)'],
            'HSDPA CSSR': ['HSDPA CSSR (%)', 'HSDPA CSSR', 'HSDPA Call Setup Success Rate',
                           'HSDPA Call Setup Success Rate (%)', 'PS CSSR_HSDPA'],
            'CS CDR': ['CS CDR (%)', 'CS CDR', 'CS Call Drop Rate', 'CS Call Drop Rate (%)'],
            'HSDPA CDR': ['HSDPA CDR (%)', 'HSDPA CDR', 'HSDPA Call Drop Rate', 'HSDPA Call Drop Rate (%)',
                          'PS CDR_HSPDA'],
            'CS Traffic (Erl)': ['CS Traffic (Erl)', 'CS Traffic (Erlang)', 'CS Traffic'],
            'CS Soft HOSR': ['CS Soft HOSR (%)', 'CS Soft HOSR', 'CS Soft Handover Success Rate',
                             'CS Soft Handover Success Rate (%)'],
            'HSDPA Soft HOSR': ['HSDPA Soft HOSR (%)', 'HSDPA Soft HOSR', 'HSDPA Soft Handover Success Rate',
                                'HSDPA Soft Handover Success Rate (%)', 'PS Soft HOSR'],
            'PS Soft HOSR': ['PS Soft HOSR (%)', 'PS Soft HOSR', 'PS Soft Handover Success Rate',
                             'PS Soft Handover Success Rate (%)', 'HSDPA Soft HOSR'],
            'CS Hard HOSR': ['CS Hard HOSR (%)', 'CS Hard HOSR', 'CS Hard Handover Success Rate',
                             'CS Hard Handover Success Rate (%)'],
            'PS Hard HOSR': ['PS Hard HOSR (%)', 'PS Hard HOSR', 'PS Hard Handover Success Rate',
                             'PS Hard Handover Success Rate (%)'],
            'CS IRAT HOSR': ['CS IRAT HOSR (%)', 'CS IRAT HOSR', 'CS Inter-RAT Handover Success Rate',
                             'CS Inter-RAT Handover Success Rate (%)'],
            'PS IRAT HOSR': ['PS IRAT HOSR (%)', 'PS IRAT HOSR', 'PS Inter-RAT Handover Success Rate',
                             'PS Inter-RAT Handover Success Rate (%)'],
            'PS Traffic (GB)': ['PS Traffic (GB)', 'PS Traffic (Gigabytes)', 'PS Traffic'],
            'HSDPA User': ['HSDPA User', 'HSDPA Users', 'HSDPA Active Users'],
            'HSDPA Throughput': ['HSDPA Throughput (kbps)', 'HSDPA Throughput', 'HSDPA Average Throughput'],
            'Cell Availability': ['Cell Availability (%)', 'Cell Availability', 'Availability']
        }

    def _get_zte_kpi_mapping(self):
        """
        KPI mapping cho ZTE
        """
        return {
            'CS CSSR': ['CS CSSR_VNM', 'CS CSSR', 'CS Call Setup Success Rate', 'CS Call Setup Success Rate (%)'],
            'HSDPA CSSR': ['HSDPA CSSR (%)', 'HSDPA CSSR', 'HSDPA Call Setup Success Rate',
                           'HSDPA Call Setup Success Rate (%)', 'PS CSSR'],
            'CS CDR': ['CS CDR_VNM', 'CS CDR', 'CS Call Drop Rate', 'CS Call Drop Rate (%)'],
            'HSDPA CDR': ['PS CDR_HSDPA_VNM', 'HSDPA CDR', 'HSDPA Call Drop Rate', 'HSDPA Call Drop Rate (%)',
                          'PS CDR_HSPDA'],
            'CS Traffic (Erl)': ['CS Traffic (Erl)', 'CS Traffic (Erlang)', 'CS Traffic (Erl)_VNM'],
            'CS Soft HOSR': ['CS Soft HOSR_VNM', 'CS Soft HOSR', 'CS Soft Handover Success Rate',
                             'CS Soft Handover Success Rate (%)'],
            'HSDPA Soft HOSR': ['PS Soft HOSR_VNM', 'HSDPA Soft HOSR', 'HSDPA Soft Handover Success Rate',
                                'HSDPA Soft Handover Success Rate (%)', 'PS Soft HOSR'],
            'PS Soft HOSR': ['PS Soft HOSR_VNM', 'PS Soft HOSR', 'PS Soft Handover Success Rate',
                             'PS Soft Handover Success Rate (%)'],
            'CS Hard HOSR': ['CS Hard HOSR_VNM', 'CS Hard HOSR', 'CS Hard Handover Success Rate',
                             'CS Hard Handover Success Rate (%)'],
            'PS Hard HOSR': ['PS Hard HOSR_VNM', 'PS Hard HOSR', 'PS Hard Handover Success Rate',
                             'PS Hard Handover Success Rate (%)'],
            'CS IRAT HOSR': ['CS InterRAT HOSR_VNM', 'CS IRAT HOSR', 'CS Inter-RAT Handover Success Rate',
                             'CS Inter-RAT Handover Success Rate (%)'],
            'PS IRAT HOSR': ['PS InterRAT HOSR_VNM', 'PS IRAT HOSR', 'PS Inter-RAT Handover Success Rate',
                             'PS Inter-RAT Handover Success Rate (%)'],
            'PS Traffic (GB)': ['PS Traffic (GB)', 'PS Traffic (Gigabytes)', 'PS Traffic'],
            'HSDPA User': ['HSDPA User', 'HSDPA Users', 'HSDPA Active Users'],
            'HSDPA Throughput': ['HSDPA Throughput (kbps)', 'HSDPA Throughput', 'HSDPA Average Throughput'],
            'Cell Availability': ['Cell Availability (%)', 'Cell Availability', 'Availability']
        }

    # ---------- Existing methods (keeping original functionality) ----------
    def aggregate_daily_data(self, df, date_col):
        try:
            target_rncs = ['HLRE01', 'HLRE02', 'HLRE03', 'HLRE04']
            df_filtered = df[df['RNC Id'].isin(target_rncs)].copy()

            for col in df_filtered.columns:
                if col not in [date_col, 'RNC Id']:
                    df_filtered[col] = pd.to_numeric(df_filtered[col], errors='coerce')

            numeric_cols = df_filtered.select_dtypes(include=[np.number]).columns.tolist()
            if date_col in numeric_cols:
                numeric_cols.remove(date_col)

            agg_dict = {col: 'mean' for col in numeric_cols}

            non_numeric_cols = df_filtered.select_dtypes(exclude=[np.number]).columns.tolist()
            for col in non_numeric_cols:
                if col not in [date_col, 'RNC Id']:
                    agg_dict[col] = 'first'

            df_aggregated = df_filtered.groupby(date_col).agg(agg_dict).reset_index()

            for col in numeric_cols:
                if col in df_aggregated.columns:
                    df_aggregated[col] = df_aggregated[col].round(2)

            print(f"✅ Đã tổng hợp dữ liệu của Ericsson: {len(df_aggregated)} ngày từ {len(df_filtered)} bản ghi")
            return df_aggregated

        except Exception as e:
            print(f"❌ Lỗi khi tổng hợp dữ liệu: {e}")
            return df

    def aggregate_daily_data_ZTE(self, df, date_col):
        try:
            target_rncs = ['HNRZ01(101)', 'HNRZ01(102)']
            df_filtered = df[df['RNC Managed NE Name'].isin(target_rncs)].copy()

            for col in df_filtered.columns:
                if col not in [date_col, 'RNC Managed NE Name']:
                    df_filtered[col] = pd.to_numeric(df_filtered[col], errors='coerce')

            numeric_cols = df_filtered.select_dtypes(include=[np.number]).columns.tolist()
            if date_col in numeric_cols:
                numeric_cols.remove(date_col)

            agg_dict = {col: 'mean' for col in numeric_cols}

            non_numeric_cols = df_filtered.select_dtypes(exclude=[np.number]).columns.tolist()
            for col in non_numeric_cols:
                if col not in [date_col, 'RNC Managed NE Name']:
                    agg_dict[col] = 'first'

            df_aggregated = df_filtered.groupby(date_col).agg(agg_dict).reset_index()

            for col in numeric_cols:
                if col in df_aggregated.columns:
                    df_aggregated[col] = df_aggregated[col].round(2)

            print(f"✅ Đã tổng hợp dữ liệu của ZTE: {len(df_aggregated)} ngày từ {len(df_filtered)} bản ghi")
            return df_aggregated

        except Exception as e:
            print(f"❌ Lỗi khi tổng hợp dữ liệu: {e}")
            return df

    def create_daily_dashboard_table_ericsson(self, csv_all_day, csv_busy_hour, output_dir):
        try:
            print("\n📊 Đang tạo bảng Daily Dashboard của ericsson...")

            df_all = pd.read_csv(csv_all_day)
            df_bh = pd.read_csv(csv_busy_hour)

            date_col = df_all.columns[0]
            df_all[date_col] = pd.to_datetime(df_all[date_col])
            df_bh[date_col] = pd.to_datetime(df_bh[date_col])

            print(f"📅 Dữ liệu gốc - 24h: {len(df_all)} bản ghi, BH: {len(df_bh)} bản ghi")

            df_all_agg = self.aggregate_daily_data(df_all, date_col)
            df_bh_agg = self.aggregate_daily_data(df_bh, date_col)

            print(f"📈 Dữ liệu sau tổng hợp - 24h: {len(df_all_agg)} ngày, BH: {len(df_bh_agg)} ngày")

            # Improved KPI mapping with better column matching
            kpi_mapping = {
                'CS CSSR': ['CS CSSR (%)', 'CS CSSR', 'CS Call Setup Success Rate', 'CS Call Setup Success Rate (%)'],
                'HSDPA CSSR': ['HSDPA CSSR (%)', 'HSDPA CSSR', 'HSDPA Call Setup Success Rate',
                               'HSDPA Call Setup Success Rate (%)', 'PS CSSR_HSDPA'],
                'CS CDR': ['CS CDR (%)', 'CS CDR', 'CS Call Drop Rate', 'CS Call Drop Rate (%)'],
                'HSDPA CDR': ['HSDPA CDR (%)', 'HSDPA CDR', 'HSDPA Call Drop Rate', 'HSDPA Call Drop Rate (%)',
                              'PS CDR_HSPDA'],
                'CS Traffic (Erl)': ['CS Traffic (Erl)', 'CS Traffic (Erlang)', 'CS Traffic'],
                'CS Soft HOSR': ['CS Soft HOSR (%)', 'CS Soft HOSR', 'CS Soft Handover Success Rate',
                                 'CS Soft Handover Success Rate (%)'],
                'HSDPA Soft HOSR': ['HSDPA Soft HOSR (%)', 'HSDPA Soft HOSR', 'HSDPA Soft Handover Success Rate',
                                    'HSDPA Soft Handover Success Rate (%)', 'PS Soft HOSR'],
                'CS IRAT HOSR': ['CS IRAT HOSR (%)', 'CS IRAT HOSR', 'CS Inter-RAT Handover Success Rate',
                                 'CS Inter-RAT Handover Success Rate (%)'],
                'PS IRAT HOSR': ['PS IRAT HOSR (%)', 'PS IRAT HOSR', 'PS Inter-RAT Handover Success Rate',
                                 'PS Inter-RAT Handover Success Rate (%)'],
                'PS Traffic (GB)': ['PS Traffic (GB)', 'PS Traffic (Gigabytes)', 'PS Traffic'],
            }

            # Create figure with better layout
            fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(14, 12))
            fig.suptitle('Daily 3G KPI Dashboard of Ericsson', fontsize=16, fontweight='bold', y=0.95)

            # Get dates with better logic
            latest = df_all_agg[date_col].max()
            prev = df_all_agg[df_all_agg[date_col] < latest][date_col].max() if pd.notna(latest) else pd.NaT
            week_candidate = latest - timedelta(days=7) if pd.notna(latest) else pd.NaT
            week_date = df_all_agg[df_all_agg[date_col] <= week_candidate][date_col].max() if pd.notna(
                week_candidate) else pd.NaT

            latest_dates = []
            if pd.notna(latest):
                latest_dates.append(latest)
            if pd.notna(prev):
                latest_dates.append(prev)
            if pd.notna(week_date) and (week_date not in latest_dates):
                latest_dates.append(week_date)

            # Create improved dashboards
            self._create_improved_dashboard_subplot(ax1, df_all_agg, latest_dates, date_col, kpi_mapping,
                                                    "Daily 3G KPI Dashboard of Ericsson (24h)", "#FFA500")

            self._create_improved_dashboard_subplot(ax2, df_bh_agg, latest_dates, date_col, kpi_mapping,
                                                    "Daily 3G KPI Dashboard of Ericsson (BH)", "#FF6B35")

            plt.tight_layout()
            plt.subplots_adjust(top=0.93)

            dashboard_path = os.path.join(output_dir, "Daily_3G_KPI_Dashboard_of_Ericsson.png")
            plt.savefig(dashboard_path, dpi=300, bbox_inches='tight', facecolor='white')
            plt.close()

            print(f"✅ Đã tạo bảng Daily Dashboard của Ericsson: {dashboard_path}")
            return dashboard_path

        except Exception as e:
            print(f"❌ Lỗi khi tạo Daily Dashboard: {e}")
            return None

    def create_daily_dashboard_table_ZTE(self, csv_all_day, csv_busy_hour, output_dir):
        try:
            print("\n📊 Đang tạo bảng Daily Dashboard của ZTE...")

            df_all = pd.read_csv(csv_all_day)
            df_bh = pd.read_csv(csv_busy_hour)

            date_col = df_all.columns[1]
            df_all[date_col] = pd.to_datetime(df_all[date_col])
            df_bh[date_col] = pd.to_datetime(df_bh[date_col])

            print(f"📅 Dữ liệu gốc - 24h: {len(df_all)} bản ghi, BH: {len(df_bh)} bản ghi")

            df_all_agg = self.aggregate_daily_data_ZTE(df_all, date_col)
            df_bh_agg = self.aggregate_daily_data_ZTE(df_bh, date_col)

            print(f"📈 Dữ liệu sau tổng hợp - 24h: {len(df_all_agg)} ngày, BH: {len(df_bh_agg)} ngày")

            # Improved KPI mapping with better column matching
            kpi_mapping = {
                'CS CSSR': ['CS CSSR_VNM', 'CS CSSR', 'CS Call Setup Success Rate', 'CS Call Setup Success Rate (%)'],
                'HSDPA CSSR': ['HSDPA CSSR (%)', 'HSDPA CSSR', 'HSDPA Call Setup Success Rate',
                               'HSDPA Call Setup Success Rate (%)', 'PS CSSR'],
                'CS CDR': ['CS CDR_VNM', 'CS CDR', 'CS Call Drop Rate', 'CS Call Drop Rate (%)'],
                'HSDPA CDR': ['PS CDR_HSDPA_VNM', 'HSDPA CDR', 'HSDPA Call Drop Rate', 'HSDPA Call Drop Rate (%)',
                              'PS CDR_HSPDA'],
                'CS Traffic (Erl)': ['CS Traffic (Erl)', 'CS Traffic (Erlang)', 'CS Traffic (Erl)_VNM'],
                'CS Soft HOSR': ['CS Soft HOSR_VNM', 'CS Soft HOSR', 'CS Soft Handover Success Rate',
                                 'CS Soft Handover Success Rate (%)'],
                'HSDPA Soft HOSR': ['PS Soft HOSR_VNM', 'HSDPA Soft HOSR', 'HSDPA Soft Handover Success Rate',
                                    'HSDPA Soft Handover Success Rate (%)', 'PS Soft HOSR'],
                'CS IRAT HOSR': ['CS InterRAT HOSR_VNM', 'CS IRAT HOSR', 'CS Inter-RAT Handover Success Rate',
                                 'CS Inter-RAT Handover Success Rate (%)'],
                'PS IRAT HOSR': ['PS InterRAT HOSR_VNM', 'PS IRAT HOSR', 'PS Inter-RAT Handover Success Rate',
                                 'PS Inter-RAT Handover Success Rate (%)'],
                'PS Traffic (GB)': ['PS Traffic (GB)', 'PS Traffic (Gigabytes)', 'PS Traffic'],
            }
            fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(14, 12))
            fig.suptitle('Daily 3G KPI Dashboard of ZTE', fontsize=16, fontweight='bold', y=0.95)

            # Get dates with better logic
            latest = df_all_agg[date_col].max()
            prev = df_all_agg[df_all_agg[date_col] < latest][date_col].max() if pd.notna(latest) else pd.NaT
            week_candidate = latest - timedelta(days=7) if pd.notna(latest) else pd.NaT
            week_date = df_all_agg[df_all_agg[date_col] <= week_candidate][date_col].max() if pd.notna(
                week_candidate) else pd.NaT

            latest_dates = []
            if pd.notna(latest):
                latest_dates.append(latest)
            if pd.notna(prev):
                latest_dates.append(prev)
            if pd.notna(week_date) and (week_date not in latest_dates):
                latest_dates.append(week_date)

            # Create improved dashboards
            self._create_improved_dashboard_subplot(ax1, df_all_agg, latest_dates, date_col, kpi_mapping,
                                                    "Daily 3G KPI Dashboard of ZTE (24h)", "#FFA500")

            self._create_improved_dashboard_subplot(ax2, df_bh_agg, latest_dates, date_col, kpi_mapping,
                                                    "Daily 3G KPI Dashboard of ZTE (BH)", "#FF6B35")

            plt.tight_layout()
            plt.subplots_adjust(top=0.93)

            dashboard_path = os.path.join(output_dir, "Daily_3G_KPI_Dashboard_of_ZTE.png")
            plt.savefig(dashboard_path, dpi=300, bbox_inches='tight', facecolor='white')
            plt.close()

            print(f"✅ Đã tạo bảng Daily Dashboard của ZTE: {dashboard_path}")
            return dashboard_path

        except Exception as e:
            print(f"❌ Lỗi khi tạo Daily Dashboard: {e}")
            return None

    def _create_improved_dashboard_subplot(self, ax, df, latest_dates, date_col, kpi_mapping, title, header_color):
        """
        Tạo dashboard với layout cải thiện
        """
        ax.clear()
        ax.set_xlim(0, 10)
        ax.set_ylim(0, 10)
        ax.axis('off')

        # Title
        ax.text(5, 9.5, title, ha='center', va='center', fontsize=12, fontweight='bold')

        # ========== BẢNG 1: Success Rates & Traffic ==========
        self._draw_improved_table(ax, df, latest_dates, date_col, kpi_mapping,
                                  ['CS CSSR', 'HSDPA CSSR', 'CS CDR', 'HSDPA CDR', 'CS Traffic (Erl)'],
                                  [99.00, 98.00, 0.80, 1.50, None],
                                  header_color, y_start=8.5)

        # ========== BẢNG 2: Handover Success Rates & PS Traffic ==========
        self._draw_improved_table(ax, df, latest_dates, date_col, kpi_mapping,
                                  ['CS Soft HOSR', 'HSDPA Soft HOSR', 'CS IRAT HOSR', 'PS IRAT HOSR',
                                   'PS Traffic (GB)'],
                                  [99.00, 98.00, 97.00, 92.00, None],
                                  header_color, y_start=4.5)

    def _draw_improved_table(self, ax, df, latest_dates, date_col, kpi_mapping, kpi_list, targets, header_color,
                             y_start):
        """
        Vẽ bảng với format cải thiện
        """
        # Table parameters
        col_width = 1.6
        row_height = 0.4
        x_start = 0.4

        # Prepare headers
        headers = ['Item'] + kpi_list

        # Prepare data
        table_data = []

        # Target row
        target_row = ['Target (%)']
        for i, target in enumerate(targets):
            if target is None:
                target_row.append('-')
            else:
                target_row.append(f"{target:.2f}")
        table_data.append(target_row)

        # Data rows
        date_rows = []
        for date in latest_dates:
            date_str = date.strftime('%d-%b-%y')
            row_data = [date_str]

            for kpi_name in kpi_list:
                value = self._get_kpi_value(df, date, date_col, kpi_name, kpi_mapping)
                if value is not None:
                    if kpi_name in ['CS Traffic (Erl)', 'PS Traffic (GB)']:
                        row_data.append(f"{value:,.0f}")
                    else:
                        row_data.append(f"{value:.2f}")
                else:
                    row_data.append('-')

            table_data.append(row_data)
            date_rows.append(row_data)

        # Compare rows
        if len(date_rows) >= 2:
            # Compare with D-1
            comp_d1 = ['Compare with (D-1)']
            for j in range(1, len(headers)):
                kpi_name = headers[j]
                try:
                    curr_str = date_rows[0][j]
                    prev_str = date_rows[1][j]

                    if curr_str != '-' and prev_str != '-':
                        curr_val = float(curr_str.replace(',', ''))
                        prev_val = float(prev_str.replace(',', ''))

                        if kpi_name in ['CS CDR', 'HSDPA CDR']:
                            # For drop rates, improvement means decrease
                            diff = prev_val - curr_val
                        elif kpi_name in ['CS Traffic (Erl)', 'PS Traffic (GB)']:
                            # For traffic, show percentage change
                            diff = (curr_val - prev_val) / prev_val * 100
                            comp_d1.append(f"{diff:+.0f}%")
                            continue
                        else:
                            # For success rates, improvement means increase
                            diff = curr_val - prev_val

                        comp_d1.append(f"{diff:+.2f}%")
                    else:
                        comp_d1.append('0%')
                except:
                    comp_d1.append('-')
            table_data.append(comp_d1)

            # Compare with D-7
            if len(date_rows) >= 3:
                comp_d7 = ['Compare with (D-7)']
                for j in range(1, len(headers)):
                    kpi_name = headers[j]
                    try:
                        curr_str = date_rows[0][j]
                        week_str = date_rows[2][j]

                        if curr_str != '-' and week_str != '-':
                            curr_val = float(curr_str.replace(',', ''))
                            week_val = float(week_str.replace(',', ''))

                            if kpi_name in ['CS CDR', 'HSDPA CDR']:
                                diff = week_val - curr_val
                            elif kpi_name in ['CS Traffic (Erl)', 'PS Traffic (GB)']:
                                diff = (curr_val - week_val) / week_val * 100
                                comp_d7.append(f"{diff:+.0f}%")
                                continue
                            else:
                                diff = curr_val - week_val

                            comp_d7.append(f"{diff:+.2f}%")
                        else:
                            comp_d7.append('0%')
                    except:
                        comp_d7.append('-')
                table_data.append(comp_d7)

        # Draw the table
        self._draw_formatted_table(ax, headers, table_data, targets, header_color,
                                   x_start, y_start, col_width, row_height)

    def _get_kpi_value(self, df, date, date_col, kpi_name, kpi_mapping):
        """
        Lấy KPI theo trung bình cộng trong ngày (không lấy bản ghi đầu tiên).
        """
        possible_cols = kpi_mapping.get(kpi_name, [kpi_name])

        # Lọc dữ liệu cho ngày cần lấy
        day_data = df[df[date_col].dt.date == date.date()]
        if day_data.empty:
            return None

        for col_name in possible_cols:
            if col_name in df.columns:
                val = day_data[col_name].mean()
                if pd.notna(val):
                    return float(val)

            for actual_col in df.columns:
                if col_name.lower().replace(' ', '').replace('(', '').replace(')', '') in \
                        actual_col.lower().replace(' ', '').replace('(', '').replace(')', ''):
                    val = day_data[actual_col].mean()
                    if pd.notna(val):
                        return float(val)

        return None

    def _draw_formatted_table(self, ax, headers, data, targets, header_color,
                              x_start, y_start, col_width, row_height):
        """
        Draw table with proper formatting like the reference image
        """
        # Draw headers
        for i, header in enumerate(headers):
            x = x_start + i * col_width
            rect = plt.Rectangle((x, y_start), col_width, row_height,
                                 facecolor=header_color, edgecolor='black', linewidth=1)
            ax.add_patch(rect)

            # Adjust font size for headers
            font_size = 8 if len(header) > 12 else 9
            ax.text(x + col_width / 2, y_start + row_height / 2, header,
                    ha='center', va='center', fontsize=font_size,
                    fontweight='bold', color='white')

        # Draw data rows
        for row_idx, row_data in enumerate(data):
            y = y_start - (row_idx + 1) * row_height

            for col_idx, value in enumerate(row_data):
                x = x_start + col_idx * col_width

                # Determine background color and text formatting
                bg_color, text_color, font_weight, display_value = self._format_cell(
                    row_idx, col_idx, value, row_data, headers, targets)

                # Draw cell
                rect = plt.Rectangle((x, y), col_width, row_height,
                                     facecolor=bg_color, edgecolor='black', linewidth=1)
                ax.add_patch(rect)

                # Draw text
                font_size = 7 if len(str(display_value)) > 10 else 8
                ax.text(x + col_width / 2, y + row_height / 2, display_value,
                        ha='center', va='center', fontsize=font_size,
                        color=text_color, weight=font_weight)

    def _format_cell(self, row_idx, col_idx, value, row_data, headers, targets):
        """
        Format individual cell with proper colors and symbols
        """
        bg_color = 'white'
        text_color = 'black'
        font_weight = 'normal'
        display_value = str(value)

        # Target row formatting
        if row_idx == 0:
            bg_color = '#FFFACD'  # Light yellow
            font_weight = 'bold'
            return bg_color, text_color, font_weight, display_value

        # Compare rows formatting
        if 'Compare' in str(row_data[0]):
            bg_color = '#E6E6FA'  # Lavender

            if col_idx > 0 and value != '-':
                value_str = str(value)
                try:
                    # Extract numeric value
                    clean_val = value_str.replace('%', '').replace('+', '').strip()
                    if clean_val and clean_val != '-':
                        val_float = float(clean_val)

                        # Add arrows and colors based on value
                        if value_str.startswith('+'):
                            if abs(val_float) <= 1:
                                display_value = f"{value} →"
                                text_color = 'black'
                            else:
                                display_value = f"{value} ↑"
                                text_color = 'green'
                            font_weight = 'bold'
                        elif value_str.startswith('-'):
                            if abs(val_float) <= 1:
                                display_value = f"{value} →"
                                text_color = 'black'
                            else:
                                display_value = f"{value} ↓"
                                text_color = 'red'
                            font_weight = 'bold'
                except:
                    pass

            return bg_color, text_color, font_weight, display_value

        # Data row formatting with target comparison
        if col_idx > 0 and col_idx - 1 < len(targets):
            target = targets[col_idx - 1]
            if target is not None:
                try:
                    actual_val = float(str(value).replace(',', ''))
                    header_name = headers[col_idx]

                    # Check if value meets target
                    should_highlight = False
                    if header_name in ['CS CDR', 'HSDPA CDR'] and actual_val > target:
                        should_highlight = True
                    elif header_name not in ['CS CDR', 'HSDPA CDR'] and actual_val < target:
                        should_highlight = True

                    if should_highlight:
                        bg_color = '#FFB3B3'  # Light red
                        text_color = '#B22222'  # Dark red
                        font_weight = 'bold'
                except:
                    pass

        return bg_color, text_color, font_weight, display_value

    # ---------- Utilities ----------
    def verify_csv_structure(self, csv_path):
        try:
            df = pd.read_csv(csv_path)
            print(f"🔍 Kiểm tra: {csv_path}")
            print(f"   📏 shape: {df.shape}")
            print(f"   📋 10 cột đầu: {list(df.columns[:10])}")
            print(df.head(3))
            suspicious_cols = [c for c in df.columns if str(c).lower().startswith("unnamed")]
            return len(suspicious_cols) == 0
        except Exception as e:
            print(f"❌ Lỗi khi kiểm tra {csv_path}: {e}")
            return False


def main():
    processor = ExcelCSVProcessorFor3G()

    converted_files_zte = {}
    converted_files_ericsson = {}

    excel_files_zte = {
        '3G_RNO_KPIs_BH_ZTE_2025-08-06.xlsx': '3G_RNO_KPIs_BH_ZTE_2025-08-06.csv',
        '3G_RNO_KPIs_WD_ZTE_2025-08-06.xlsx': '3G_RNO_KPIs_WD_ZTE_2025-08-06.csv'
    }
    excel_files_ericsson = {
        '3G_RNO_KPIs_BH_scheduled2025-08-06.xlsx': '3G_RNO_KPIs_BH_scheduled2025-08-06.csv',
        '3G_RNO_KPIs_WD_scheduled2025-08-06.xlsx': '3G_RNO_KPIs_WD_scheduled2025-08-06.csv'
    }

    # Convert Excel files to CSV
    for excel_file_zte, csv_file_zte in excel_files_zte.items():
        if os.path.exists(excel_file_zte):
            df = processor.clean_excel_to_csv_ZTE(excel_file_zte, csv_file_zte)
            if df is not None:
                converted_files_zte[excel_file_zte] = csv_file_zte
                processor.verify_csv_structure(csv_file_zte)
        else:
            print(f"⚠️ File không tồn tại: {excel_file_zte}")

    for excel_file_ericsson, csv_file_ericsson in excel_files_ericsson.items():
        if os.path.exists(excel_file_ericsson):
            df = processor.clean_excel_to_csv_ericsson(excel_file_ericsson, csv_file_ericsson)
            if df is not None:
                converted_files_ericsson[excel_file_ericsson] = csv_file_ericsson
                processor.verify_csv_structure(csv_file_ericsson)
        else:
            print(f"⚠️ File không tồn tại: {excel_file_ericsson}")

    # Create original dashboards (aggregated data)
    if len(converted_files_ericsson) >= 2:
        csv_files_ericsson = list(converted_files_ericsson.values())
        csv_all_day_ericsson = csv_files_ericsson[0]
        csv_busy_hour_ericsson = csv_files_ericsson[1]
        output_dir_ericsson = "output_ericsson"
        os.makedirs(output_dir_ericsson, exist_ok=True)
        processor.create_daily_dashboard_table_ericsson(csv_all_day_ericsson, csv_busy_hour_ericsson,
                                                        output_dir_ericsson)

    if len(converted_files_zte) >= 2:
        csv_files_zte = list(converted_files_zte.values())
        csv_all_day_zte = csv_files_zte[0]
        csv_busy_hour_zte = csv_files_zte[1]
        output_dir_zte = "output_zte"
        os.makedirs(output_dir_zte, exist_ok=True)
        processor.create_daily_dashboard_table_ZTE(csv_all_day_zte, csv_busy_hour_zte, output_dir_zte)

    # NEW: Create RNC dashboards and charts
    if len(converted_files_ericsson) >= 2 and len(converted_files_zte) >= 2:
        print("\n🎯 Bắt đầu tạo Daily 3G KPI Dashboard By RNC...")

        # Create output directory for RNC dashboards
        rnc_output_dir = "output_rnc_dashboards"
        os.makedirs(rnc_output_dir, exist_ok=True)

        # Get file paths
        csv_files_ericsson = list(converted_files_ericsson.values())
        csv_files_zte = list(converted_files_zte.values())

        csv_all_day_ericsson = [f for f in csv_files_ericsson if 'WD' in f][0] if any(
            'WD' in f for f in csv_files_ericsson) else csv_files_ericsson[0]
        csv_bh_ericsson = [f for f in csv_files_ericsson if 'BH' in f][0] if any(
            'BH' in f for f in csv_files_ericsson) else csv_files_ericsson[1]

        csv_all_day_zte = [f for f in csv_files_zte if 'WD' in f][0] if any('WD' in f for f in csv_files_zte) else \
        csv_files_zte[0]
        csv_bh_zte = [f for f in csv_files_zte if 'BH' in f][0] if any('BH' in f for f in csv_files_zte) else \
        csv_files_zte[1]

        # Create RNC dashboards and trend charts
        processor.create_daily_rnc_dashboard(
            csv_all_day_ericsson=csv_all_day_ericsson,
            csv_bh_ericsson=csv_bh_ericsson,
            csv_all_day_zte=csv_all_day_zte,
            csv_bh_zte=csv_bh_zte,
            output_dir=rnc_output_dir
        )

        print("🎉 Hoàn thành tất cả dashboards và biểu đồ!")
    else:
        print("⚠️ Không đủ file để tạo RNC Dashboard. Cần có ít nhất 2 file Ericsson và 2 file ZTE.")


if __name__ == "__main__":
    main()