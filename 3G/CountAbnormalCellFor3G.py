import os
from datetime import timedelta, datetime
import numpy as np
import pandas as pd
import csv
from matplotlib import pyplot as plt
import matplotlib.dates as mdates
from matplotlib.patches import Rectangle
import seaborn as sns
from matplotlib.table import Table
from reportlab.lib.pagesizes import A3
from reportlab.pdfgen import canvas
from reportlab.lib.units import inch
from PIL import Image
from datetime import datetime

# Set style for better looking plots
plt.style.use('default')
sns.set_palette("husl")


class CountAbnormalCellFor3G:
    def __init__(self):
        self.clean_data = {}

        # RTWP mapping for different vendors
        self.ericsson_rncs = ['HLRE01', 'HLRE02', 'HLRE03', 'HLRE04']
        self.zte_rncs = 'HNRZ01(101)'

        # Province mapping based on site ID prefix (first 4 characters)
        self.province_mapping = {
            'U105': 'Bac Giang', 'U104': 'Bac Kan', 'U106': 'Bac Ninh',
            'U113': 'Cao Bang', 'U118': 'Dien Bien', 'U122': 'Ha Giang',
            'U123': 'Ha Nam', 'U124': 'Ha Noi', 'U125': 'Ha Noi',
            'U424': 'Ha Noi', 'U226': 'Ha Tinh', 'U127': 'Hai Duong',
            'U128': 'Hai Phong', 'U130': 'Hoa Binh', 'U131': 'Hung Yen',
            'U135': 'Lai Chau', 'U137': 'Lang Son', 'U138': 'Lao Cai',
            'U140': 'Nam Dinh', 'U241': 'Nghe An', 'U142': 'Ninh Binh',
            'U144': 'Phu Tho', 'U544': 'Phu Tho', 'U149': 'Quang Ninh',
            'U152': 'Son La', 'U154': 'Thai Binh', 'U155': 'Thai Nguyen',
            'U256': 'Thanh Hoa', 'U161': 'Tuyen Quang', 'U163': 'Vinh Phuc',
            'U164': 'Yen Bai', 'U209': 'Binh Dinh', 'U215': 'Da Nang',
            'U415': 'Da Nang', 'U216': 'Dak Lak', 'U217': 'Dak Nong',
            'U221': 'Gia Lai', 'U232': 'Khanh Hoa', 'U432': 'Khanh Hoa',
            'U234': 'Kon Tum', 'U245': 'Phu Yen', 'U246': 'Quang Binh',
            'U247': 'Quang Nam', 'U248': 'Quang Ngai', 'U250': 'Quang Tri',
            'U257': 'Thua Thien Hue', 'U301': 'An Giang', 'U302': 'Ba Ria Vung Tau',
            'U303': 'Bac Lieu', 'U307': 'Ben Tre', 'U308': 'Binh Duong',
            'U359': 'TP.HCM', 'U408': 'Binh Duong', 'U310': 'Binh Phuoc',
            'U311': 'Binh Thuan', 'U312': 'Ca Mau', 'U314': 'Can Tho',
            'U333': 'Kien Giang', 'U319': 'Dong Nai', 'U320': 'Dong Thap',
            'U329': 'Hau Giang', 'U357': 'TP.HCM', 'U459': 'TP.HCM',
            'U236': 'Lam Dong', 'U436': 'Lam Dong', 'U339': 'Long An',
            'U343': 'Ninh Thuan', 'U351': 'Soc Trang', 'U353': 'Tay Ninh',
            'U358': 'Tien Giang', 'U360': 'Tra Vinh', 'U362': 'Vinh Long'
        }

    # ========== EXCEL TO CSV CONVERSION ==========
    def _find_header_row_generic(self, df, keywords, max_rows=30):
        """Generic header row detection"""
        for i in range(min(max_rows, len(df))):
            row = df.iloc[i].astype(str).str.strip().str.lower().tolist()
            if any(kw.lower() in row for kw in keywords):
                return i
        return 0

    def _find_header_row_ericsson(self, df, max_rows=30):
        """Find header row for Ericsson Excel files"""
        return self._find_header_row_generic(df, ["date"], max_rows)

    def _find_header_row_zte(self, df, max_rows=80):
        """Find header row for ZTE Excel files"""
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

    @staticmethod
    def _is_type_row(series):
        """Check if row contains type information"""
        tokens = {"int", "float", "double", "number", "str", "string", "date", "datetime", "timestamp"}
        vals = [str(x).strip().lower() for x in series.tolist()]
        nonempty = [v for v in vals if v not in ("", "nan")]
        if not nonempty:
            return False
        match_ratio = sum(v in tokens for v in nonempty) / len(nonempty)
        return match_ratio >= 0.6

    @staticmethod
    def _is_unit_row(series):
        """Check if row contains unit information"""
        unit_tokens = {"%", "ms", "s", "gb", "mb", "kbps", "erlang", "dbm", "num", "den"}
        vals = [str(x).strip().lower() for x in series.tolist()]
        nonempty = [v for v in vals if v not in ("", "nan")]
        if not nonempty:
            return False
        shortish = sum(len(v) <= 6 for v in nonempty) / len(nonempty) >= 0.7
        contains_units = sum(any(t in v for t in unit_tokens) for v in nonempty) / len(nonempty) >= 0.4
        return shortish and contains_units

    def clean_excel_to_csv_ericsson(self, excel_path, csv_path, sheet_name=0):
        """Convert Ericsson Excel file to CSV"""
        try:
            print(f"Converting Ericsson file: {excel_path}")
            preview = pd.read_excel(excel_path, sheet_name=sheet_name, nrows=30, header=None)
            header_row = self._find_header_row_ericsson(preview, max_rows=30)

            df = pd.read_excel(excel_path, sheet_name=sheet_name, header=header_row)
            df = df.loc[:, ~df.columns.astype(str).str.contains(r"^Unnamed", na=False)]
            df = df.dropna(how="all")

            date_col = df.columns[1]
            df[date_col] = pd.to_datetime(df[date_col], errors="coerce", dayfirst=True)
            df = df.dropna(subset=[date_col]).sort_values(by=date_col).reset_index(drop=True)

            df.to_csv(csv_path, index=False, encoding="utf-8", date_format="%Y-%m-%d %H:%M:%S",
                      lineterminator="\n", quoting=csv.QUOTE_MINIMAL)
            print(f"Saved Ericsson CSV: {csv_path}, shape: {df.shape}")
            self.clean_data[csv_path] = df
            return df
        except Exception as e:
            print(f"Error processing Ericsson file: {e}")
            return None

    def clean_excel_to_csv_ZTE(self, excel_path, csv_path, sheet_name=0):
        """Convert ZTE Excel file to CSV"""
        try:
            print(f"Converting ZTE file: {excel_path}")
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

            df.to_csv(csv_path, index=False, encoding="utf-8", date_format="%Y-%m-%d %H:%M:%S",
                      lineterminator="\n", quoting=csv.QUOTE_MINIMAL)
            print(f"Saved ZTE CSV: {csv_path}, shape: {df.shape}")
            self.clean_data[csv_path] = df
            return df
        except Exception as e:
            print(f"Error processing ZTE file: {e}")
            return None

    def verify_csv_structure(self, csv_path):
        """Verify CSV structure after conversion"""
        try:
            df = pd.read_csv(csv_path)
            print(f"Verifying: {csv_path}")
            print(f"   Shape: {df.shape}")
            print(f"   First 10 columns: {list(df.columns[:10])}")
            print(df.head(3))
            suspicious_cols = [c for c in df.columns if str(c).lower().startswith("unnamed")]
            return len(suspicious_cols) == 0
        except Exception as e:
            print(f"Error verifying {csv_path}: {e}")
            return False

    # ========== VENDOR-SPECIFIC CELL COUNTING ==========

    def get_province_from_ericsson_cell(self, ucell_id):
        """Extract province from Ericsson UCell ID based on first 4 characters"""
        if pd.isna(ucell_id):
            return None
        cell_str = str(ucell_id).strip()
        if len(cell_str) >= 4:
            prefix = cell_str[:4].upper()
            return self.province_mapping.get(prefix, None)
        return None

    def get_province_from_zte_cell(self, cell_name):
        """Extract province from ZTE Cell Name based on first 4 characters"""
        if pd.isna(cell_name):
            return None
        cell_str = str(cell_name).strip()
        if len(cell_str) >= 4:
            prefix = cell_str[:4].upper()
            return self.province_mapping.get(prefix, None)
        return None

    def count_abnormal_cells_ericsson(self, csv_path, rtwp_threshold=-95):
        """Count Ericsson cells with RTWP > threshold by province and hour"""
        try:
            # Read CSV file
            df = pd.read_csv(csv_path)
            print(f"\nProcessing ERICSSON data from {csv_path}")
            print(f"Total rows: {len(df)}")

            # Check required columns exist
            required_cols = ['Date', 'Hour', 'UCell Id', 'RTWP 3G']
            missing_cols = [col for col in required_cols if col not in df.columns]

            if missing_cols:
                print(f"ERROR: Missing columns: {missing_cols}")
                print(f"Available columns: {df.columns.tolist()}")
                return None

            # NO DATETIME CONVERSION - Use Date and Hour as they are
            # Just ensure they are not null
            df = df.dropna(subset=['Date', 'Hour', 'UCell Id', 'RTWP 3G'])

            print(f"Valid rows after removing nulls: {len(df)}")

            # Process data
            results = []
            processed_cells = set()

            for idx, row in df.iterrows():
                ucell_id = str(row['UCell Id']).strip()

                # Check if valid UCell ID starting with U
                if ucell_id.upper().startswith('U'):
                    province = self.get_province_from_ericsson_cell(ucell_id)

                    if province:
                        try:
                            rtwp_value = float(row['RTWP 3G'])
                            if rtwp_value > rtwp_threshold:
                                # Use Date and Hour as-is from the CSV
                                date_val = row['Date']
                                hour_val = int(row['Hour']) if pd.notna(row['Hour']) else 0

                                # Create unique key
                                cell_key = f"{date_val}_{hour_val}_{ucell_id}"

                                if cell_key not in processed_cells:
                                    processed_cells.add(cell_key)
                                    results.append({
                                        'Date': date_val,
                                        'Hour': hour_val,
                                        'Cell': ucell_id,
                                        'Province': province,
                                        'RTWP': rtwp_value
                                    })
                        except (ValueError, TypeError):
                            continue

            if results:
                result_df = pd.DataFrame(results)
                print(f"Found {len(result_df)} records with RTWP > {rtwp_threshold} dBm")
                print(f"Unique cells affected: {result_df['Cell'].nunique()}")

                # Show province distribution
                province_counts = result_df.groupby('Province')['Cell'].nunique().sort_values(ascending=False)
                print(f"\nProvinces distribution:")
                for prov, count in province_counts.items():
                    print(f"  {prov}: {count} cells")

                return result_df
            else:
                print("No abnormal cells found in Ericsson data")
                return pd.DataFrame()

        except Exception as e:
            print(f"Error processing Ericsson data: {e}")
            import traceback
            traceback.print_exc()
            return None

    def count_abnormal_cells_zte(self, csv_path, rtwp_threshold=-95):
        """Count ZTE cells with RTWP > threshold by province and hour"""
        try:
            # Read CSV file
            df = pd.read_csv(csv_path)
            print(f"\nProcessing ZTE data from {csv_path}")
            print(f"Total rows: {len(df)}")

            # Find columns
            date_col = None
            cell_name_col = None

            for col in df.columns:
                col_lower = col.lower()
                if 'start time' in col_lower:
                    date_col = col
                elif 'cell name' in col_lower:
                    cell_name_col = col

            if not date_col:
                print("ERROR: No 'Start Time' column found")
                return None
            if not cell_name_col:
                print("ERROR: No 'Cell Name' column found")
                return None

            print(f"Using columns: Start Time={date_col}, Cell Name={cell_name_col}")

            # Find RTWP columns
            rtwp_cols = []
            for col in df.columns:
                col_lower = col.lower()
                if any(keyword in col_lower for keyword in ['rtwp', 'avg', 'mean', 'average']):
                    if col not in [date_col, cell_name_col]:
                        rtwp_cols.append(col)

            print(f"RTWP columns found: {rtwp_cols}")

            if not rtwp_cols:
                print("ERROR: No RTWP columns found")
                return None

            # Convert Start Time to datetime to extract date and hour
            df['temp_datetime'] = pd.to_datetime(df[date_col], errors='coerce')
            df = df.dropna(subset=['temp_datetime'])

            # Extract date and hour
            df['Date'] = df['temp_datetime'].dt.date
            df['Hour'] = df['temp_datetime'].dt.hour

            # Drop temp column
            df = df.drop('temp_datetime', axis=1)

            print(f"Valid rows after date processing: {len(df)}")

            # Process data
            results = []
            processed_cells = set()

            for idx, row in df.iterrows():
                cell_name = str(row[cell_name_col]).strip()

                if cell_name and cell_name != 'nan':
                    province = self.get_province_from_zte_cell(cell_name)

                    if province:
                        for rtwp_col in rtwp_cols:
                            if pd.notna(row[rtwp_col]):
                                try:
                                    rtwp_value = float(row[rtwp_col])
                                    if rtwp_value > rtwp_threshold:
                                        cell_key = f"{row['Date']}_{row['Hour']}_{cell_name}_{rtwp_col}"

                                        if cell_key not in processed_cells:
                                            processed_cells.add(cell_key)
                                            results.append({
                                                'Date': row['Date'],
                                                'Hour': row['Hour'],
                                                'Cell': cell_name,
                                                'Province': province,
                                                'RTWP': rtwp_value
                                            })
                                except (ValueError, TypeError):
                                    continue

            if results:
                result_df = pd.DataFrame(results)
                print(f"Found {len(result_df)} records with RTWP > {rtwp_threshold} dBm")
                print(f"Unique cells affected: {result_df['Cell'].nunique()}")

                # Show province distribution
                province_counts = result_df.groupby('Province')['Cell'].nunique().sort_values(ascending=False)
                print(f"\nProvinces distribution:")
                for prov, count in province_counts.items():
                    print(f"  {prov}: {count} cells")

                return result_df
            else:
                print("No abnormal cells found in ZTE data")
                return pd.DataFrame()

        except Exception as e:
            print(f"Error processing ZTE data: {e}")
            import traceback
            traceback.print_exc()
            return None

    def create_summary_table(self, abnormal_cells_df):
        """Create summary table of cell counts by province and hour"""
        if abnormal_cells_df is None or abnormal_cells_df.empty:
            print("No data to create summary table")
            return None

        try:
            # Group by Date, Hour, and Province to count unique cells
            summary = abnormal_cells_df.groupby(['Date', 'Hour', 'Province'])['Cell'].nunique().reset_index()
            summary.columns = ['Date', 'Hour', 'Province', 'Cell_Count']

            # Pivot to create the desired format
            pivot_table = summary.pivot_table(
                index=['Date', 'Hour'],
                columns='Province',
                values='Cell_Count',
                fill_value=0,
                aggfunc='sum'
            )

            # Add Grand Total column
            pivot_table['Grand Total'] = pivot_table.sum(axis=1)

            # Reset index to make Date and Hour regular columns
            pivot_table = pivot_table.reset_index()

            # Format Date column
            pivot_table['Date'] = pd.to_datetime(pivot_table['Date']).dt.strftime('%Y-%m-%d')

            print("\nSummary Table Created:")
            print(f"Shape: {pivot_table.shape}")
            print(f"Date range: {pivot_table['Date'].min()} to {pivot_table['Date'].max()}")
            print(
                f"Provinces included: {[col for col in pivot_table.columns if col not in ['Date', 'Hour', 'Grand Total']]}")

            return pivot_table

        except Exception as e:
            print(f"Error creating summary table: {e}")
            return None

    def export_summary_to_excel(self, summary_table, abnormal_cells_df=None, output_path='rtwp_summary.xlsx'):
        """Export summary table and detailed data to Excel with formatting"""
        if summary_table is None or summary_table.empty:
            print("No data to export")
            return

        try:
            with pd.ExcelWriter(output_path, engine='openpyxl') as writer:
                # Write summary table
                summary_table.to_excel(writer, sheet_name='RTWP Summary', index=False)

                # Write detailed data if available
                if abnormal_cells_df is not None and not abnormal_cells_df.empty:
                    abnormal_cells_df.to_excel(writer, sheet_name='Detailed Data', index=False)

                    # Create province statistics
                    province_stats = abnormal_cells_df.groupby('Province').agg({
                        'Cell': 'nunique',
                        'RTWP': ['mean', 'max', 'min', 'count']
                    }).round(2)
                    province_stats.columns = ['Unique_Cells', 'Avg_RTWP', 'Max_RTWP', 'Min_RTWP', 'Total_Records']
                    province_stats = province_stats.sort_values('Unique_Cells', ascending=False)
                    province_stats.to_excel(writer, sheet_name='Province Statistics')

                    # Create hourly statistics if Hour column exists
                    if 'Hour' in abnormal_cells_df.columns:
                        hourly_stats = abnormal_cells_df.groupby('Hour').agg({
                            'Cell': 'nunique',
                            'RTWP': 'mean'
                        }).round(2)
                        hourly_stats.columns = ['Unique_Cells', 'Avg_RTWP']
                        hourly_stats.to_excel(writer, sheet_name='Hourly Statistics')

                print(f"Summary exported to: {output_path}")

        except Exception as e:
            print(f"Error exporting to Excel: {e}")

    # def export_summary_table_as_image(self, summary_table, output_path='rtwp_summary_table.png'):
    #     """Export summary table as PNG image"""
    #     if summary_table is None or summary_table.empty:
    #         print("No data to export")
    #         return
    #
    #     try:
    #         # Prepare data for display
    #         display_df = summary_table.copy()
    #
    #         # Limit rows for better visibility (show first 30 rows)
    #         if len(display_df) > 30:
    #             display_df = display_df.head(30)
    #             print(f"Note: Showing first 30 rows out of {len(summary_table)} total rows")
    #
    #         # Create figure
    #         fig, ax = plt.subplots(figsize=(20, 12))
    #         ax.axis('tight')
    #         ax.axis('off')
    #
    #         # Create table
    #         table_data = []
    #
    #         # Add headers
    #         headers = list(display_df.columns)
    #         table_data.append(headers)
    #
    #         # Add data rows
    #         for idx, row in display_df.iterrows():
    #             table_data.append([str(val) for val in row.values])
    #
    #         # Create table
    #         table = ax.table(cellText=table_data[1:],
    #                          colLabels=table_data[0],
    #                          cellLoc='center',
    #                          loc='center',
    #                          colWidths=[0.08] * len(headers))
    #
    #         # Style the table
    #         table.auto_set_font_size(False)
    #         table.set_fontsize(8)
    #         table.scale(1.2, 1.5)
    #
    #         # Color header
    #         for i in range(len(headers)):
    #             table[(0, i)].set_facecolor('#4CAF50')
    #             table[(0, i)].set_text_props(weight='bold', color='white')
    #
    #         # Color Grand Total column
    #         if 'Grand Total' in headers:
    #             gt_idx = headers.index('Grand Total')
    #             for i in range(1, len(table_data)):
    #                 table[(i, gt_idx)].set_facecolor('#FFE5B4')
    #                 table[(i, gt_idx)].set_text_props(weight='bold')
    #
    #         # Alternate row colors
    #         for i in range(1, len(table_data)):
    #             if i % 2 == 0:
    #                 for j in range(len(headers)):
    #                     if headers[j] != 'Grand Total':
    #                         table[(i, j)].set_facecolor('#F0F0F0')
    #
    #         # Add title
    #         plt.title('RTWP Abnormal Cells Summary Table\n(Count of cells with RTWP > -95 dBm by Province and Hour)',
    #                   fontsize=14, fontweight='bold', pad=20)
    #
    #         # Add subtitle with date range
    #         date_range = f"Date Range: {summary_table['Date'].min()} to {summary_table['Date'].max()}"
    #         plt.text(0.5, 0.95, date_range, transform=fig.transFigure,
    #                  ha='center', fontsize=10, style='italic')
    #
    #         # Save figure
    #         plt.savefig(output_path, dpi=300, bbox_inches='tight', pad_inches=0.5)
    #         print(f"\nSummary table saved as image: {output_path}")
    #         plt.close()
    #
    #     except Exception as e:
    #         print(f"Error exporting summary table as image: {e}")

    def analyze_rtwp_patterns(self, abnormal_cells_df):
        """Analyze patterns in RTWP data"""
        if abnormal_cells_df is None or abnormal_cells_df.empty:
            return None

        patterns = {
            'daily_pattern': {},
            'hourly_pattern': {},
            'province_severity': {}
        }

        try:
            # Daily pattern
            daily_counts = abnormal_cells_df.groupby('Date')['Cell'].nunique()
            if not daily_counts.empty:
                patterns['daily_pattern'] = {
                    'peak_day': str(daily_counts.idxmax()),
                    'peak_count': int(daily_counts.max()),
                    'average_daily': float(daily_counts.mean())
                }

            # Hourly pattern
            hourly_avg = abnormal_cells_df.groupby('Hour')['RTWP'].mean()
            if not hourly_avg.empty:
                patterns['hourly_pattern'] = {
                    'worst_hour': int(hourly_avg.idxmax()),
                    'worst_hour_avg_rtwp': float(hourly_avg.max()),
                    'best_hour': int(hourly_avg.idxmin()),
                    'best_hour_avg_rtwp': float(hourly_avg.min())
                }

            # Province severity
            province_severity = abnormal_cells_df.groupby('Province')['RTWP'].mean().sort_values(ascending=False).head(
                5)
            if not province_severity.empty:
                patterns['province_severity'] = province_severity.to_dict()

            return patterns
        except Exception as e:
            print(f"Error analyzing patterns: {e}")
            return None

    def plot_top_provinces(self, summary_table, top_n=4, save_path='rtwp_trend_analysis.png'):
        """Plot line chart for top N provinces with most abnormal cells"""
        if summary_table is None or summary_table.empty:
            print("No data to plot")
            return

        try:
            # Get province columns (exclude Date, Hour, Grand Total)
            province_cols = [col for col in summary_table.columns
                             if col not in ['Date', 'Hour', 'Grand Total']]

            # Calculate total cells per province
            province_totals = summary_table[province_cols].sum().sort_values(ascending=False)

            # Get top N provinces
            top_provinces = province_totals.head(top_n).index.tolist()

            print(f"\nTop {top_n} provinces with most abnormal cells:")
            for i, prov in enumerate(top_provinces, 1):
                print(f"{i}. {prov}: {int(province_totals[prov])} cells")

            # Create datetime column for plotting
            summary_table['DateTime'] = pd.to_datetime(summary_table['Date']) + \
                                        pd.to_timedelta(summary_table['Hour'], unit='h')

            # Sort by datetime
            summary_table = summary_table.sort_values('DateTime')

            # Create the plot with subplots
            fig = plt.figure(figsize=(16, 10))

            # Main plot
            ax1 = plt.subplot(2, 1, 1)

            # Color palette
            colors = ['#FF6B6B', '#4ECDC4', '#45B7D1', '#96CEB4']
            markers = ['o', 's', '^', 'D']

            # Plot lines for each top province
            for i, province in enumerate(top_provinces):
                ax1.plot(summary_table['DateTime'],
                         summary_table[province],
                         label=province,
                         color=colors[i % len(colors)],
                         marker=markers[i % len(markers)],
                         markersize=4,
                         linewidth=2,
                         alpha=0.8)

            # Formatting main plot
            ax1.set_xlabel('Date and Hour', fontsize=12, fontweight='bold')
            ax1.set_ylabel('Number of Cells with RTWP > -95 dBm', fontsize=12, fontweight='bold')
            ax1.set_title('RTWP Abnormal Cells Trend Analysis by Province',
                          fontsize=14, fontweight='bold', pad=20)

            # Format x-axis
            ax1.xaxis.set_major_formatter(mdates.DateFormatter('%m-%d %H:00'))
            ax1.xaxis.set_major_locator(mdates.HourLocator(interval=6))
            plt.setp(ax1.xaxis.get_majorticklabels(), rotation=45, ha='right')

            # Add grid
            ax1.grid(True, alpha=0.3, linestyle='--')
            ax1.set_facecolor('#F8F9FA')

            # Legend
            ax1.legend(loc='upper left', frameon=True, shadow=True,
                       title='Top Provinces', title_fontsize=11)

            # Add statistics box
            peak_idx = summary_table['Grand Total'].idxmax()
            peak_hour = summary_table.loc[peak_idx, 'Hour']
            peak_date = summary_table.loc[peak_idx, 'Date']
            peak_value = summary_table.loc[peak_idx, 'Grand Total']

            stats_text = f"Analysis Period: {summary_table['Date'].min()} to {summary_table['Date'].max()}\n"
            stats_text += f"Total Provinces: {len(province_cols)}\n"
            stats_text += f"Peak: {peak_date} {peak_hour}:00 ({int(peak_value)} cells)"

            props = dict(boxstyle='round', facecolor='wheat', alpha=0.5)
            ax1.text(0.98, 0.97, stats_text, transform=ax1.transAxes, fontsize=9,
                     verticalalignment='top', horizontalalignment='right', bbox=props)

            # Add hourly distribution subplot
            ax2 = plt.subplot(2, 1, 2)

            # Calculate hourly average for top provinces
            hourly_avg = summary_table.groupby('Hour')[top_provinces].mean()

            # Create bar chart for hourly distribution
            x = np.arange(24)
            width = 0.2

            for i, province in enumerate(top_provinces[:min(4, len(top_provinces))]):
                if province in hourly_avg.columns:
                    offset = width * (i - len(top_provinces) / 2 + 0.5)
                    ax2.bar(x + offset,
                            [hourly_avg.loc[h, province] if h in hourly_avg.index else 0 for h in x],
                            width,
                            label=province,
                            color=colors[i % len(colors)],
                            alpha=0.7)

            ax2.set_xlabel('Hour of Day', fontsize=12, fontweight='bold')
            ax2.set_ylabel('Average Number of Abnormal Cells', fontsize=12, fontweight='bold')
            ax2.set_title('Hourly Distribution of Abnormal Cells', fontsize=13, fontweight='bold')
            ax2.set_xticks(x)
            ax2.set_xticklabels([f'{h:02d}' for h in x])
            ax2.grid(True, alpha=0.3, axis='y', linestyle='--')
            ax2.legend(loc='upper right')
            ax2.set_facecolor('#F8F9FA')

            # Adjust layout and save
            plt.tight_layout()
            plt.savefig(save_path, dpi=300, bbox_inches='tight')
            print(f"\nChart saved to: {save_path}")
            plt.show()

        except Exception as e:
            print(f"Error creating plot: {e}")
            import traceback
            traceback.print_exc()

    def export_summary_table_as_image(self, summary_table, output_path='rtwp_summary_table.png', abnormal_cells_df=None):
        """Export summary table as PNG image"""
        if summary_table is None or summary_table.empty:
            print("No data to export as image")
            return

        try:
            # Prepare data for display
            display_df = summary_table.copy()

            # Limit rows for better visibility
            # max_rows = 169
            # if len(display_df) > max_rows:
            #     display_df = display_df.head(max_rows)
            #     print(f"Note: Showing first {max_rows} rows out of {len(summary_table)} total rows")

            # Create figure
            fig, ax = plt.subplots(figsize=(20, min(12, len(display_df) * 0.5 + 2)))
            ax.axis('tight')
            ax.axis('off')

            # Prepare table data
            table_data = []
            headers = list(display_df.columns)

            # Add data rows
            for idx, row in display_df.iterrows():
                table_data.append([str(val) for val in row.values])

            # Create table
            table = ax.table(cellText=table_data,
                             colLabels=headers,
                             cellLoc='center',
                             loc='center',
                             colWidths=[0.08 if col not in ['Date'] else 0.12 for col in headers])

            # Style the table
            table.auto_set_font_size(False)
            table.set_fontsize(8)
            table.scale(1.2, 1.5)

            # Color header
            for i in range(len(headers)):
                table[(0, i)].set_facecolor('#4CAF50')
                table[(0, i)].set_text_props(weight='bold', color='white')

            # Color Grand Total column if exists
            if 'Grand Total' in headers:
                gt_idx = headers.index('Grand Total')
                for i in range(1, len(table_data) + 1):
                    table[(i, gt_idx)].set_facecolor('#FFE5B4')
                    table[(i, gt_idx)].set_text_props(weight='bold')

            # Highlight Ha Noi column if exists
            if 'Ha Noi' in headers:
                hn_idx = headers.index('Ha Noi')
                for i in range(1, len(table_data) + 1):
                    table[(i, hn_idx)].set_facecolor('#E6F3FF')

            # Alternate row colors
            for i in range(1, len(table_data) + 1):
                if i % 2 == 0:
                    for j in range(len(headers)):
                        if headers[j] not in ['Grand Total', 'Ha Noi']:
                            table[(i, j)].set_facecolor('#F0F0F0')

            # Add title
            # plt.title('RTWP Abnormal Cells Summary Table\n(Count of cells with RTWP > -95 dBm by Province and Hour)',
            #           fontsize=14, fontweight='bold', pad=20)

            # Add date range subtitle
            # date_range = f"Date Range: {summary_table['Date'].min()} to {summary_table['Date'].max()}"
            # total_cells = summary_table['Grand Total'].sum() if 'Grand Total' in summary_table.columns else 0
            # subtitle = f"{date_range} | Total Cells: {int(total_cells)}"
            # plt.text(0.5, 0.95, subtitle, transform=fig.transFigure,
            #          ha='center', fontsize=10, style='italic')

            # Save figure
            plt.savefig(output_path, dpi=300, bbox_inches='tight', pad_inches=0.5)
            print(f"\nSummary table saved as image: {output_path}")
            plt.close()

        except Exception as e:
            print(f"Error exporting summary table as image: {e}")
            import traceback
            traceback.print_exc()
        """Export summary table and detailed data to Excel with formatting"""
        if summary_table is None or summary_table.empty:
            print("No data to export")
            return

        try:
            with pd.ExcelWriter(output_path, engine='openpyxl') as writer:
                # Write summary table
                summary_table.to_excel(writer, sheet_name='RTWP Summary', index=False)

                # Write detailed data if available
                if abnormal_cells_df is not None and not abnormal_cells_df.empty:
                    abnormal_cells_df.to_excel(writer, sheet_name='Detailed Data', index=False)

                    # Create province statistics
                    province_stats = abnormal_cells_df.groupby('Province').agg({
                        'Cell': 'nunique',
                        'RTWP': ['mean', 'max', 'min', 'count']
                    }).round(2)
                    province_stats.columns = ['Unique_Cells', 'Avg_RTWP', 'Max_RTWP', 'Min_RTWP', 'Total_Records']
                    province_stats = province_stats.sort_values('Unique_Cells', ascending=False)
                    province_stats.to_excel(writer, sheet_name='Province Statistics')

                    # Create hourly statistics
                    hourly_stats = abnormal_cells_df.groupby('Hour').agg({
                        'Cell': 'nunique',
                        'RTWP': 'mean'
                    }).round(2)
                    hourly_stats.columns = ['Unique_Cells', 'Avg_RTWP']
                    hourly_stats.to_excel(writer, sheet_name='Hourly Statistics')

                print(f"Summary exported to: {output_path}")

        except Exception as e:
            print(f"Error exporting to Excel: {e}")

    def generate_detailed_report(self, abnormal_cells_df, output_path='rtwp_detailed_report.txt'):
        """Generate detailed text report of the analysis"""
        if abnormal_cells_df is None or abnormal_cells_df.empty:
            print("No data for report generation")
            return

        try:
            with open(output_path, 'w', encoding='utf-8') as f:
                f.write("=" * 80 + "\n")
                f.write("RTWP ABNORMAL CELLS ANALYSIS REPORT\n")
                f.write("=" * 80 + "\n\n")

                # Overview
                f.write("1. OVERVIEW\n")
                f.write("-" * 40 + "\n")
                f.write(
                    f"Analysis Date Range: {abnormal_cells_df['Date'].min()} to {abnormal_cells_df['Date'].max()}\n")
                f.write(f"Total Abnormal Records: {len(abnormal_cells_df)}\n")
                f.write(f"Unique Cells Affected: {abnormal_cells_df['Cell'].nunique()}\n")
                f.write(f"Provinces Affected: {abnormal_cells_df['Province'].nunique()}\n")
                f.write(f"RTWP Threshold: > -95 dBm\n\n")

                # Top affected provinces
                f.write("2. TOP AFFECTED PROVINCES\n")
                f.write("-" * 40 + "\n")
                province_summary = abnormal_cells_df.groupby('Province').agg({
                    'Cell': 'nunique',
                    'RTWP': ['mean', 'max']
                }).round(2)
                province_summary.columns = ['Unique_Cells', 'Avg_RTWP', 'Max_RTWP']
                province_summary = province_summary.sort_values('Unique_Cells', ascending=False).head(10)

                for idx, (province, row) in enumerate(province_summary.iterrows(), 1):
                    f.write(f"{idx}. {province}:\n")
                    f.write(f"   - Unique Cells: {int(row['Unique_Cells'])}\n")
                    f.write(f"   - Average RTWP: {row['Avg_RTWP']} dBm\n")
                    f.write(f"   - Maximum RTWP: {row['Max_RTWP']} dBm\n")

                # Peak hours analysis
                f.write("\n3. PEAK HOURS ANALYSIS\n")
                f.write("-" * 40 + "\n")
                hourly_summary = abnormal_cells_df.groupby('Hour')['Cell'].nunique().sort_values(ascending=False).head(
                    5)

                for hour, count in hourly_summary.items():
                    f.write(f"Hour {hour:02d}:00 - {count} unique cells\n")

                # Critical cells
                f.write("\n4. MOST CRITICAL CELLS (Highest RTWP)\n")
                f.write("-" * 40 + "\n")
                critical_cells = abnormal_cells_df.groupby('Cell').agg({
                    'RTWP': 'mean',
                    'Province': 'first'
                }).sort_values('RTWP', ascending=False).head(10)

                for idx, (cell, row) in enumerate(critical_cells.iterrows(), 1):
                    f.write(f"{idx}. Cell: {cell}\n")
                    f.write(f"   - Province: {row['Province']}\n")
                    f.write(f"   - Average RTWP: {row['RTWP']:.2f} dBm\n")

                # Province distribution
                f.write("\n5. PROVINCE DISTRIBUTION\n")
                f.write("-" * 40 + "\n")
                province_dist = abnormal_cells_df.groupby('Province')['Cell'].nunique().sort_values(ascending=False)
                for province, count in province_dist.items():
                    f.write(f"{province}: {count} cells\n")

                f.write("\n" + "=" * 80 + "\n")
                f.write("END OF REPORT\n")
                f.write("=" * 80 + "\n")

            print(f"Detailed report saved to: {output_path}")

        except Exception as e:
            print(f"Error generating report: {e}")

    def create_pdf_from_images(self,image1_path, image2_path, output_pdf_path):
        """
        Tạo file PDF từ 2 ảnh với tiêu đề và thời gian tạo

        Args:
            image1_path (str): Đường dẫn đến ảnh thứ nhất
            image2_path (str): Đường dẫn đến ảnh thứ hai
            output_pdf_path (str): Đường dẫn file PDF output
        """
        # Kiểm tra file ảnh có tồn tại không
        if not os.path.exists(image1_path):
            raise FileNotFoundError(f"Không tìm thấy ảnh: {image1_path}")
        if not os.path.exists(image2_path):
            raise FileNotFoundError(f"Không tìm thấy ảnh: {image2_path}")

            # Tạo canvas PDF với khổ A3 dọc
        c = canvas.Canvas(output_pdf_path, pagesize=A3)
        width, height = A3  # A3: 842 x 1191 points

        print(f"Kích thước trang A3: {width:.0f} x {height:.0f} points")

        # Lấy thời gian hiện tại
        current_time = datetime.now()
        date_str = current_time.strftime("%d/%m/%Y")
        time_str = current_time.strftime("%H:%M:%S")

        # Vẽ tiêu đề
        c.setFont("Helvetica-Bold", 20)
        title = "Count Abnormal Cell For 3G"
        title_width = c.stringWidth(title, "Helvetica-Bold", 20)
        c.drawString((width - title_width) / 2, height - 35, title)

        # Vẽ ngày tháng và giờ
        c.setFont("Helvetica", 14)
        datetime_text = f"{date_str} - {time_str}"
        datetime_width = c.stringWidth(datetime_text, "Helvetica", 14)
        c.drawString((width - datetime_width) / 2, height - 60, datetime_text)

        # Tính toán không gian có sẵn - margin siêu tối thiểu để giãn tối đa
        top_margin = 75  # Sau tiêu đề và ngày tháng
        bottom_margin = 5  # Margin dưới siêu tối thiểu
        side_margin = 5  # Margin trái/phải siêu tối thiểu
        gap_between_images = 10  # Khoảng cách giữa 2 ảnh tối thiểu

        available_height = height - top_margin - bottom_margin - gap_between_images
        available_width = width - (side_margin * 2)

        print(f"Không gian khả dụng: {available_width:.0f} x {available_height:.0f}")

        # Mở và phân tích ảnh
        img1 = Image.open(image1_path)
        img2 = Image.open(image2_path)

        img1_aspect = img1.width / img1.height
        img2_aspect = img2.width / img2.height

        print(f"Ảnh 1 (bảng): {img1.width} x {img1.height}, tỷ lệ: {img1_aspect:.2f}")
        print(f"Ảnh 2 (biểu đồ): {img2.width} x {img2.height}, tỷ lệ: {img2_aspect:.2f}")

        # CHIẾN LƯỢC: Kéo giãn cả 2 ảnh ra toàn bộ chiều rộng
        # Mỗi ảnh sẽ có chiều rộng = available_width

        # Tính chiều cao cho ảnh 1 khi kéo giãn toàn bộ chiều rộng
        img1_width = available_width
        img1_height = img1_width / img1_aspect

        # Tính chiều cao cho ảnh 2 khi kéo giãn toàn bộ chiều rộng
        img2_width = available_width
        img2_height = img2_width / img2_aspect

        # Kiểm tra tổng chiều cao có vượt quá không gian không
        total_images_height = img1_height + img2_height

        if total_images_height > available_height:
            # Nếu vượt quá, cần điều chỉnh tỷ lệ
            scale_factor = available_height / total_images_height

            img1_height = img1_height * scale_factor
            img1_width = img1_height * img1_aspect

            img2_height = img2_height * scale_factor
            img2_width = img2_height * img2_aspect

            print(f"Điều chỉnh tỷ lệ: {scale_factor:.3f}")
        else:
            print("Không cần điều chỉnh tỷ lệ - đủ không gian")

        # Tính vị trí ảnh 1 (ảnh trên)
        img1_x = (width - img1_width) / 2
        img1_y = height - top_margin - img1_height

        # Tính vị trí ảnh 2 (ảnh dưới)
        img2_x = (width - img2_width) / 2
        img2_y = img1_y - gap_between_images - img2_height

        # Vẽ ảnh 1 (bảng tóm tắt)
        c.drawImage(image1_path, img1_x, img1_y, img1_width, img1_height)

        # Vẽ ảnh 2 (biểu đồ)
        c.drawImage(image2_path, img2_x, img2_y, img2_width, img2_height)

        # Lưu PDF
        c.save()

        # Thông tin chi tiết
        print(f"PDF A3 đã được tạo thành công: {output_pdf_path}")
        print(f"Kích thước ảnh bảng tóm tắt: {img1_width:.0f} x {img1_height:.0f}")
        print(f"Kích thước ảnh biểu đồ: {img2_width:.0f} x {img2_height:.0f}")

        # Tính tỷ lệ sử dụng không gian
        total_page_area = width * height
        used_area = (img1_width * img1_height) + (img2_width * img2_height)
        usage_percentage = (used_area / total_page_area) * 100

        print(f"Tỷ lệ sử dụng không gian trang:")
        print(f"  - Ảnh bảng tóm tắt: {(img1_width * img1_height) / total_page_area * 100:.1f}%")
        print(f"  - Ảnh biểu đồ: {(img2_width * img2_height) / total_page_area * 100:.1f}%")
        print(f"  - Tổng cộng: {usage_percentage:.1f}%")
        print(f"  - Chiều rộng sử dụng: {max(img1_width, img2_width) / width * 100:.1f}% của trang")


def main():
    processor = CountAbnormalCellFor3G()

    # File mappings
    excel_files_ericsson = {'RTWP_3G.xlsx': 'RTWP_3G_Ericsson.csv'}
    excel_files_zte = {'History Performance_UMTS _RNO_Avg_Mean_RTWP.xlsx': 'RTWP_3G_ZTE.csv'}

    all_abnormal_cells = []

    print("=" * 80)
    print("RTWP ABNORMAL CELL ANALYSIS SYSTEM")
    print("=" * 80)

    # Process ZTE files
    print("\n[STEP 1] Processing ZTE Files...")
    print("-" * 40)
    for excel_file, csv_file in excel_files_zte.items():
        if os.path.exists(excel_file):
            # Convert Excel to CSV
            df = processor.clean_excel_to_csv_ZTE(excel_file, csv_file)
            if df is not None and processor.verify_csv_structure(csv_file):
                # Count abnormal cells using ZTE-specific function
                abnormal_cells = processor.count_abnormal_cells_zte(csv_file, rtwp_threshold=-95)
                if abnormal_cells is not None and not abnormal_cells.empty:
                    all_abnormal_cells.append(abnormal_cells)
        else:
            print(f"Warning: File not found: {excel_file}")

    # Process Ericsson files
    print("\n[STEP 2] Processing Ericsson Files...")
    print("-" * 40)
    for excel_file, csv_file in excel_files_ericsson.items():
        if os.path.exists(excel_file):
            # Convert Excel to CSV
            df = processor.clean_excel_to_csv_ericsson(excel_file, csv_file)
            if df is not None and processor.verify_csv_structure(csv_file):
                # Count abnormal cells using Ericsson-specific function
                abnormal_cells = processor.count_abnormal_cells_ericsson(csv_file, rtwp_threshold=-95)
                if abnormal_cells is not None and not abnormal_cells.empty:
                    all_abnormal_cells.append(abnormal_cells)
        else:
            print(f"Warning: File not found: {excel_file}")

    # Combine all results
    if all_abnormal_cells:
        print("\n[STEP 3] Combining and Analyzing Results...")
        print("-" * 40)

        combined_df = pd.concat(all_abnormal_cells, ignore_index=True)
        print(f"Total abnormal records found: {len(combined_df)}")
        print(f"Unique cells: {combined_df['Cell'].nunique()}")

        # Create summary table
        print("\n[STEP 4] Creating Summary Table...")
        summary_table = processor.create_summary_table(combined_df)

        if summary_table is not None:
            # Display first few rows
            print("\nSummary Table Preview (first 10 rows):")
            print(summary_table.head(10))

            # Check for Ha Noi specifically
            if 'Ha Noi' in summary_table.columns:
                ha_noi_total = summary_table['Ha Noi'].sum()
                print(f"\n*** Ha Noi Total Cells: {int(ha_noi_total)} ***")

            # Analyze patterns
            print("\n[STEP 5] Analyzing Patterns...")
            patterns = processor.analyze_rtwp_patterns(combined_df)
            if patterns:
                print("\nPattern Analysis:")
                if patterns.get('daily_pattern'):
                    print(f"  • Peak Day: {patterns['daily_pattern'].get('peak_day', 'N/A')} "
                          f"({patterns['daily_pattern'].get('peak_count', 0)} cells)")
                if patterns.get('hourly_pattern'):
                    print(f"  • Worst Hour: {patterns['hourly_pattern'].get('worst_hour', 'N/A')}:00 "
                          f"(Avg RTWP: {patterns['hourly_pattern'].get('worst_hour_avg_rtwp', 0):.2f} dBm)")
                if patterns.get('province_severity'):
                    if patterns['province_severity']:
                        top_province = list(patterns['province_severity'].keys())[0]
                        top_severity = list(patterns['province_severity'].values())[0]
                        print(f"  • Most Severe Province: {top_province} ({top_severity:.2f} dBm)")

            # Export summary table as PNG image
            print("\n[STEP 6] Exporting Summary Table as Image...")
            processor.export_summary_table_as_image(summary_table, 'rtwp_summary_table.png',combined_df)

            # Export to Excel (for reference)
            print("\n[STEP 7] Exporting to Excel...")
            processor.export_summary_to_excel(
                summary_table,
                combined_df,
               'rtwp_analysis_data.xlsx'
            )

            # Generate detailed text report
            print("\n[STEP 8] Generating Detailed Report...")
            processor.generate_detailed_report(combined_df, 'rtwp_analysis_report.txt')

            # Create visualization
            print("\n[STEP 9] Creating Visualizations...")
            processor.plot_top_provinces(summary_table, top_n=4, save_path='rtwp_trend_chart.png')

            # Final statistics
            print("\n" + "=" * 80)
            print("ANALYSIS COMPLETE - SUMMARY STATISTICS")
            print("=" * 80)
            print(f"Date Range: {combined_df['Date'].min()} to {combined_df['Date'].max()}")
            print(f"Provinces Affected: {combined_df['Province'].nunique()}")
            print(f"Unique Cells with Issues: {combined_df['Cell'].nunique()}")
            print(f"Total Abnormal Readings: {len(combined_df)}")

            # Province breakdown
            print("\nProvince Breakdown:")
            province_counts = combined_df.groupby('Province')['Cell'].nunique().sort_values(ascending=False)
            for province, count in province_counts.head(10).items():
                print(f"  {province}: {count} cells")

            # Top 5 worst cells
            print("\nTop 5 Critical Cells (Highest Average RTWP):")
            worst_cells = combined_df.groupby('Cell').agg({
                'RTWP': 'mean',
                'Province': 'first'
            }).sort_values('RTWP', ascending=False).head(5)

            for idx, (cell, row) in enumerate(worst_cells.iterrows(), 1):
                print(f"   {idx}. {cell} ({row['Province']}): {row['RTWP']:.2f} dBm")

            print("\nOutput Files Generated:")
            print("   1. rtwp_summary_table.png - Summary table as image")
            print("   2. rtwp_analysis_data.xlsx - Complete Excel analysis")
            print("   3. rtwp_analysis_report.txt - Detailed text report")
            print("   4. rtwp_trend_chart.png - Trend visualization chart")
            print("   5. RTWP_3G_Ericsson.csv - Cleaned Ericsson data")
            print("   6. RTWP_3G_ZTE.csv - Cleaned ZTE data")

    else:
        print("\nNo abnormal cells found in any files")
        print("Possible reasons:")
        print("  1. No cells have RTWP > -95 dBm")
        print("  2. Cell names don't match the province mapping")
        print("  3. Data format is different than expected")
        print("\nPlease check:")
        print("  - CSV files were created successfully")
        print("  - Cell names start with codes like U124, U125, etc.")
        print("  - RTWP values are in dBm format")

    try:
        processor.create_pdf_from_images(
            image1_path= 'rtwp_summary_table.png' ,
            image2_path= 'rtwp_trend_chart.png' ,
            output_pdf_path= 'rtwp_analysis_report.pdf'
        )
    except FileNotFoundError as e:
        print(f"Lỗi: {e}")
    except Exception as e:
        print(f"Có lỗi xảy ra: {e}")
    print("\n" + "=" * 80)
    print("PROCESSING COMPLETE - Thank you for using RTWP Analysis System")
    print("=" * 80)


if __name__ == "__main__":
    main()