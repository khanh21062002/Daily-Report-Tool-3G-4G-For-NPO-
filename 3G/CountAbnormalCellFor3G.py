import os
from datetime import timedelta, datetime
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

            date_col = df.columns[0]
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

    # ========== NEW FUNCTIONS FOR CELL COUNTING ==========

    def get_province_from_cell(self, cell_name):
        """Extract province from cell name based on first 4 characters"""
        if pd.isna(cell_name):
            return None
        cell_str = str(cell_name).strip()
        if len(cell_str) >= 4:
            prefix = cell_str[:4].upper()
            return self.province_mapping.get(prefix, None)
        return None

    def count_abnormal_cells_by_province(self, csv_path, vendor='ericsson', rtwp_threshold=-95):
        """Count cells with RTWP > threshold by province and hour"""
        try:
            # Read CSV file
            df = pd.read_csv(csv_path)
            print(f"\nProcessing {vendor.upper()} data from {csv_path}")
            print(f"Total rows: {len(df)}")
            print(f"Columns: {list(df.columns[:10])}")  # Show first 10 columns for debugging

            # Find date/time column
            date_col = None
            for col in df.columns:
                if 'date' in col.lower() or 'time' in col.lower():
                    date_col = col
                    break

            if date_col is None:
                print("Error: No date/time column found")
                return None

            print(f"Using date column: {date_col}")

            # Convert to datetime - handle different formats
            # First, check if already datetime
            if df[date_col].dtype == 'object' or df[date_col].dtype == 'string':
                # Try multiple date formats
                for date_format in ['%Y-%m-%d %H:%M:%S', '%d/%m/%Y %H:%M', '%Y/%m/%d %H:%M:%S', None]:
                    try:
                        if date_format:
                            df[date_col] = pd.to_datetime(df[date_col], format=date_format, errors='coerce')
                        else:
                            df[date_col] = pd.to_datetime(df[date_col], errors='coerce', dayfirst=True)
                        break
                    except:
                        continue

            # Check if conversion was successful
            if df[date_col].dtype != 'datetime64[ns]':
                print(f"Error: Could not convert {date_col} to datetime. Current dtype: {df[date_col].dtype}")
                print(f"Sample values: {df[date_col].head()}")
                return None

            # Remove rows with invalid dates
            df = df.dropna(subset=[date_col])

            if len(df) == 0:
                print("Error: No valid dates found after conversion")
                return None

            # Extract date and hour
            df['Date'] = df[date_col].dt.date
            df['Hour'] = df[date_col].dt.hour

            # Process based on vendor
            results = []

            if vendor.lower() == 'ericsson':
                # For Ericsson: columns are UCell IDs
                # Identify RTWP value columns (exclude date, metadata columns)
                cell_columns = []
                for col in df.columns:
                    if col not in [date_col, 'Date', 'Hour']:
                        # Check if column name looks like a cell ID (starts with U followed by numbers)
                        col_str = str(col).strip()
                        if col_str.startswith('U') and len(col_str) >= 4:
                            cell_columns.append(col)

                print(f"Found {len(cell_columns)} cell columns")

                if not cell_columns:
                    print("Warning: No cell columns found. Checking all numeric columns...")
                    # If no U-prefixed columns, try all numeric columns
                    cell_columns = [col for col in df.select_dtypes(include=[np.number]).columns
                                    if col not in ['Date', 'Hour']]

                for idx, row in df.iterrows():
                    for cell_col in cell_columns:
                        if pd.notna(row[cell_col]):
                            try:
                                rtwp_value = float(row[cell_col])
                                if rtwp_value > rtwp_threshold:
                                    province = self.get_province_from_cell(cell_col)
                                    if province:
                                        results.append({
                                            'Date': row['Date'],
                                            'Hour': row['Hour'],
                                            'Cell': cell_col,
                                            'Province': province,
                                            'RTWP': rtwp_value
                                        })
                            except (ValueError, TypeError):
                                continue

            elif vendor.lower() == 'zte':
                # For ZTE: Need to identify Cell Name and RTWP columns
                print("Processing ZTE data structure...")

                # Look for cell name column
                cell_name_col = None
                for col in df.columns:
                    col_lower = col.lower()
                    if 'cell' in col_lower and ('name' in col_lower or 'id' in col_lower):
                        cell_name_col = col
                        break
                    # Also check if column contains cell-like values
                    elif df[col].dtype == 'object':
                        sample_val = df[col].dropna().iloc[0] if len(df[col].dropna()) > 0 else ''
                        if str(sample_val).startswith('U') and len(str(sample_val)) >= 4:
                            cell_name_col = col
                            break

                if cell_name_col:
                    print(f"Cell name column: {cell_name_col}")

                    # Find RTWP value columns
                    rtwp_cols = []
                    for col in df.columns:
                        if col not in [date_col, cell_name_col, 'Date', 'Hour']:
                            # Check if numeric column
                            if df[col].dtype in [np.float64, np.int64]:
                                rtwp_cols.append(col)
                            elif 'rtwp' in col.lower() or 'avg' in col.lower() or 'mean' in col.lower():
                                rtwp_cols.append(col)

                    print(f"RTWP columns found: {len(rtwp_cols)}")

                    for idx, row in df.iterrows():
                        cell_name = row[cell_name_col]
                        province = self.get_province_from_cell(cell_name)

                        if province:
                            for rtwp_col in rtwp_cols:
                                if pd.notna(row[rtwp_col]):
                                    try:
                                        rtwp_value = float(row[rtwp_col])
                                        if rtwp_value > rtwp_threshold:
                                            results.append({
                                                'Date': row['Date'],
                                                'Hour': row['Hour'],
                                                'Cell': cell_name,
                                                'Province': province,
                                                'RTWP': rtwp_value
                                            })
                                    except (ValueError, TypeError):
                                        continue
                else:
                    print("Warning: Could not identify cell name column in ZTE data")
                    # Try to process as column-based structure like Ericsson
                    print("Attempting to process as column-based structure...")

                    cell_columns = []
                    for col in df.columns:
                        if col not in [date_col, 'Date', 'Hour']:
                            col_str = str(col).strip()
                            if col_str.startswith('U') and len(col_str) >= 4:
                                cell_columns.append(col)

                    if cell_columns:
                        for idx, row in df.iterrows():
                            for cell_col in cell_columns:
                                if pd.notna(row[cell_col]):
                                    try:
                                        rtwp_value = float(row[cell_col])
                                        if rtwp_value > rtwp_threshold:
                                            province = self.get_province_from_cell(cell_col)
                                            if province:
                                                results.append({
                                                    'Date': row['Date'],
                                                    'Hour': row['Hour'],
                                                    'Cell': cell_col,
                                                    'Province': province,
                                                    'RTWP': rtwp_value
                                                })
                                    except (ValueError, TypeError):
                                        continue

            if results:
                result_df = pd.DataFrame(results)
                print(f"Found {len(result_df)} cells with RTWP > {rtwp_threshold} dBm")
                return result_df
            else:
                print("No abnormal cells found")
                return pd.DataFrame()

        except Exception as e:
            print(f"Error processing {csv_path}: {e}")
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

                f.write("\n" + "=" * 80 + "\n")
                f.write("END OF REPORT\n")
                f.write("=" * 80 + "\n")

            print(f"Detailed report saved to: {output_path}")

        except Exception as e:
            print(f"Error generating report: {e}")


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
                # Count abnormal cells
                abnormal_cells = processor.count_abnormal_cells_by_province(
                    csv_file, vendor='zte', rtwp_threshold=-95
                )
                if abnormal_cells is not None and not abnormal_cells.empty:
                    all_abnormal_cells.append(abnormal_cells)
        else:
            print(f"⚠ Warning: File not found: {excel_file}")

    # Process Ericsson files
    print("\n[STEP 2] Processing Ericsson Files...")
    print("-" * 40)
    for excel_file, csv_file in excel_files_ericsson.items():
        if os.path.exists(excel_file):
            # Convert Excel to CSV
            df = processor.clean_excel_to_csv_ericsson(excel_file, csv_file)
            if df is not None and processor.verify_csv_structure(csv_file):
                # Count abnormal cells
                abnormal_cells = processor.count_abnormal_cells_by_province(
                    csv_file, vendor='ericsson', rtwp_threshold=-95
                )
                if abnormal_cells is not None and not abnormal_cells.empty:
                    all_abnormal_cells.append(abnormal_cells)
        else:
            print(f"⚠ Warning: File not found: {excel_file}")

    # Combine all results
    if all_abnormal_cells:
        print("\n[STEP 3] Combining and Analyzing Results...")
        print("-" * 40)

        combined_df = pd.concat(all_abnormal_cells, ignore_index=True)
        print(f"✓ Total abnormal cells found: {len(combined_df)}")

        # Create summary table
        print("\n[STEP 4] Creating Summary Table...")
        summary_table = processor.create_summary_table(combined_df)

        if summary_table is not None:
            # Display first few rows
            print("\n📊 Summary Table Preview (first 5 rows):")
            print(summary_table.head(5))

            # Analyze patterns
            print("\n[STEP 5] Analyzing Patterns...")
            patterns = processor.analyze_rtwp_patterns(combined_df)
            if patterns:
                print("\n🔍 Pattern Analysis:")
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

            # Export to Excel with all sheets
            print("\n[STEP 6] Exporting to Excel...")
            processor.export_summary_to_excel(
                summary_table,
                abnormal_cells_df=combined_df,
                output_path='rtwp_complete_analysis.xlsx'
            )

            # Generate detailed text report
            print("\n[STEP 7] Generating Detailed Report...")
            processor.generate_detailed_report(combined_df, 'rtwp_analysis_report.txt')

            # Create visualization
            print("\n[STEP 8] Creating Visualizations...")
            processor.plot_top_provinces(summary_table, top_n=4, save_path='rtwp_trend_chart.png')

            # Final statistics
            print("\n" + "=" * 80)
            print("📈 ANALYSIS COMPLETE - SUMMARY STATISTICS")
            print("=" * 80)
            print(f"📅 Date Range: {combined_df['Date'].min()} to {combined_df['Date'].max()}")
            print(f"🌍 Provinces Affected: {combined_df['Province'].nunique()}")
            print(f"📡 Unique Cells with Issues: {combined_df['Cell'].nunique()}")
            print(f"📊 Total Abnormal Readings: {len(combined_df)}")

            # Top 5 worst cells
            print("\n🚨 Top 5 Critical Cells (Highest Average RTWP):")
            worst_cells = combined_df.groupby('Cell').agg({
                'RTWP': 'mean',
                'Province': 'first'
            }).sort_values('RTWP', ascending=False).head(5)

            for idx, (cell, row) in enumerate(worst_cells.iterrows(), 1):
                print(f"   {idx}. {cell} ({row['Province']}): {row['RTWP']:.2f} dBm")

            print("\n✅ Output Files Generated:")
            print("   1. rtwp_complete_analysis.xlsx - Complete Excel analysis with multiple sheets")
            print("   2. rtwp_analysis_report.txt - Detailed text report")
            print("   3. rtwp_trend_chart.png - Trend visualization chart")
            print("   4. RTWP_3G_Ericsson.csv - Cleaned Ericsson data")
            print("   5. RTWP_3G_ZTE.csv - Cleaned ZTE data")

    else:
        print("\n❌ No abnormal cells found in any files")
        print("Possible reasons:")
        print("  1. No cells have RTWP > -95 dBm")
        print("  2. Cell names don't match the province mapping")
        print("  3. Data format is different than expected")
        print("\nPlease check:")
        print("  - CSV files were created successfully")
        print("  - Cell names start with codes like U124, U125, etc.")
        print("  - RTWP values are in dBm format")

    print("\n" + "=" * 80)
    print("PROCESSING COMPLETE - Thank you for using RTWP Analysis System")
    print("=" * 80)


if __name__ == "__main__":
    main()