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
        self.ericsson_rncs = ['HLRE01', 'HLRE02', 'HLRE03', 'HLRE04']
        self.zte_rncs = ['HNRZ01(101)', 'HNRZ01(102)']
        # Combined RNC list for unified processing
        self.all_rncs = self.ericsson_rncs + ['HNRZ01']  # Simplified HNRZ01 name for display

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
                      lineterminator="", quoting=csv.QUOTE_MINIMAL)
            print(f"Saved Ericsson CSV: {csv_path}, shape: {df.shape}")
            self.cleaned_data[csv_path] = df
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
                      lineterminator="", quoting=csv.QUOTE_MINIMAL)
            print(f"Saved ZTE CSV: {csv_path}, shape: {df.shape}")
            self.cleaned_data[csv_path] = df
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

    # ========== DATA AGGREGATION ==========
    def aggregate_daily_data(self, df, date_col):
        """Aggregate Ericsson data by date"""
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

            print(f"Aggregated Ericsson data: {len(df_aggregated)} days from {len(df_filtered)} records")
            return df_aggregated

        except Exception as e:
            print(f"Error aggregating Ericsson data: {e}")
            return df

    def aggregate_daily_data_ZTE(self, df, date_col):
        """Aggregate ZTE data by date - FIXED VERSION"""
        try:
            target_rncs = ['HNRZ01(101)', 'HNRZ02(102)']
            df_filtered = df[df['RNC Managed NE Name'].isin(target_rncs)].copy()

            for col in df_filtered.columns:
                if col not in [date_col, 'RNC Managed NE Name']:
                    df_filtered[col] = pd.to_numeric(df_filtered[col], errors='coerce')

            print("ZTE data types after conversion:")
            print(df_filtered.dtypes)

            numeric_cols = df_filtered.select_dtypes(include=[np.number]).columns.tolist()
            if date_col in numeric_cols:
                numeric_cols.remove(date_col)

            # Key fix: Group by BOTH date AND RNC, then aggregate by date only
            agg_dict_by_rnc = {col: 'mean' for col in numeric_cols}

            # First: aggregate by date and RNC to get daily averages per RNC
            df_by_rnc_date = df_filtered.groupby([date_col, 'RNC Managed NE Name']).agg(agg_dict_by_rnc).reset_index()

            print(f"After RNC-date aggregation: {len(df_by_rnc_date)} records")
            print("Sample data by RNC-Date:")
            print(df_by_rnc_date.head())

            # Second: aggregate by date only to get average across both RNCs
            agg_dict_final = {col: 'mean' for col in numeric_cols}

            non_numeric_cols = df_by_rnc_date.select_dtypes(exclude=[np.number]).columns.tolist()
            for col in non_numeric_cols:
                if col not in [date_col, 'RNC Managed NE Name']:
                    agg_dict_final[col] = 'first'

            print("Numeric cols for final aggregation:", numeric_cols)
            print("Non-numeric cols for final aggregation:", non_numeric_cols)

            # Final aggregation: average across both RNCs for each date
            df_aggregated = df_by_rnc_date.groupby(date_col).agg(agg_dict_final).reset_index()

            # Round numeric values
            for col in numeric_cols:
                if col in df_aggregated.columns:
                    df_aggregated[col] = df_aggregated[col].round(2)

            print(f"Final aggregated ZTE data: {len(df_aggregated)} days from {len(df_filtered)} original records")
            print("Final aggregated sample:")
            print(df_aggregated.head())

            return df_aggregated

        except Exception as e:
            print(f"Error aggregating ZTE data: {e}")
            import traceback
            traceback.print_exc()
            return df

    # ========== INDIVIDUAL VENDOR DASHBOARDS ==========
    def create_daily_dashboard_table_ericsson(self, csv_all_day, csv_busy_hour, output_dir):
        """Create Ericsson individual dashboard"""
        try:
            print("\nCreating Ericsson Daily Dashboard...")

            df_all = pd.read_csv(csv_all_day)
            df_bh = pd.read_csv(csv_busy_hour)

            date_col = df_all.columns[0]
            df_all[date_col] = pd.to_datetime(df_all[date_col])
            df_bh[date_col] = pd.to_datetime(df_bh[date_col])

            print(f"Raw data - 24h: {len(df_all)} records, BH: {len(df_bh)} records")

            df_all_agg = self.aggregate_daily_data(df_all, date_col)
            df_bh_agg = self.aggregate_daily_data(df_bh, date_col)

            print(f"Aggregated data - 24h: {len(df_all_agg)} days, BH: {len(df_bh_agg)} days")

            kpi_mapping = self._get_ericsson_kpi_mapping()

            # Create figure
            fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(14, 12))
            fig.suptitle('Daily 3G KPI Dashboard of Ericsson', fontsize=16, fontweight='bold', y=0.95)

            # Get latest dates
            latest_dates = self._get_latest_dates_for_aggregated_data(df_all_agg, date_col, 3)

            # Create dashboards
            self._create_individual_dashboard_subplot(ax1, df_all_agg, latest_dates, date_col, kpi_mapping,
                                                      "Daily 3G KPI Dashboard of Ericsson (24h)", "#FFA500")

            self._create_individual_dashboard_subplot(ax2, df_bh_agg, latest_dates, date_col, kpi_mapping,
                                                      "Daily 3G KPI Dashboard of Ericsson (BH)", "#FF6B35")

            plt.tight_layout()
            plt.subplots_adjust(top=0.93)

            dashboard_path = os.path.join(output_dir, "Daily_3G_KPI_Dashboard_of_Ericsson.png")
            plt.savefig(dashboard_path, dpi=300, bbox_inches='tight', facecolor='white')
            plt.close()

            print(f"Created Ericsson Dashboard: {dashboard_path}")
            return dashboard_path

        except Exception as e:
            print(f"Error creating Ericsson Dashboard: {e}")
            return None

    def create_daily_dashboard_table_ZTE(self, csv_all_day, csv_busy_hour, output_dir):
        """Create ZTE individual dashboard"""
        try:
            print("\nCreating ZTE Daily Dashboard...")

            df_all = pd.read_csv(csv_all_day)
            df_bh = pd.read_csv(csv_busy_hour)

            date_col = df_all.columns[1]
            df_all[date_col] = pd.to_datetime(df_all[date_col])
            df_bh[date_col] = pd.to_datetime(df_bh[date_col])

            print(f"Raw data - 24h: {len(df_all)} records, BH: {len(df_bh)} records")

            df_all_agg = self.aggregate_daily_data_ZTE(df_all, date_col)
            df_bh_agg = self.aggregate_daily_data_ZTE(df_bh, date_col)

            print(f"Aggregated data - 24h: {len(df_all_agg)} days, BH: {len(df_bh_agg)} days")

            kpi_mapping = self._get_zte_kpi_mapping()

            # Create figure
            fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(14, 12))
            fig.suptitle('Daily 3G KPI Dashboard of ZTE', fontsize=16, fontweight='bold', y=0.95)

            # Get latest dates
            latest_dates = self._get_latest_dates_for_aggregated_data(df_all_agg, date_col, 3)

            # Create dashboards
            self._create_individual_dashboard_subplot(ax1, df_all_agg, latest_dates, date_col, kpi_mapping,
                                                      "Daily 3G KPI Dashboard of ZTE (24h)", "#FFA500")

            self._create_individual_dashboard_subplot(ax2, df_bh_agg, latest_dates, date_col, kpi_mapping,
                                                      "Daily 3G KPI Dashboard of ZTE (BH)", "#FF6B35")

            plt.tight_layout()
            plt.subplots_adjust(top=0.93)

            dashboard_path = os.path.join(output_dir, "Daily_3G_KPI_Dashboard_of_ZTE.png")
            plt.savefig(dashboard_path, dpi=300, bbox_inches='tight', facecolor='white')
            plt.close()

            print(f"Created ZTE Dashboard: {dashboard_path}")
            return dashboard_path

        except Exception as e:
            print(f"Error creating ZTE Dashboard: {e}")
            return None

    def _create_individual_dashboard_subplot(self, ax, df, latest_dates, date_col, kpi_mapping, title, header_color):
        """Create subplot for individual vendor dashboard"""
        ax.clear()
        ax.set_xlim(0, 10)
        ax.set_ylim(0, 10)
        ax.axis('off')

        # Title
        ax.text(5, 9.5, title, ha='center', va='center', fontsize=12, fontweight='bold')

        # Table 1: Success Rates & Traffic
        self._draw_individual_dashboard_table(ax, df, latest_dates, date_col, kpi_mapping,
                                              ['CS CSSR', 'HSDPA CSSR', 'CS CDR', 'HSDPA CDR', 'CS Traffic (Erl)'],
                                              [99.00, 98.00, 0.80, 1.50, None],
                                              header_color, y_start=8.5)

        # Table 2: Handover Success Rates & PS Traffic
        self._draw_individual_dashboard_table(ax, df, latest_dates, date_col, kpi_mapping,
                                              ['CS Soft HOSR', 'HSDPA Soft HOSR', 'CS IRAT HOSR', 'PS IRAT HOSR',
                                               'PS Traffic (GB)'],
                                              [99.00, 98.00, 97.00, 92.00, None],
                                              header_color, y_start=4.5)

    def _draw_individual_dashboard_table(self, ax, df, latest_dates, date_col, kpi_mapping, kpi_list, targets,
                                         header_color, y_start):
        """Draw table for individual dashboard"""
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
                value = self._get_individual_kpi_value(df, date, date_col, kpi_name, kpi_mapping)
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
                            diff = prev_val - curr_val
                        elif kpi_name in ['CS Traffic (Erl)', 'PS Traffic (GB)']:
                            diff = (curr_val - prev_val) / prev_val * 100
                            comp_d1.append(f"{diff:+.0f}%")
                            continue
                        else:
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
        self._draw_formatted_table(ax, headers, table_data, targets, header_color, x_start, y_start, col_width,
                                   row_height)

    def _get_individual_kpi_value(self, df, date, date_col, kpi_name, kpi_mapping):
        """Get KPI value for individual dashboard (aggregated data)"""
        possible_cols = kpi_mapping.get(kpi_name, [kpi_name])

        # Filter data for specific date
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

    # ========== BY RNC DASHBOARDS & CHARTS ==========
    def create_daily_rnc_dashboard(self, csv_all_day_ericsson, csv_bh_ericsson, csv_all_day_zte, csv_bh_zte,
                                   output_dir):
        """Create unified RNC dashboard and trend charts"""
        try:
            print("\nCreating Daily 3G KPI Dashboard By RNC...")

            # Load and prepare data
            df_ericsson_24h = self._load_and_prepare_data(csv_all_day_ericsson, vendor="Ericsson")
            df_ericsson_bh = self._load_and_prepare_data(csv_bh_ericsson, vendor="Ericsson")
            df_zte_24h = self._load_and_prepare_data(csv_all_day_zte, vendor="ZTE")
            df_zte_bh = self._load_and_prepare_data(csv_bh_zte, vendor="ZTE")

            # Combine data for unified processing
            combined_24h = self._combine_vendor_data(df_ericsson_24h, df_zte_24h)
            combined_bh = self._combine_vendor_data(df_ericsson_bh, df_zte_bh)

            # Create unified RNC dashboard tables
            self._create_unified_rnc_dashboard_table(combined_24h, combined_bh, output_dir)

            # Create trend charts with all RNCs
            self._create_unified_trend_charts(combined_24h, combined_bh, output_dir)

            print("Completed all RNC Dashboard and charts")

        except Exception as e:
            print(f"Error creating RNC Dashboard: {e}")
            import traceback
            traceback.print_exc()

    def _load_and_prepare_data(self, csv_path, vendor):
        """Load and prepare data with proper metadata"""
        try:
            df = pd.read_csv(csv_path)

            # Determine date column and RNC column based on vendor
            if vendor == "Ericsson":
                date_col = df.columns[0]
                rnc_col = 'RNC Id'
                rncs = self.ericsson_rncs
            else:  # ZTE
                date_cols = [col for col in df.columns if 'time' in col.lower() or 'date' in col.lower()]
                date_col = date_cols[0] if date_cols else df.columns[1]
                rnc_col = 'RNC Managed NE Name'
                rncs = self.zte_rncs

            # Convert date column
            df[date_col] = pd.to_datetime(df[date_col], errors='coerce')

            # Filter for target RNCs
            df = df[df[rnc_col].isin(rncs)].copy()

            # Convert numeric columns
            for col in df.columns:
                if col not in [date_col, rnc_col]:
                    df[col] = pd.to_numeric(df[col], errors='coerce')

            # Add metadata
            df._date_col = date_col
            df._rnc_col = rnc_col
            df._rncs = rncs
            df._vendor = vendor

            print(f"Loaded {vendor} data: {len(df)} rows")
            return df

        except Exception as e:
            print(f"Error loading {vendor} data: {e}")
            import traceback
            traceback.print_exc()

            # Return empty DataFrame with proper metadata
            empty_df = pd.DataFrame()
            if vendor == "Ericsson":
                empty_df._date_col = 'Date'
                empty_df._rnc_col = 'RNC Id'
                empty_df._rncs = self.ericsson_rncs
            else:
                empty_df._date_col = 'Start Time'
                empty_df._rnc_col = 'RNC Managed NE Name'
                empty_df._rncs = self.zte_rncs
            empty_df._vendor = vendor

            return empty_df

    def _combine_vendor_data(self, df_ericsson, df_zte):
        """Combine Ericsson and ZTE data into unified format"""
        try:
            combined_data = []

            # Process Ericsson data
            if not df_ericsson.empty and hasattr(df_ericsson, '_rnc_col') and hasattr(df_ericsson, '_date_col'):
                ericsson_data = df_ericsson.copy()
                ericsson_data['RNC_Standard'] = ericsson_data[df_ericsson._rnc_col]
                ericsson_data['Date_Standard'] = ericsson_data[df_ericsson._date_col]
                combined_data.append(ericsson_data)
            elif not df_ericsson.empty:
                print("Warning: Ericsson data missing metadata attributes")

            # Process ZTE data
            if not df_zte.empty and hasattr(df_zte, '_rnc_col') and hasattr(df_zte, '_date_col'):
                zte_data = df_zte.copy()
                zte_data['RNC_Standard'] = 'HNRZ01'  # Combine both ZTE RNCs under HNRZ01
                zte_data['Date_Standard'] = zte_data[df_zte._date_col]

                # Aggregate ZTE data by date (average of both ZTE RNCs)
                numeric_cols = zte_data.select_dtypes(include=[np.number]).columns
                agg_dict = {col: 'mean' for col in numeric_cols if col not in ['Date_Standard']}
                agg_dict['RNC_Standard'] = 'first'

                zte_aggregated = zte_data.groupby('Date_Standard').agg(agg_dict).reset_index()
                combined_data.append(zte_aggregated)
            elif not df_zte.empty:
                print("Warning: ZTE data missing metadata attributes")

            if combined_data:
                result = pd.concat(combined_data, ignore_index=True, sort=False)
                result._date_col = 'Date_Standard'
                result._rnc_col = 'RNC_Standard'
                result._rncs = self.all_rncs
                return result
            else:
                # Return empty DataFrame with proper attributes
                empty_df = pd.DataFrame()
                empty_df._date_col = 'Date_Standard'
                empty_df._rnc_col = 'RNC_Standard'
                empty_df._rncs = self.all_rncs
                return empty_df

        except Exception as e:
            print(f"Error combining vendor data: {e}")
            import traceback
            traceback.print_exc()
            # Return empty DataFrame with proper attributes
            empty_df = pd.DataFrame()
            empty_df._date_col = 'Date_Standard'
            empty_df._rnc_col = 'RNC_Standard'
            empty_df._rncs = self.all_rncs
            return empty_df

    def _create_unified_rnc_dashboard_table(self, df_24h, df_bh, output_dir):
        """Create unified RNC dashboard table"""
        try:
            print("Creating unified RNC dashboard table...")

            if df_24h.empty or df_bh.empty:
                print("Warning: No data available for dashboard")
                return

            # Create figure
            fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(20, 16))
            fig.suptitle('Daily 3G KPI Dashboard By RNC (BH & 24h)',
                         fontsize=18, fontweight='bold', y=0.95)

            # Get parameters
            date_col = df_24h._date_col
            rnc_col = df_24h._rnc_col
            rncs = self.all_rncs
            kpi_mapping = self._get_unified_kpi_mapping()

            latest_dates = self._get_latest_dates(df_24h, date_col, 3)

            if len(latest_dates) == 0:
                print("Warning: No date data available")
                return

            # Create dashboards
            self._draw_unified_rnc_dashboard(ax1, df_bh, latest_dates, date_col, rnc_col, rncs, kpi_mapping,
                                             "Daily 3G KPI Dashboard By RNC (BH)", "#FFA500")

            self._draw_unified_rnc_dashboard(ax2, df_24h, latest_dates, date_col, rnc_col, rncs, kpi_mapping,
                                             "Daily 3G KPI Dashboard By RNC (24h)", "#FF6B35")

            plt.tight_layout()
            plt.subplots_adjust(top=0.92)

            # Save
            dashboard_path = os.path.join(output_dir, "Daily_3G_KPI_Dashboard_By_RNC.png")
            plt.savefig(dashboard_path, dpi=300, bbox_inches='tight', facecolor='white')
            plt.close()

            print(f"Created RNC Dashboard: {dashboard_path}")

        except Exception as e:
            print(f"Error creating unified dashboard table: {e}")
            import traceback
            traceback.print_exc()

    def _draw_unified_rnc_dashboard(self, ax, df, latest_dates, date_col, rnc_col, rncs, kpi_mapping, title,
                                    header_color):
        """Draw unified RNC dashboard"""
        ax.clear()
        ax.set_xlim(0, 15)
        ax.set_ylim(0, 20)
        ax.axis('off')

        # Title
        ax.text(7.5, 19, title, ha='center', va='center', fontsize=14, fontweight='bold')

        # KPI groups
        kpi_groups = [
            {
                'kpis': ['CS CSSR', 'HSDPA CSSR', 'CS CDR', 'HSDPA CDR'],
                'targets': [99.00, 98.00, 0.80, 1.50],
                'y_start': 17.5
            },
            {
                'kpis': ['CS Soft HOSR', 'HSDPA Soft HOSR', 'CS IRAT HOSR', 'PS IRAT HOSR'],
                'targets': [99.00, 98.00, 97.00, 92.00],
                'y_start': 11.5
            },
            {
                'kpis': ['CS Traffic (Erl)', 'PS Traffic (GB)'],
                'targets': [None, None],
                'y_start': 5.5
            }
        ]

        for group in kpi_groups:
            self._draw_unified_kpi_group_table(ax, df, latest_dates, date_col, rnc_col, rncs,
                                               kpi_mapping, group['kpis'], group['targets'],
                                               header_color, group['y_start'])

    def _draw_unified_kpi_group_table(self, ax, df, latest_dates, date_col, rnc_col, rncs, kpi_mapping, kpis, targets,
                                      header_color, y_start):
        """Draw unified KPI group table"""
        # Table parameters
        col_width = 2.0
        row_height = 0.4
        x_start = 0.5

        # Headers
        headers = ['Item'] + rncs + ['KPI']

        # Draw headers
        for i, header in enumerate(headers):
            x = x_start + i * col_width
            rect = Rectangle((x, y_start), col_width, row_height,
                             facecolor=header_color, edgecolor='black', linewidth=1)
            ax.add_patch(rect)

            font_size = 9 if len(header) <= 8 else 8
            ax.text(x + col_width / 2, y_start + row_height / 2, header,
                    ha='center', va='center', fontsize=font_size,
                    fontweight='bold', color='white')

        # Draw data for each KPI
        current_y = y_start
        for kpi_idx, (kpi_name, target) in enumerate(zip(kpis, targets)):
            current_y -= row_height
            self._draw_unified_single_kpi_rows(ax, df, latest_dates, date_col, rnc_col, rncs,
                                               kpi_mapping, kpi_name, target, headers,
                                               x_start, current_y, col_width, row_height)
            # Add space between KPIs
            current_y -= row_height * (len(latest_dates) + 2)

    def _draw_unified_single_kpi_rows(self, ax, df, latest_dates, date_col, rnc_col, rncs, kpi_mapping, kpi_name,
                                      target, headers, x_start, y_start, col_width, row_height):
        """Draw single KPI rows for unified dashboard"""
        rows_data = []

        # Target row
        target_row = ['Target (%)'] + ([f"{target:.2f}" if target else '-'] * len(rncs)) + [kpi_name]
        rows_data.append(target_row)

        # Date rows
        for date in latest_dates:
            date_str = date.strftime('%d-%b-%y')
            row = [date_str]

            for rnc in rncs:
                value = self._get_unified_rnc_kpi_value(df, date, date_col, rnc_col, rnc, kpi_name, kpi_mapping)
                if value is not None:
                    if kpi_name in ['CS Traffic (Erl)', 'PS Traffic (GB)']:
                        row.append(f"{value:,.0f}")
                    else:
                        row.append(f"{value:.2f}")
                else:
                    row.append('-')
            row.append('')
            rows_data.append(row)

        # Comparison rows
        if len(rows_data) >= 3:
            comp_row = ['Delta (D-1)']
            for rnc_idx in range(len(rncs)):
                try:
                    curr_str = rows_data[1][rnc_idx + 1]
                    prev_str = rows_data[2][rnc_idx + 1]

                    if curr_str != '-' and prev_str != '-':
                        curr_val = float(curr_str.replace(',', ''))
                        prev_val = float(prev_str.replace(',', ''))

                        if kpi_name in ['CS CDR', 'HSDPA CDR']:
                            diff = prev_val - curr_val
                        else:
                            diff = curr_val - prev_val

                        symbol = '▲' if diff > 0.5 else ('▼' if diff < -0.5 else '▶')
                        comp_row.append(f"{diff:+.1f}% {symbol}")
                    else:
                        comp_row.append('-')
                except:
                    comp_row.append('-')

            comp_row.append('')
            rows_data.append(comp_row)

        # Draw all rows
        for row_idx, row in enumerate(rows_data):
            y = y_start - row_idx * row_height

            for col_idx, cell_value in enumerate(row):
                x = x_start + col_idx * col_width

                bg_color, text_color, font_weight = self._get_cell_style(
                    row_idx, col_idx, cell_value, row, target, kpi_name, len(rncs))

                rect = Rectangle((x, y), col_width, row_height,
                                 facecolor=bg_color, edgecolor='black', linewidth=1)
                ax.add_patch(rect)

                font_size = 7 if len(str(cell_value)) > 12 else 8
                ax.text(x + col_width / 2, y + row_height / 2, str(cell_value),
                        ha='center', va='center', fontsize=font_size,
                        color=text_color, weight=font_weight)
    def _create_unified_trend_charts(self, df_24h, df_bh, output_dir):
        """Create unified trend charts for all RNCs"""
        try:
            print("Creating unified trend charts...")

            # Define chart groups based on requirements
            bh_kpi_groups = {
                'Traffic': {
                    'kpis': ['CS Traffic (Erl)', 'PS Traffic (GB)'],
                    'filename': 'Chart_Busy_Hour_Traffic.png',
                    'title': 'Chart Busy Hour - Traffic'
                },
                'Success_Rates': {
                    'kpis': ['CS CSSR', 'HSDPA CSSR'],
                    'filename': 'Chart_Busy_Hour_Success_Rates.png',
                    'title': 'Chart Busy Hour - Success Rates'
                },
                'Drop_Rates': {
                    'kpis': ['CS CDR', 'HSDPA CDR'],
                    'filename': 'Chart_Busy_Hour_Drop_Rates.png',
                    'title': 'Chart Busy Hour - Drop Rates'
                },
                'Handover_Soft': {
                    'kpis': ['CS Soft HOSR', 'PS Soft HOSR'],
                    'filename': 'Chart_Busy_Hour_Soft_Handover.png',
                    'title': 'Chart Busy Hour - Soft Handover'
                },
                'Handover_Hard': {
                    'kpis': ['CS Hard HOSR', 'PS Hard HOSR'],
                    'filename': 'Chart_Busy_Hour_Hard_Handover.png',
                    'title': 'Chart Busy Hour - Hard Handover'
                },
                'Handover_IRAT': {
                    'kpis': ['CS IRAT HOSR', 'PS IRAT HOSR'],
                    'filename': 'Chart_Busy_Hour_IRAT_Handover.png',
                    'title': 'Chart Busy Hour - IRAT Handover'
                },
                'HSDPA_Performance': {
                    'kpis': ['HSDPA User', 'HSDPA Throughput (Kbps)'],
                    'filename': 'Chart_Busy_Hour_HSDPA_Performance.png',
                    'title': 'Chart Busy Hour - HSDPA Performance'
                }
            }

            h24_kpi_groups = {
                'Traffic': {
                    'kpis': ['CS Traffic (Erl)', 'PS Traffic (GB)'],
                    'filename': 'Chart_24h_Traffic.png',
                    'title': 'Chart 24h - Traffic'
                },
                'Success_Rates': {
                    'kpis': ['CS CSSR', 'HSDPA CSSR'],
                    'filename': 'Chart_24h_Success_Rates.png',
                    'title': 'Chart 24h - Success Rates'
                },
                'Drop_Rates': {
                    'kpis': ['CS CDR', 'HSDPA CDR'],
                    'filename': 'Chart_24h_Drop_Rates.png',
                    'title': 'Chart 24h - Drop Rates'
                },
                'Handover_Soft': {
                    'kpis': ['CS Soft HOSR', 'PS Soft HOSR'],
                    'filename': 'Chart_24h_Soft_Handover.png',
                    'title': 'Chart 24h - Soft Handover'
                },
                'Handover_Hard': {
                    'kpis': ['CS Hard HOSR', 'PS Hard HOSR'],
                    'filename': 'Chart_24h_Hard_Handover.png',
                    'title': 'Chart 24h - Hard Handover'
                },
                'HSDPA_Performance': {
                    'kpis': ['HSDPA User', 'HSDPA Throughput (Kbps)'],
                    'filename': 'Chart_24h_HSDPA_Performance.png',
                    'title': 'Chart 24h - HSDPA Performance'
                },
                'Cell_Availability': {
                    'kpis': ['Cell Availability'],
                    'filename': 'Chart_24h_Cell_Availability.png',
                    'title': 'Chart 24h - Cell Availability'
                }
            }

            # Create charts
            for group_name, group_info in bh_kpi_groups.items():
                self._create_unified_kpi_chart(df_bh, group_info['kpis'],
                                             group_info['title'], group_info['filename'], output_dir)

            for group_name, group_info in h24_kpi_groups.items():
                self._create_unified_kpi_chart(df_24h, group_info['kpis'],
                                             group_info['title'], group_info['filename'], output_dir)

        except Exception as e:
            print(f"Error creating unified trend charts: {e}")
            import traceback
            traceback.print_exc()

    def _create_unified_kpi_chart(self, df, kpis, chart_title, filename, output_dir):
        """Create chart for unified KPI group"""
        try:
            if df.empty:
                print(f"Warning: No data for {filename}")
                return

            date_col = df._date_col
            rnc_col = df._rnc_col
            rncs = self.all_rncs
            kpi_mapping = self._get_unified_kpi_mapping()

            # Filter available KPIs
            available_kpis = [kpi for kpi in kpis if self._check_kpi_available(df, kpi, kpi_mapping)]

            if not available_kpis:
                print(f"Warning: No KPIs available for {filename}")
                return

            # Create subplot layout
            n_kpis = len(available_kpis)
            if n_kpis == 1:
                fig, axes = plt.subplots(1, 1, figsize=(12, 8))
                axes = [axes]
            elif n_kpis == 2:
                fig, axes = plt.subplots(1, 2, figsize=(16, 8))
            elif n_kpis <= 4:
                fig, axes = plt.subplots(2, 2, figsize=(16, 12))
                axes = axes.flatten()
            else:
                fig, axes = plt.subplots(3, 2, figsize=(16, 15))
                axes = axes.flatten()

            fig.suptitle(chart_title, fontsize=16, fontweight='bold', y=0.95)

            # Plot each KPI
            for i, kpi in enumerate(available_kpis):
                if i < len(axes):
                    success = self._plot_unified_kpi(axes[i], df, date_col, rnc_col, rncs, kpi_mapping, kpi)
                    if not success:
                        print(f"Error plotting KPI {kpi}")

            # Hide unused subplots
            for i in range(len(available_kpis), len(axes)):
                axes[i].set_visible(False)

            plt.tight_layout()
            plt.subplots_adjust(top=0.92)

            # Save chart
            chart_path = os.path.join(output_dir, filename)
            plt.savefig(chart_path, dpi=300, bbox_inches='tight', facecolor='white')
            plt.close()

            print(f"Created chart: {filename}")

        except Exception as e:
            print(f"Error creating chart {filename}: {e}")
            import traceback
            traceback.print_exc()

    def _plot_unified_kpi(self, ax, df, date_col, rnc_col, rncs, kpi_mapping, kpi_name):
        """Plot unified KPI with all RNCs including HNRZ01"""
        try:
            # Prepare data
            plot_data = {}
            dates = sorted(df[date_col].dropna().dt.date.unique())

            if len(dates) == 0:
                print(f"Warning: No date data for KPI {kpi_name}")
                return False

            # Define colors for each RNC
            colors = {
                'HLRE01': 'blue',
                'HLRE02': 'orange',
                'HLRE03': 'gray',
                'HLRE04': 'gold',
                'HNRZ01': 'lightblue'
            }

            for rnc in rncs:
                rnc_data = df[df[rnc_col] == rnc]
                if rnc_data.empty:
                    continue

                values = []
                valid_dates = []

                for date in dates:
                    day_data = rnc_data[rnc_data[date_col].dt.date == date]
                    if not day_data.empty:
                        value = self._get_unified_rnc_kpi_value_from_data(day_data, kpi_name, kpi_mapping)
                        if value is not None and not np.isnan(value):
                            values.append(float(value))
                            valid_dates.append(pd.Timestamp(date))

                if len(values) > 0 and len(valid_dates) > 0:
                    plot_data[rnc] = {'dates': valid_dates, 'values': values}

            if not plot_data:
                ax.text(0.5, 0.5, f'No data available for {kpi_name}',
                        ha='center', va='center', transform=ax.transAxes)
                ax.set_title(f"{kpi_name}", fontsize=12, fontweight='bold')
                return False

            # Plot lines for each RNC
            for rnc, data in plot_data.items():
                if len(data['values']) > 0:
                    color = colors.get(rnc, 'black')
                    ax.plot(data['dates'], data['values'], marker='o', label=rnc,
                            color=color, linewidth=2, markersize=4)

            # Add target line
            targets = {
                'CS CSSR': 99.0, 'HSDPA CSSR': 98.0, 'CS CDR': 0.8, 'HSDPA CDR': 1.5,
                'CS Soft HOSR': 99.0, 'PS Soft HOSR': 98.0, 'CS IRAT HOSR': 97.0, 'PS IRAT HOSR': 92.0
            }

            if kpi_name in targets:
                ax.axhline(y=targets[kpi_name], color='red', linestyle='--', linewidth=2, label='Target')

            # Format chart
            ax.set_title(f"{kpi_name}", fontsize=12, fontweight='bold')
            ax.set_xlabel('Date', fontsize=10)

            # Set y-label based on KPI
            if 'Traffic' in kpi_name:
                if 'Erl' in kpi_name:
                    ax.set_ylabel('Erlang', fontsize=10)
                elif 'GB' in kpi_name:
                    ax.set_ylabel('GB', fontsize=10)
            elif 'Throughput' in kpi_name:
                ax.set_ylabel('Kbps', fontsize=10)
            elif 'User' in kpi_name:
                ax.set_ylabel('Users', fontsize=10)
            elif 'Availability' in kpi_name:
                ax.set_ylabel('Percentage', fontsize=10)
            else:
                ax.set_ylabel('Percentage', fontsize=10)

            ax.legend(bbox_to_anchor=(1.05, 1), loc='upper left', fontsize=9)
            ax.grid(True, alpha=0.3)

            # Improved x-axis formatting
            if len(dates) > 30:
                ax.xaxis.set_major_locator(mdates.WeekdayLocator(interval=1))
                ax.xaxis.set_major_formatter(mdates.DateFormatter('%d-%b'))
            elif len(dates) > 14:
                ax.xaxis.set_major_locator(mdates.DayLocator(interval=2))
                ax.xaxis.set_major_formatter(mdates.DateFormatter('%d-%b'))
            else:
                ax.xaxis.set_major_locator(mdates.DayLocator(interval=1))
                ax.xaxis.set_major_formatter(mdates.DateFormatter('%d-%b'))

            plt.setp(ax.xaxis.get_majorticklabels(), rotation=45)

            return True

        except Exception as e:
            print(f"Error plotting KPI {kpi_name}: {e}")
            ax.text(0.5, 0.5, f'Error plotting {kpi_name}',
                    ha='center', va='center', transform=ax.transAxes)
            ax.set_title(f"{kpi_name}", fontsize=12, fontweight='bold')
            return False
    # ========== HELPER METHODS ==========
    def _get_latest_dates(self, df, date_col, n_dates=3):
        """Get latest N dates from dataframe"""
        try:
            unique_dates = sorted(df[date_col].dropna().dt.date.unique(), reverse=True)
            return [pd.Timestamp(date) for date in unique_dates[:n_dates]]
        except Exception as e:
            print(f"Error getting latest dates: {e}")
            return []

    def _get_latest_dates_for_aggregated_data(self, df, date_col, n_dates=3):
        """Get latest N dates for aggregated data"""
        try:
            latest = df[date_col].max()
            prev = df[df[date_col] < latest][date_col].max() if pd.notna(latest) else pd.NaT
            week_candidate = latest - timedelta(days=7) if pd.notna(latest) else pd.NaT
            week_date = df[df[date_col] <= week_candidate][date_col].max() if pd.notna(week_candidate) else pd.NaT

            latest_dates = []
            if pd.notna(latest):
                latest_dates.append(latest)
            if pd.notna(prev):
                latest_dates.append(prev)
            if pd.notna(week_date) and (week_date not in latest_dates):
                latest_dates.append(week_date)

            return latest_dates[:n_dates]
        except Exception as e:
            print(f"Error getting latest dates for aggregated data: {e}")
            return []

    def _get_unified_rnc_kpi_value(self, df, date, date_col, rnc_col, rnc, kpi_name, kpi_mapping):
        """Get KPI value for specific RNC and date (unified version)"""
        try:
            rnc_day_data = df[(df[rnc_col] == rnc) & (df[date_col].dt.date == date.date())]
            if rnc_day_data.empty:
                return None
            return self._get_unified_rnc_kpi_value_from_data(rnc_day_data, kpi_name, kpi_mapping)
        except Exception as e:
            return None

    def _get_unified_rnc_kpi_value_from_data(self, day_data, kpi_name, kpi_mapping):
        """Get KPI value from filtered data (unified version)"""
        try:
            possible_cols = kpi_mapping.get(kpi_name, [kpi_name])

            for col_name in possible_cols:
                if col_name in day_data.columns:
                    values = day_data[col_name].dropna()
                    if len(values) > 0:
                        val = values.mean()
                        if pd.notna(val):
                            return float(val)

                for actual_col in day_data.columns:
                    col_clean = col_name.lower().replace(' ', '').replace('(', '').replace(')', '').replace('%', '')
                    actual_clean = actual_col.lower().replace(' ', '').replace('(', '').replace(')', '').replace('%',
                                                                                                                 '')

                    if col_clean in actual_clean or actual_clean in col_clean:
                        values = day_data[actual_col].dropna()
                        if len(values) > 0:
                            val = values.mean()
                            if pd.notna(val):
                                return float(val)

            return None
        except Exception as e:
            return None

    def _check_kpi_available(self, df, kpi_name, kpi_mapping):
        """Check if KPI is available in dataframe"""
        try:
            possible_cols = kpi_mapping.get(kpi_name, [kpi_name])

            for col_name in possible_cols:
                if col_name in df.columns:
                    return True

                for actual_col in df.columns:
                    col_clean = col_name.lower().replace(' ', '').replace('(', '').replace(')', '').replace('%', '')
                    actual_clean = actual_col.lower().replace(' ', '').replace('(', '').replace(')', '').replace('%',
                                                                                                                 '')

                    if col_clean in actual_clean or actual_clean in col_clean:
                        return True

            return False
        except:
            return False

    def _get_cell_style(self, row_idx, col_idx, cell_value, row, target, kpi_name, num_rncs):
        """Determine cell style"""
        bg_color = 'white'
        text_color = 'black'
        font_weight = 'normal'

        # Target row
        if row_idx == 0:
            bg_color = '#FFFACD'
            font_weight = 'bold'
        # Comparison row
        elif 'Delta' in str(row[0]):
            bg_color = '#E6E6FA'
            if '▲' in str(cell_value):
                text_color = 'green'
                font_weight = 'bold'
            elif '▼' in str(cell_value):
                text_color = 'red'
                font_weight = 'bold'
        # Data rows with target checking
        else:
            if col_idx > 0 and col_idx <= num_rncs and target is not None:
                try:
                    val = float(str(cell_value).replace(',', ''))
                    violates_target = False
                    if kpi_name in ['CS CDR', 'HSDPA CDR'] and val > target:
                        violates_target = True
                    elif kpi_name not in ['CS CDR', 'HSDPA CDR'] and val < target:
                        violates_target = True

                    if violates_target:
                        bg_color = '#FFB3B3'
                        text_color = '#B22222'
                        font_weight = 'bold'
                except:
                    pass

        return bg_color, text_color, font_weight

    def _draw_formatted_table(self, ax, headers, data, targets, header_color, x_start, y_start, col_width, row_height):
        """Draw formatted table"""
        # Draw headers
        for i, header in enumerate(headers):
            x = x_start + i * col_width
            rect = plt.Rectangle((x, y_start), col_width, row_height,
                                 facecolor=header_color, edgecolor='black', linewidth=1)
            ax.add_patch(rect)

            font_size = 8 if len(header) > 12 else 9
            ax.text(x + col_width / 2, y_start + row_height / 2, header,
                    ha='center', va='center', fontsize=font_size,
                    fontweight='bold', color='white')

        # Draw data rows
        for row_idx, row_data in enumerate(data):
            y = y_start - (row_idx + 1) * row_height

            for col_idx, value in enumerate(row_data):
                x = x_start + col_idx * col_width

                bg_color, text_color, font_weight, display_value = self._format_cell_for_individual_dashboard(
                    row_idx, col_idx, value, row_data, headers, targets)

                rect = plt.Rectangle((x, y), col_width, row_height,
                                     facecolor=bg_color, edgecolor='black', linewidth=1)
                ax.add_patch(rect)

                font_size = 7 if len(str(display_value)) > 10 else 8
                ax.text(x + col_width / 2, y + row_height / 2, display_value,
                        ha='center', va='center', fontsize=font_size,
                        color=text_color, weight=font_weight)

    def _format_cell_for_individual_dashboard(self, row_idx, col_idx, value, row_data, headers, targets):
        """Format cell for individual dashboard"""
        bg_color = 'white'
        text_color = 'black'
        font_weight = 'normal'
        display_value = str(value)

        # Target row
        if row_idx == 0:
            bg_color = '#FFFACD'
            font_weight = 'bold'
            return bg_color, text_color, font_weight, display_value

        # Compare rows
        if 'Compare' in str(row_data[0]):
            bg_color = '#E6E6FA'

            if col_idx > 0 and value != '-':
                value_str = str(value)
                try:
                    clean_val = value_str.replace('%', '').replace('+', '').strip()
                    if clean_val and clean_val != '-':
                        val_float = float(clean_val)

                        if value_str.startswith('+'):
                            if abs(val_float) <= 1:
                                display_value = f"{value} ▶"
                                text_color = 'black'
                            else:
                                display_value = f"{value} ▲"
                                text_color = 'green'
                            font_weight = 'bold'
                        elif value_str.startswith('-'):
                            if abs(val_float) <= 1:
                                display_value = f"{value} ▶"
                                text_color = 'black'
                            else:
                                display_value = f"{value} ▼"
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

                    should_highlight = False
                    if header_name in ['CS CDR', 'HSDPA CDR'] and actual_val > target:
                        should_highlight = True
                    elif header_name not in ['CS CDR', 'HSDPA CDR'] and actual_val < target:
                        should_highlight = True

                    if should_highlight:
                        bg_color = '#FFB3B3'
                        text_color = '#B22222'
                        font_weight = 'bold'
                except:
                    pass

        return bg_color, text_color, font_weight, display_value
    def _get_unified_kpi_mapping(self):
        """Get unified KPI mapping for both vendors"""
        return {
            'CS CSSR': ['CS CSSR (%)', 'CS CSSR', 'CS Call Setup Success Rate', 'CS Call Setup Success Rate (%)',
                       'CSCSSR', 'CS CSSR_VNM'],
            'HSDPA CSSR': ['HSDPA CSSR (%)', 'HSDPA CSSR', 'HSDPA Call Setup Success Rate',
                          'HSDPA Call Setup Success Rate (%)', 'PS CSSR_HSDPA', 'PS CSSR', 'HSDPACSSR'],
            'CS CDR': ['CS CDR (%)', 'CS CDR', 'CS Call Drop Rate', 'CS Call Drop Rate (%)',
                      'CSCDR', 'CS CDR_VNM'],
            'HSDPA CDR': ['HSDPA CDR (%)', 'HSDPA CDR', 'HSDPA Call Drop Rate', 'HSDPA Call Drop Rate (%)',
                         'PS CDR_HSPDA', 'PS CDR_HSDPA', 'HSDPACDR', 'PS CDR_HSDPA_VNM'],
            'CS Traffic (Erl)': ['CS Traffic (Erl)', 'CS Traffic (Erlang)', 'CS Traffic', 'CSTraffic',
                                 'CS Traffic (Erl)_VNM'],
            'CS Soft HOSR': ['CS Soft HOSR (%)', 'CS Soft HOSR', 'CS Soft Handover Success Rate',
                            'CS Soft Handover Success Rate (%)', 'CSSoftHOSR', 'CS Soft HOSR_VNM'],
            'HSDPA Soft HOSR': ['HSDPA Soft HOSR (%)', 'HSDPA Soft HOSR', 'HSDPA Soft Handover Success Rate',
                               'HSDPA Soft Handover Success Rate (%)', 'PS Soft HOSR', 'HSDPASoftHOSR',
                               'PS Soft HOSR_VNM'],
            'PS Soft HOSR': ['PS Soft HOSR (%)', 'PS Soft HOSR', 'PS Soft Handover Success Rate',
                            'PS Soft Handover Success Rate (%)', 'HSDPA Soft HOSR', 'PSSoftHOSR',
                            'PS Soft HOSR_VNM'],
            'CS Hard HOSR': ['CS Hard HOSR (%)', 'CS Hard HOSR', 'CS Hard Handover Success Rate',
                            'CS Hard Handover Success Rate (%)', 'CSHardHOSR', 'CS Hard HOSR_VNM'],
            'PS Hard HOSR': ['PS Hard HOSR (%)', 'PS Hard HOSR', 'PS Hard Handover Success Rate',
                            'PS Hard Handover Success Rate (%)', 'PSHardHOSR', 'PS Hard HOSR_VNM'],
            'CS IRAT HOSR': ['CS IRAT HOSR (%)', 'CS IRAT HOSR', 'CS Inter-RAT Handover Success Rate',
                            'CS Inter-RAT Handover Success Rate (%)', 'CS InterRAT HOSR', 'CSIRAHOSR',
                            'CS InterRAT HOSR_VNM'],
            'PS IRAT HOSR': ['PS IRAT HOSR (%)', 'PS IRAT HOSR', 'PS Inter-RAT Handover Success Rate',
                            'PS Inter-RAT Handover Success Rate (%)', 'PS InterRAT HOSR', 'PSIRAHOSR',
                            'PS InterRAT HOSR_VNM'],
            'PS Traffic (GB)': ['PS Traffic (GB)', 'PS Traffic (Gigabytes)', 'PS Traffic', 'PSTraffic'],
            'HSDPA User': ['HSDPA User', 'HSDPA Users', 'HSDPA Active Users', 'HSDPAUser'],
            'HSDPA Throughput (Kbps)': ['HSDPA Throughput (kbps)', 'HSDPA Throughput', 'HSDPA Average Throughput',
                                       'HSDPAThroughput', 'HSDPA Throughput (Kbps)'],
            'Cell Availability': ['Cell Availability (%)', 'Cell Availability', 'Availability', 'CellAvailability']
        }

    def _get_ericsson_kpi_mapping(self):
        """KPI mapping for Ericsson"""
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
            'CS IRAT HOSR': ['CS IRAT HOSR (%)', 'CS IRAT HOSR', 'CS Inter-RAT Handover Success Rate',
                             'CS Inter-RAT Handover Success Rate (%)'],
            'PS IRAT HOSR': ['PS IRAT HOSR (%)', 'PS IRAT HOSR', 'PS Inter-RAT Handover Success Rate',
                             'PS Inter-RAT Handover Success Rate (%)'],
            'PS Traffic (GB)': ['PS Traffic (GB)', 'PS Traffic (Gigabytes)', 'PS Traffic']
        }

    def _get_zte_kpi_mapping(self):
        """KPI mapping for ZTE"""
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
            'CS IRAT HOSR': ['CS InterRAT HOSR_VNM', 'CS IRAT HOSR', 'CS Inter-RAT Handover Success Rate',
                             'CS Inter-RAT Handover Success Rate (%)'],
            'PS IRAT HOSR': ['PS InterRAT HOSR_VNM', 'PS IRAT HOSR', 'PS Inter-RAT Handover Success Rate',
                             'PS Inter-RAT Handover Success Rate (%)'],
            'PS Traffic (GB)': ['PS Traffic (GB)', 'PS Traffic (Gigabytes)', 'PS Traffic']
        }
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
            print(f"Warning: File not found: {excel_file_zte}")

    for excel_file_ericsson, csv_file_ericsson in excel_files_ericsson.items():
        if os.path.exists(excel_file_ericsson):
            df = processor.clean_excel_to_csv_ericsson(excel_file_ericsson, csv_file_ericsson)
            if df is not None:
                converted_files_ericsson[excel_file_ericsson] = csv_file_ericsson
                processor.verify_csv_structure(csv_file_ericsson)
        else:
            print(f"Warning: File not found: {excel_file_ericsson}")

    # Create individual vendor dashboards (aggregated data)
    if len(converted_files_ericsson) >= 2:
        csv_files_ericsson = list(converted_files_ericsson.values())
        csv_all_day_ericsson = csv_files_ericsson[1]
        csv_busy_hour_ericsson = csv_files_ericsson[0]
        output_dir_ericsson = "output_ericsson"
        os.makedirs(output_dir_ericsson, exist_ok=True)
        processor.create_daily_dashboard_table_ericsson(csv_all_day_ericsson, csv_busy_hour_ericsson,
                                                        output_dir_ericsson)

    if len(converted_files_zte) >= 2:
        csv_files_zte = list(converted_files_zte.values())
        csv_all_day_zte = csv_files_zte[1]
        csv_busy_hour_zte = csv_files_zte[0]
        output_dir_zte = "output_zte"
        os.makedirs(output_dir_zte, exist_ok=True)
        processor.create_daily_dashboard_table_ZTE(csv_all_day_zte, csv_busy_hour_zte, output_dir_zte)

    # Create improved RNC dashboards and charts
    if len(converted_files_ericsson) >= 2 and len(converted_files_zte) >= 2:
        print("\nStarting Daily 3G KPI Dashboard By RNC (Improved Version)...")

        # Create output directory for RNC dashboards
        rnc_output_dir = "output_rnc_dashboards_improved"
        os.makedirs(rnc_output_dir, exist_ok=True)

        # Get file paths
        csv_files_ericsson = list(converted_files_ericsson.values())
        csv_files_zte = list(converted_files_zte.values())

        # Identify WD (24h) and BH files
        csv_all_day_ericsson = None
        csv_bh_ericsson = None
        csv_all_day_zte = None
        csv_bh_zte = None

        for f in csv_files_ericsson:
            if 'WD' in f:
                csv_all_day_ericsson = f
            elif 'BH' in f:
                csv_bh_ericsson = f

        for f in csv_files_zte:
            if 'WD' in f:
                csv_all_day_zte = f
            elif 'BH' in f:
                csv_bh_zte = f

        # Fallback if naming convention is different
        if not csv_all_day_ericsson:
            csv_all_day_ericsson = csv_files_ericsson[0]
        if not csv_bh_ericsson:
            csv_bh_ericsson = csv_files_ericsson[1] if len(csv_files_ericsson) > 1 else csv_files_ericsson[0]

        if not csv_all_day_zte:
            csv_all_day_zte = csv_files_zte[0]
        if not csv_bh_zte:
            csv_bh_zte = csv_files_zte[1] if len(csv_files_zte) > 1 else csv_files_zte[0]

        # Create improved RNC dashboards and trend charts
        processor.create_daily_rnc_dashboard(
            csv_all_day_ericsson=csv_all_day_ericsson,
            csv_bh_ericsson=csv_bh_ericsson,
            csv_all_day_zte=csv_all_day_zte,
            csv_bh_zte=csv_bh_zte,
            output_dir=rnc_output_dir
        )

        print("Completed all dashboards and charts!")
    else:
        print("Warning: Not enough files to create RNC Dashboard. Need at least 2 Ericsson and 2 ZTE files.")


if __name__ == "__main__":
    main()
