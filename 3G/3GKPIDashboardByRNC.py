import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.patches as patches
from matplotlib.colors import LinearSegmentedColormap
import seaborn as sns
from datetime import datetime, timedelta
import os
import glob
import warnings
import re

warnings.filterwarnings('ignore')
plt.rcParams['font.family'] = ['DejaVu Sans', 'Arial', 'sans-serif']
plt.rcParams['axes.unicode_minus'] = False
plt.style.use('default')
sns.set_palette("husl")


class Enhanced3GDashboard:
    def __init__(self, csv_folder_path=None, csv_files_list=None):
        """
        Initialize dashboard with multiple CSV files
        Args:
            csv_folder_path: Path to folder containing CSV files
            csv_files_list: List of CSV file paths
        """
        self.df_combined = pd.DataFrame()
        self.ericsson_data = pd.DataFrame()
        self.zte_data = pd.DataFrame()
        self.data_date = None  # Store the data date for naming output files

        if csv_folder_path:
            self.load_csv_from_folder(csv_folder_path)
        elif csv_files_list:
            self.load_csv_from_list(csv_files_list)

        self.prepare_data()

    def find_excel_files_by_pattern(self, directory="."):
        """
        Find Excel files based on patterns
        Returns dict with file types and their paths
        """
        file_patterns = {
            'ericsson_bh': r'3G_RNO_KPIs_BH_scheduled.*\.xlsx$',
            'ericsson_wd': r'3G_RNO_KPIs_WD_scheduled.*\.xlsx$',
            'zte_bh': r'3G_RNO_KPIs_BH.*ZTE.*\.xlsx$',
            'zte_wd': r'3G_RNO_KPIs_WD.*ZTE.*\.xlsx$'
        }

        found_files = {}
        all_files = os.listdir(directory)

        for file_type, pattern in file_patterns.items():
            matching_files = [f for f in all_files if re.match(pattern, f, re.IGNORECASE)]
            if matching_files:
                # Take the most recent file if multiple matches
                matching_files.sort(reverse=True)
                found_files[file_type] = os.path.join(directory, matching_files[0])
                print(f"Found {file_type}: {matching_files[0]}")
            else:
                print(f"No file found for pattern {file_type}: {pattern}")

        return found_files

    def extract_date_from_filename(self, filename):
        """Extract date from filename"""
        # Look for date pattern YYYY-MM-DD in filename
        date_match = re.search(r'(\d{4}-\d{2}-\d{2})', filename)
        if date_match:
            try:
                return datetime.strptime(date_match.group(1), '%Y-%m-%d').date()
            except:
                pass

        # Look for date pattern YYYYMMDD in filename
        date_match = re.search(r'(\d{8})', filename)
        if date_match:
            try:
                return datetime.strptime(date_match.group(1), '%Y%m%d').date()
            except:
                pass

        return datetime.now().date()

    def load_csv_from_folder(self, folder_path):
        """Load all CSV files from a folder"""
        csv_files = glob.glob(os.path.join(folder_path, "*.csv"))
        self.load_csv_from_list(csv_files)

    def load_csv_from_list(self, csv_files):
        """Load multiple CSV files"""
        all_data = []

        for file_path in csv_files:
            try:
                print(f"Loading: {os.path.basename(file_path)}")
                df = pd.read_csv(file_path)

                # Add file source information
                df['Source_File'] = os.path.basename(file_path)

                # Extract date from filename for output naming
                if self.data_date is None:
                    self.data_date = self.extract_date_from_filename(file_path)

                # Determine vendor based on filename or columns
                if 'ZTE' in file_path.upper() or any('_VNM' in col for col in df.columns):
                    df['Vendor'] = 'ZTE'
                    if self.zte_data.empty:
                        self.zte_data = df.copy()
                    else:
                        self.zte_data = pd.concat([self.zte_data, df], ignore_index=True)
                else:
                    df['Vendor'] = 'Ericsson'
                    if self.ericsson_data.empty:
                        self.ericsson_data = df.copy()
                    else:
                        self.ericsson_data = pd.concat([self.ericsson_data, df], ignore_index=True)

                all_data.append(df)

            except Exception as e:
                print(f"Error loading {file_path}: {str(e)}")

        if all_data:
            self.df_combined = pd.concat(all_data, ignore_index=True)

    def prepare_data(self):
        """Prepare and clean data for dashboard"""
        if self.df_combined.empty:
            print("No data loaded!")
            return

        # Convert date columns
        for df in [self.df_combined, self.ericsson_data, self.zte_data]:
            if df.empty:
                continue

            if 'Date' in df.columns:
                try:
                    df['Date'] = pd.to_datetime(df['Date'], errors='coerce')
                except:
                    print(f"Warning: Could not convert Date column")
            elif 'Start Time' in df.columns:
                try:
                    df['Date'] = pd.to_datetime(df['Start Time'], errors='coerce')
                except:
                    print(f"Warning: Could not convert Start Time column")

        # Remove rows where Date conversion failed
        self.df_combined = self.df_combined.dropna(subset=['Date'])
        self.ericsson_data = self.ericsson_data.dropna(
            subset=['Date']) if not self.ericsson_data.empty else self.ericsson_data
        self.zte_data = self.zte_data.dropna(subset=['Date']) if not self.zte_data.empty else self.zte_data

        # Get the required 3 dates: latest, second latest, and 7 days before latest
        self.get_target_dates()

    def get_target_dates(self):
        """Get the 3 target dates for dashboard"""
        all_dates = self.df_combined['Date'].dropna().unique()
        # Fix: Convert to Series and then sort
        all_dates = pd.Series(pd.to_datetime(all_dates)).sort_values().values

        if len(all_dates) < 2:
            print("Not enough date data!")
            return

        # Latest date
        self.latest_date = pd.to_datetime(all_dates[-1])

        # Second latest date
        self.second_latest_date = pd.to_datetime(all_dates[-2]) if len(all_dates) > 1 else self.latest_date

        # Date 7 days before latest (find closest available date)
        target_week_ago = self.latest_date - timedelta(days=7)
        week_ago_dates = [d for d in all_dates if pd.to_datetime(d) <= target_week_ago]
        self.week_ago_date = pd.to_datetime(week_ago_dates[-1]) if len(week_ago_dates) > 0 else pd.to_datetime(
            all_dates[0])

        self.target_dates = [self.latest_date, self.second_latest_date, self.week_ago_date]
        print(f"Target dates: {[d.strftime('%Y-%m-%d') for d in self.target_dates]}")

        # Update data_date to latest date if not set
        if self.data_date is None:
            self.data_date = self.latest_date.date()

    def get_kpi_mapping_ericsson(self):
        """Map Ericsson KPI columns"""
        kpi_mapping = {}

        if self.ericsson_data.empty:
            return kpi_mapping

        columns = self.ericsson_data.columns

        # CS CSSR (%)
        cs_cssr_cols = [col for col in columns if 'CS CSSR' in col and not 'MultiRAB' in col]
        if cs_cssr_cols:
            kpi_mapping['CS CSSR (%)'] = cs_cssr_cols[0]

        # HSDPA CSSR (%)
        hsdpa_cssr_cols = [col for col in columns if 'PS CSSR_HSDPA' in col or ('HSDPA' in col and 'CSSR' in col)]
        if hsdpa_cssr_cols:
            kpi_mapping['HSDPA CSSR (%)'] = hsdpa_cssr_cols[0]

        # CS CDR (%)
        cs_cdr_cols = [col for col in columns if 'CS CDR' in col and not 'MultiRAB' in col]
        if cs_cdr_cols:
            kpi_mapping['CS CDR (%)'] = cs_cdr_cols[0]

        # HSDPA CDR (%)
        hsdpa_cdr_cols = [col for col in columns if 'PS CDR_HSPDA' in col or 'PS CDR_HSDPA' in col]
        if hsdpa_cdr_cols:
            kpi_mapping['HSDPA CDR (%)'] = hsdpa_cdr_cols[0]

        # CS Soft HOSR (%)
        cs_soft_hosr_cols = [col for col in columns if 'CS Soft HOSR' in col]
        if cs_soft_hosr_cols:
            kpi_mapping['CS Soft HOSR (%)'] = cs_soft_hosr_cols[0]

        # PS Soft HOSR (%)
        ps_soft_hosr_cols = [col for col in columns if 'PS Soft HOSR' in col]
        if ps_soft_hosr_cols:
            kpi_mapping['PS Soft HOSR (%)'] = ps_soft_hosr_cols[0]

        # CS Traffic (Erl)
        cs_traffic_cols = [col for col in columns if 'CS Traffic' in col]
        if cs_traffic_cols:
            kpi_mapping['CS Traffic (Erl)'] = cs_traffic_cols[0]

        # PS Traffic (GB)
        ps_traffic_cols = [col for col in columns if 'PS Traffic' in col]
        if ps_traffic_cols:
            kpi_mapping['PS Traffic (GB)'] = ps_traffic_cols[0]

        return kpi_mapping

    def get_kpi_mapping_zte(self):
        """Map ZTE KPI columns"""
        kpi_mapping = {}

        if self.zte_data.empty:
            return kpi_mapping

        columns = self.zte_data.columns

        # CS CSSR (%)
        cs_cssr_cols = [col for col in columns if 'CS CSSR_VNM' in col]
        if cs_cssr_cols:
            kpi_mapping['CS CSSR (%)'] = cs_cssr_cols[0]

        # HSDPA CSSR (%)
        hsdpa_cssr_cols = [col for col in columns if 'PS CSSR_VNM' in col]
        if hsdpa_cssr_cols:
            kpi_mapping['HSDPA CSSR (%)'] = hsdpa_cssr_cols[0]

        # CS CDR (%)
        cs_cdr_cols = [col for col in columns if 'CS CDR_VNM' in col]
        if cs_cdr_cols:
            kpi_mapping['CS CDR (%)'] = cs_cdr_cols[0]

        # HSDPA CDR (%)
        hsdpa_cdr_cols = [col for col in columns if 'PS CDR_HSDPA_VNM' in col]
        if hsdpa_cdr_cols:
            kpi_mapping['HSDPA CDR (%)'] = hsdpa_cdr_cols[0]

        # CS Soft HOSR (%)
        cs_soft_hosr_cols = [col for col in columns if 'CS Soft HOSR_VNM' in col]
        if cs_soft_hosr_cols:
            kpi_mapping['CS Soft HOSR (%)'] = cs_soft_hosr_cols[0]

        # PS Soft HOSR (%)
        ps_soft_hosr_cols = [col for col in columns if 'PS Soft HOSR_VNM' in col]
        if ps_soft_hosr_cols:
            kpi_mapping['PS Soft HOSR (%)'] = ps_soft_hosr_cols[0]

        # CS Traffic (Erl)
        cs_traffic_cols = [col for col in columns if 'CS Traffic (Erl)_VNM' in col]
        if cs_traffic_cols:
            kpi_mapping['CS Traffic (Erl)'] = cs_traffic_cols[0]

        # PS Traffic (GB)
        ps_traffic_cols = [col for col in columns if 'PS Traffic (GB)' in col and 'VNM' not in col]
        if ps_traffic_cols:
            kpi_mapping['PS Traffic (GB)'] = ps_traffic_cols[0]

        return kpi_mapping

    def get_rnc_identifiers_ericsson(self):
        """Get Ericsson RNC identifiers - filter out unwanted ones"""
        if self.ericsson_data.empty:
            return []

        if 'RNC Id' in self.ericsson_data.columns:
            unique_rncs = self.ericsson_data['RNC Id'].dropna().unique()
            # Filter out HLTBRE1 and other unwanted RNCs
            filtered_rncs = []
            for rnc in unique_rncs:
                rnc_str = str(rnc).strip()
                # Skip HLTBRE1 and empty values
                if rnc_str and rnc_str != 'HLTBRE1' and not pd.isna(rnc):
                    filtered_rncs.append(rnc_str)

            return sorted(filtered_rncs)

        return []

    def get_rnc_identifiers_zte(self):
        """Get ZTE RNC identifiers - FIXED VERSION"""
        if self.zte_data.empty:
            return []

        if 'RNC Managed NE Name' in self.zte_data.columns:
            unique_rncs = self.zte_data['RNC Managed NE Name'].dropna().unique()
            print(f"DEBUG: All ZTE RNCs found: {unique_rncs}")  # Debug line

            rnc_list = []
            for rnc in unique_rncs:
                if pd.notna(rnc):
                    rnc_str = str(rnc).strip()
                    if rnc_str:  # Not empty after strip
                        rnc_list.append(rnc_str)

            print(f"DEBUG: Filtered ZTE RNC list: {rnc_list}")  # Debug line

            # Return first HNRZ01 entry or simplified name
            if rnc_list:
                # Use the first available RNC and create a simplified display name
                first_rnc = rnc_list[0]
                if 'HNRZ01' in first_rnc:
                    return ['HNRZ01']  # Simplified display name
                else:
                    return [first_rnc]

            return []

        return []

    def extract_kpi_data(self, vendor, kpi_name, column_name, rnc_id):
        """Extract KPI data for specific vendor, KPI, and RNC - FIXED VERSION"""
        if vendor == 'Ericsson':
            data_df = self.ericsson_data
            rnc_column = 'RNC Id'
        else:  # ZTE
            data_df = self.zte_data
            rnc_column = 'RNC Managed NE Name'

        if data_df.empty or column_name not in data_df.columns:
            print(f"DEBUG: No data or column not found for {vendor} - {kpi_name} - {column_name}")
            return [np.nan, np.nan, np.nan]

        print(f"DEBUG: Processing {vendor} - {kpi_name} - RNC: {rnc_id}")

        values = []
        for target_date in self.target_dates:
            try:
                # For ZTE with simplified RNC name, search for any HNRZ01 variant
                if vendor == 'ZTE' and rnc_id == 'HNRZ01':
                    # Get all HNRZ01 entries for the specific date
                    date_filter = data_df['Date'].dt.date == target_date.date()
                    rnc_filter = data_df[rnc_column].astype(str).str.contains('HNRZ01', na=False)
                    filtered_data = data_df[date_filter & rnc_filter]

                    print(f"DEBUG ZTE: Date {target_date.date()}, Found {len(filtered_data)} records")
                else:
                    # Original logic for Ericsson
                    filtered_data = data_df[
                        (data_df['Date'].dt.date == target_date.date()) &
                        (data_df[rnc_column].astype(str).str.contains(str(rnc_id), na=False))
                        ]

                    print(
                        f"DEBUG Ericsson: Date {target_date.date()}, RNC {rnc_id}, Found {len(filtered_data)} records")

                if not filtered_data.empty and column_name in filtered_data.columns:
                    # For ZTE, calculate average of all HNRZ01 entries
                    column_data = filtered_data[column_name].dropna()
                    if not column_data.empty:
                        if vendor == 'ZTE' and len(column_data) > 1:
                            # Average multiple HNRZ01 entries
                            value = column_data.mean()
                            print(f"DEBUG ZTE: Averaged {len(column_data)} values = {value}")
                        else:
                            # Single value or Ericsson
                            value = column_data.iloc[0]
                            print(f"DEBUG: Single value = {value}")

                        # Convert percentage strings to float if needed
                        if isinstance(value, str):
                            if '%' in value:
                                try:
                                    value = float(value.replace('%', ''))
                                except:
                                    value = np.nan
                            else:
                                try:
                                    value = float(value)
                                except:
                                    value = np.nan

                        values.append(value)
                    else:
                        print(f"DEBUG: No valid data in column {column_name}")
                        values.append(np.nan)
                else:
                    print(f"DEBUG: No filtered data found or column missing")
                    values.append(np.nan)

            except Exception as e:
                print(f"WARNING: Error extracting data for {kpi_name}, {rnc_id}: {str(e)}")
                values.append(np.nan)

        print(f"DEBUG: Final values for {vendor}-{kpi_name}-{rnc_id}: {values}")
        return values

    def calculate_delta_d_1(self, current_val, previous_val, kpi_name=None):
        if pd.isna(current_val) or pd.isna(previous_val) or previous_val == 0:
            return 0

        # For CDR (Call Drop Rate), lower is better, so reverse the calculation
        if kpi_name and 'CDR' in kpi_name:
            diff = ((previous_val - current_val) / previous_val) * 100
        else:
            diff = ((current_val - previous_val) / (100 - current_val)) * 100

        return diff

    def calculate_delta_d_7(self, current_val, previous_val, kpi_name=None):
        if pd.isna(current_val) or pd.isna(previous_val) or previous_val == 0:
            return 0

        # For CDR (Call Drop Rate), lower is better, so reverse the calculation
        if kpi_name and 'CDR' in kpi_name:
            diff = ((previous_val - current_val) / previous_val) * 100
        else:
            diff = ((current_val - previous_val) / (100 - previous_val)) * 100

        return diff

    def get_color_for_delta(self, delta, kpi_name=""):
        """Get color based on delta value and KPI type"""
        temp = delta / 100
        if temp >= -0.3 and temp < 0.3:  # Minimal change
            return '#FFFF99'  # Light yellow

        # For CDR (Call Drop Rate), lower is better
        is_lower_better = 'CDR' in kpi_name

        if is_lower_better:
            if temp > 0.3:  # Improvement (decrease in CDR)
                return '#90EE90'  # Light green
            else:  # Degradation (increase in CDR)
                return '#FFB6C1'  # Light red
        else:
            if temp > 0.3:  # Improvement
                return '#90EE90'  # Light green
            else:  # Degradation
                return '#FFB6C1'  # Light red

    def format_delta(self, delta):
        """Alternative format using more compatible symbols"""
        temp = delta / 100
        if temp >= -0.3 and temp < 0.3:
            return f"► {delta:.2f}%"
        elif temp >= 0.3:
            return f"▲ +{delta:.2f}%"  # Triangle up
        else:
            return f"▼ {delta:.2f}%"  # Triangle down

    def get_delta_text_color(self, delta, kpi_name=""):
        temp = delta / 100
        """Get text color for delta based on delta value and KPI type"""
        if temp >= -0.3 and temp < 0.3:  # Minimal change
            return '#000000'  # Black for horizontal arrow

        # For CDR (Call Drop Rate), lower is better
        is_lower_better = 'CDR' in kpi_name

        if is_lower_better:
            if temp >= 0.3:  # Improvement (decrease in CDR) - green
                return '#2E7D32'  # Dark green
            else:  # Degradation (increase in CDR) - red
                return '#C62828'  # Dark red
        else:
            if temp >= 0.3:  # Improvement - green
                return '#2E7D32'  # Dark green
            else:  # Degradation - red
                return '#C62828'  # Dark red

    def create_dashboard(self, title="Daily 3G KPI Dashboard", time_period="BH", save_path=None):
        """Create the combined dashboard"""
        if self.df_combined.empty:
            print("No data to create dashboard!")
            return

        # Get KPI mappings for both vendors
        ericsson_kpis = self.get_kpi_mapping_ericsson()
        zte_kpis = self.get_kpi_mapping_zte()

        # Get RNC identifiers
        ericsson_rncs = self.get_rnc_identifiers_ericsson()
        zte_rncs = self.get_rnc_identifiers_zte()

        # Combine all RNCs with ZTE RNC (HNRZ01) at the end
        all_rncs = ericsson_rncs + zte_rncs

        if not all_rncs:
            print("No RNC identifiers found!")
            return

        print(f"Found RNCs: {all_rncs}")

        # Create figure
        fig, ax = plt.subplots(figsize=(16, 12))
        ax.axis('off')
        ax.set_xlim(0, 1)
        ax.set_ylim(0, 1)

        # Title - positioned higher to avoid overlapping with table
        fig.suptitle(f'{title} ({time_period})', fontsize=16, fontweight='bold', y=0.98)

        # Get all unique KPIs
        all_kpis = list(set(list(ericsson_kpis.keys()) + list(zte_kpis.keys())))
        all_kpis.sort()

        # Create table
        self.create_combined_table(ax, all_kpis, all_rncs, ericsson_kpis, zte_kpis, ericsson_rncs, zte_rncs)

        plt.tight_layout()
        plt.subplots_adjust(top=0.94)  # Leave more space for title

        # Generate filename with data date if save_path not provided
        if save_path is None and self.data_date:
            date_str = self.data_date.strftime('%Y-%m-%d')
            save_path = f"3G_KPI_Dashboard_{time_period}_{date_str}.png"

        # Save as PNG if path provided
        if save_path:
            plt.savefig(save_path, dpi=300, bbox_inches='tight', facecolor='white', pad_inches=0.5)
            print(f"Dashboard saved to: {save_path}")

        plt.show()
        return fig

    def create_combined_table(self, ax, all_kpis, all_rncs, ericsson_kpis, zte_kpis, ericsson_rncs, zte_rncs):
        """Create the combined table with data from both vendors"""
        rows = len(all_kpis) * 5 + 1  # 5 rows per KPI (3 dates + 2 deltas) + header
        cols = len(all_rncs) + 2  # RNCs + Item + KPI columns

        cell_width = 1.0 / cols
        # Adjust cell height to fit within available space (below title)
        available_height = 0.85  # Leave space for title at top
        cell_height = available_height / rows

        # Create header
        self.create_header(ax, all_rncs, cell_width, cell_height)

        # Create data rows for each KPI
        row_idx = 1
        for kpi_name in all_kpis:
            self.create_kpi_section_with_dates(ax, kpi_name, all_rncs, ericsson_kpis, zte_kpis,
                                               ericsson_rncs, zte_rncs, row_idx, cell_width, cell_height)
            row_idx += 5  # 5 rows per KPI

    def create_header(self, ax, all_rncs, cell_width, cell_height):
        """Create header row"""
        # Position header at the very top of available space
        y = 0.85 - cell_height  # Start from top and go down by cell_height

        # Item column
        rect = patches.Rectangle((0, y), cell_width, cell_height,
                                 linewidth=1, edgecolor='black', facecolor='orange')
        ax.add_patch(rect)
        ax.text(cell_width / 2, y + cell_height / 2, 'Item', ha='center', va='center', fontweight='bold')

        # RNC columns
        for i, rnc in enumerate(all_rncs):
            x = (i + 1) * cell_width
            rect = patches.Rectangle((x, y), cell_width, cell_height,
                                     linewidth=1, edgecolor='black', facecolor='orange')
            ax.add_patch(rect)
            # Clean RNC name - remove extra spaces and format properly
            clean_rnc = str(rnc).strip()
            if '(' in clean_rnc:
                # Extract main RNC name if there's additional info in parentheses
                clean_rnc = clean_rnc.split('(')[0].strip()
            ax.text(x + cell_width / 2, y + cell_height / 2, clean_rnc, ha='center', va='center', fontweight='bold',
                    fontsize=10)

        # KPI column
        x = (len(all_rncs) + 1) * cell_width
        rect = patches.Rectangle((x, y), cell_width, cell_height,
                                 linewidth=1, edgecolor='black', facecolor='orange')
        ax.add_patch(rect)
        ax.text(x + cell_width / 2, y + cell_height / 2, 'KPI', ha='center', va='center', fontweight='bold')

    def create_kpi_section_with_dates(self, ax, kpi_name, all_rncs, ericsson_kpis, zte_kpis,
                                      ericsson_rncs, zte_rncs, start_row, cell_width, cell_height):
        """Create 5 rows for a KPI (3 dates + 2 deltas) with merged KPI cell"""

        # Collect data for all RNCs for all 3 dates
        date_data = []  # Will store [latest_values, second_values, week_values]

        for date_idx in range(3):  # 3 target dates
            values_for_date = []
            for rnc in all_rncs:
                # Determine vendor and get data
                if rnc in ericsson_rncs and kpi_name in ericsson_kpis:
                    all_values = self.extract_kpi_data('Ericsson', kpi_name, ericsson_kpis[kpi_name], rnc)
                elif rnc in zte_rncs and kpi_name in zte_kpis:
                    all_values = self.extract_kpi_data('ZTE', kpi_name, zte_kpis[kpi_name], rnc)
                else:
                    all_values = [np.nan, np.nan, np.nan]

                values_for_date.append(all_values[date_idx])

            date_data.append(values_for_date)

        latest_values, second_values, week_values = date_data

        # Calculate deltas
        delta_d1_values = [self.calculate_delta_d_1(latest_values[i], second_values[i], kpi_name) for i in
                           range(len(latest_values))]
        delta_d7_values = [self.calculate_delta_d_7(latest_values[i], week_values[i], kpi_name) for i in
                           range(len(latest_values))]

        # Use consistent header position
        header_y = 0.85  # Same as header position

        # Create merged KPI cell first (spans 5 rows)
        kpi_x = (len(all_rncs) + 1) * cell_width
        kpi_y = header_y - (start_row + 5) * cell_height  # Bottom of the group
        kpi_height = 5 * cell_height  # Spans 5 rows

        rect = patches.Rectangle((kpi_x, kpi_y), cell_width, kpi_height,
                                 linewidth=1, edgecolor='black', facecolor='lightblue')
        ax.add_patch(rect)
        ax.text(kpi_x + cell_width / 2, kpi_y + kpi_height / 2, kpi_name,
                ha='center', va='center', fontsize=10, fontweight='bold', rotation=0)

        # Create rows for each date + deltas (without KPI column)
        # Row 1: Latest date
        y = header_y - (start_row + 1) * cell_height
        self.create_data_row_simple(ax, self.latest_date.strftime('%d-%b-%y'), latest_values,
                                    kpi_name, all_rncs, cell_width, cell_height, y, 'current')

        # Row 2: Second latest date
        y = header_y - (start_row + 2) * cell_height
        self.create_data_row_simple(ax, self.second_latest_date.strftime('%d-%b-%y'), second_values,
                                    kpi_name, all_rncs, cell_width, cell_height, y, 'current')

        # Row 3: Week ago date
        y = header_y - (start_row + 3) * cell_height
        self.create_data_row_simple(ax, self.week_ago_date.strftime('%d-%b-%y'), week_values,
                                    kpi_name, all_rncs, cell_width, cell_height, y, 'current')

        # Row 4: Delta D-1
        y = header_y - (start_row + 4) * cell_height
        self.create_data_row_simple(ax, 'Delta (D-1)', delta_d1_values,
                                    kpi_name, all_rncs, cell_width, cell_height, y, 'delta')

        # Row 5: Delta D-7
        y = header_y - (start_row + 5) * cell_height
        self.create_data_row_simple(ax, 'Delta (D-7)', delta_d7_values,
                                    kpi_name, all_rncs, cell_width, cell_height, y, 'delta')

    def create_data_row_simple(self, ax, row_label, data_values, kpi_name, all_rncs,
                               cell_width, cell_height, y, row_type):
        """Create a single data row with improved delta styling"""

        # Row label with light gray background
        clean_label = str(row_label).strip()
        rect = patches.Rectangle((0, y), cell_width, cell_height,
                                 linewidth=1, edgecolor='black', facecolor='#F5F5F5')
        ax.add_patch(rect)
        ax.text(cell_width / 2, y + cell_height / 2, clean_label, ha='center', va='center',
                fontsize=10, color='black', fontweight='normal', fontfamily='Arial')

        # Data cells
        for i, value in enumerate(data_values):
            x = (i + 1) * cell_width

            if row_type == 'delta':
                bg_color = self.get_color_for_delta(value, kpi_name)
                text_color = self.get_delta_text_color(value, kpi_name)
                display_text = self.format_delta(value)
                font_weight = 'bold'  # Bold font for delta
            else:
                bg_color = '#FFFFFF'  # White background for data cells
                text_color = 'black'
                font_weight = 'normal'

                if pd.isna(value):
                    display_text = "N/A"
                elif 'CDR' in kpi_name:
                    display_text = f"{value:.2f}"
                elif 'Traffic' in kpi_name:
                    if 'CS' in kpi_name:
                        display_text = f"{value:.2f}"
                    else:
                        display_text = f"{value:.0f}"
                else:
                    display_text = f"{value:.2f}"

            # Clean display text
            display_text = str(display_text).strip()

            rect = patches.Rectangle((x, y), cell_width, cell_height,
                                     linewidth=1, edgecolor='black', facecolor=bg_color)
            ax.add_patch(rect)
            ax.text(x + cell_width / 2, y + cell_height / 2, display_text,
                    ha='center', va='center', fontsize=10, color=text_color,
                    fontweight=font_weight, fontfamily='Arial')


# Main functions for easy usage
def find_and_create_dashboard_from_patterns(directory=".", title="Daily 3G KPI Dashboard", time_period="BH",
                                            save_png=True):
    """Find CSV files by patterns and create dashboard"""
    try:
        # Find CSV files based on patterns
        csv_patterns = {
            'ericsson_bh': r'3G_RNO_KPIs_BH_scheduled.*\.csv',
            'ericsson_wd': r'3G_RNO_KPIs_WD_scheduled.*\.csv',
            'zte_bh': r'3G_RNO_KPIs_BH.*ZTE.*\.csv',
            'zte_wd': r'3G_RNO_KPIs_WD.*ZTE.*\.csv'
        }

        found_csvs = {}
        all_files = os.listdir(directory)

        for file_type, pattern in csv_patterns.items():
            matching_files = [f for f in all_files if re.match(pattern, f, re.IGNORECASE)]
            if matching_files:
                matching_files.sort(reverse=True)  # Get most recent
                found_csvs[file_type] = os.path.join(directory, matching_files[0])
                print(f"Found {file_type}: {matching_files[0]}")

        # Select appropriate files based on time_period
        csv_files_list = []
        if time_period.upper() == "BH":
            if 'ericsson_bh' in found_csvs:
                csv_files_list.append(found_csvs['ericsson_bh'])
            if 'zte_bh' in found_csvs:
                csv_files_list.append(found_csvs['zte_bh'])
        else:  # WD/24h
            if 'ericsson_wd' in found_csvs:
                csv_files_list.append(found_csvs['ericsson_wd'])
            if 'zte_wd' in found_csvs:
                csv_files_list.append(found_csvs['zte_wd'])

        if not csv_files_list:
            print(f"No CSV files found for time period: {time_period}")
            return None

        # Create dashboard
        dashboard = Enhanced3GDashboard(csv_files_list=csv_files_list)

        save_path = None
        if save_png and dashboard.data_date:
            date_str = dashboard.data_date.strftime('%Y-%m-%d')
            save_path = f"3G_KPI_Dashboard_{time_period}_{date_str}.png"

        fig = dashboard.create_dashboard(title=title, time_period=time_period, save_path=save_path)
        return fig

    except Exception as e:
        print(f"Error creating dashboard: {str(e)}")
        return None


def create_dashboard_from_folder(folder_path, title="Daily 3G KPI Dashboard", time_period="BH", save_png=True):
    """Create dashboard from all CSV files in a folder"""
    try:
        dashboard = Enhanced3GDashboard(csv_folder_path=folder_path)

        save_path = None
        if save_png and dashboard.data_date:
            date_str = dashboard.data_date.strftime('%Y-%m-%d')
            save_path = os.path.join(folder_path, f"3G_KPI_Dashboard_{time_period}_{date_str}.png")

        fig = dashboard.create_dashboard(title=title, time_period=time_period, save_path=save_path)
        return fig

    except Exception as e:
        print(f"Error creating dashboard: {str(e)}")
        return None


def create_dashboard_from_files(csv_files_list, title="Daily 3G KPI Dashboard", time_period="BH", save_png=True,
                                output_dir="."):
    """Create dashboard from list of CSV files"""
    try:
        dashboard = Enhanced3GDashboard(csv_files_list=csv_files_list)

        save_path = None
        if save_png and dashboard.data_date:
            date_str = dashboard.data_date.strftime('%Y-%m-%d')
            save_path = os.path.join(output_dir, f"3G_KPI_Dashboard_{time_period}_{date_str}.png")

        fig = dashboard.create_dashboard(title=title, time_period=time_period, save_path=save_path)
        return fig

    except Exception as e:
        print(f"Error creating dashboard: {str(e)}")
        return None


# Example usage
if __name__ == "__main__":
    # Method 1: Auto-find files by pattern and create dashboards
    print("Creating BH Dashboard...")
    find_and_create_dashboard_from_patterns(".", "Daily 3G KPI Dashboard by RNC", "BH", save_png=True)

    print("\nCreating 24h Dashboard...")
    find_and_create_dashboard_from_patterns(".", "Daily 3G KPI Dashboard by RNC", "24h", save_png=True)

    # Method 2: Manual file specification (fallback)
    # csv_files = [
    #     "3G_RNO_KPIs_BH_scheduled2025-08-06.csv",
    #     "3G_RNO_KPIs_BH_ZTE_2025-08-06.csv",
    #     "3G_RNO_KPIs_WD_scheduled2025-08-06.csv",
    #     "3G_RNO_KPIs_WD_ZTE_2025-08-06.csv"
    # ]
    #
    # # Create BH dashboard
    # create_dashboard_from_files([f for f in csv_files if 'BH' in f],
    #                             "Daily 3G KPI Dashboard by RNC", "BH", save_png=True)
    #
    # # Create WD (24h) dashboard
    # create_dashboard_from_files([f for f in csv_files if 'WD' in f],
    #                             "Daily 3G KPI Dashboard by RNC", "24h", save_png=True)