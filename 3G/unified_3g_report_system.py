import os
import sys
import glob
import re
from datetime import datetime, timedelta
import subprocess
import shutil
from pathlib import Path
import pandas as pd
from reportlab.lib.pagesizes import A4, A3
from reportlab.pdfgen import canvas
from reportlab.lib.units import inch
from PIL import Image
import importlib.util


class Daily3GReportGenerator:
    def __init__(self, target_date=None):
        """
        Initialize the Daily 3G Report Generator

        Args:
            target_date: Target date for report (YYYY-MM-DD format). If None, uses today.
        """
        self.target_date = target_date if target_date else datetime.now().strftime('%Y-%m-%d')
        self.timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        self.report_dir = f"3G_Daily_Report_{self.target_date}"
        self.output_images = []

        # Create report directory
        os.makedirs(self.report_dir, exist_ok=True)

        print(f"=== Daily 3G Report Generator ===")
        print(f"Target Date: {self.target_date}")
        print(f"Report Directory: {self.report_dir}")
        print(f"Timestamp: {self.timestamp}")

    def find_excel_files_by_date(self, target_date):
        """
        Find Excel files matching the target date pattern

        Args:
            target_date: Date string in YYYY-MM-DD format

        Returns:
            dict: Dictionary of found files by type
        """
        print(f"\n[STEP 1] Finding Excel files for date: {target_date}")
        print("-" * 50)

        # Define file patterns with flexible date matching
        patterns = {
            'ericsson_bh': [
                f'3G_RNO_KPIs_BH_scheduled{target_date}.xlsx',
                f'3G_RNO_KPIs_BH_scheduled_{target_date}.xlsx',
                f'3G_RNO_KPIs_BH_scheduled*{target_date}*.xlsx'
            ],
            'ericsson_wd': [
                f'3G_RNO_KPIs_WD_scheduled{target_date}.xlsx',
                f'3G_RNO_KPIs_WD_scheduled_{target_date}.xlsx',
                f'3G_RNO_KPIs_WD_scheduled*{target_date}*.xlsx'
            ],
            'zte_bh': [
                f'3G_RNO_KPIs_BH_ZTE_{target_date}.xlsx',
                f'3G_RNO_KPIs_BH*ZTE*{target_date}*.xlsx',
                f'*ZTE*BH*{target_date}*.xlsx'
            ],
            'zte_wd': [
                f'3G_RNO_KPIs_WD_ZTE_{target_date}.xlsx',
                f'3G_RNO_KPIs_WD*ZTE*{target_date}*.xlsx',
                f'*ZTE*WD*{target_date}*.xlsx'
            ],
            'rtwp_ericsson': [
                f'RTWP_3G.xlsx',
                f'RTWP*3G*.xlsx',
                f'*RTWP*Ericsson*.xlsx'
            ],
            'rtwp_zte': [
                f'History Performance_UMTS _RNO_Avg_Mean_RTWP.xlsx',
                f'History*Performance*UMTS*.xlsx',
                f'*RTWP*ZTE*.xlsx'
            ]
        }

        found_files = {}

        for file_type, pattern_list in patterns.items():
            for pattern in pattern_list:
                matches = glob.glob(pattern)
                if matches:
                    # Take the most recent file if multiple matches
                    matches.sort(key=os.path.getmtime, reverse=True)
                    found_files[file_type] = matches[0]
                    print(f"✓ Found {file_type}: {matches[0]}")
                    break

            if file_type not in found_files:
                print(f"✗ Missing {file_type}")

        print(f"\nTotal files found: {len(found_files)}")
        return found_files

    def load_module_from_file(self, module_name, file_path):
        """
        Dynamically load a Python module from file path

        Args:
            module_name: Name to assign to the module
            file_path: Path to the Python file

        Returns:
            module: Loaded module object
        """
        spec = importlib.util.spec_from_file_location(module_name, file_path)
        module = importlib.util.module_from_spec(spec)
        sys.modules[module_name] = module
        spec.loader.exec_module(module)
        return module

    def run_data_visualization(self):
        """
        Execute DataVisualizationFor3G.py
        """
        print(f"\n[STEP 2] Running Data Visualization For 3G")
        print("-" * 50)

        try:
            # Load the module
            viz_module = self.load_module_from_file("DataVisualizationFor3G", "DataVisualizationFor3G.py")

            # Create processor instance
            processor = viz_module.ExcelCSVProcessorFor3G()

            # Find files by pattern
            found_files = processor.find_excel_files_by_pattern(".")

            if not found_files:
                print("No Excel files found for Data Visualization!")
                return []

            # Convert files and create dashboards
            converted_files_zte = {}
            converted_files_ericsson = {}

            # Process ZTE files
            for file_type in ['zte_bh', 'zte_wd']:
                if file_type in found_files:
                    excel_file = found_files[file_type]
                    csv_file = f"3G_RNO_KPIs_{file_type.split('_')[1].upper()}_ZTE_{self.target_date}.csv"

                    df = processor.clean_excel_to_csv_ZTE(excel_file, csv_file)
                    if df is not None:
                        converted_files_zte[excel_file] = csv_file

            # Process Ericsson files
            for file_type in ['ericsson_bh', 'ericsson_wd']:
                if file_type in found_files:
                    excel_file = found_files[file_type]
                    csv_file = f"3G_RNO_KPIs_{file_type.split('_')[1].upper()}_scheduled{self.target_date}.csv"

                    df = processor.clean_excel_to_csv_ericsson(excel_file, csv_file)
                    if df is not None:
                        converted_files_ericsson[excel_file] = csv_file

            generated_images = []

            # Create individual vendor dashboards
            if len(converted_files_ericsson) >= 2:
                csv_files_ericsson = list(converted_files_ericsson.values())
                csv_all_day_ericsson = [f for f in csv_files_ericsson if 'WD' in f][0] if any(
                    'WD' in f for f in csv_files_ericsson) else csv_files_ericsson[0]
                csv_busy_hour_ericsson = [f for f in csv_files_ericsson if 'BH' in f][0] if any(
                    'BH' in f for f in csv_files_ericsson) else csv_files_ericsson[1]

                output_dir_ericsson = "output_ericsson"
                os.makedirs(output_dir_ericsson, exist_ok=True)
                ericsson_dashboard = processor.create_daily_dashboard_table_ericsson(
                    csv_all_day_ericsson, csv_busy_hour_ericsson, output_dir_ericsson)
                if ericsson_dashboard:
                    generated_images.append(ericsson_dashboard)

            if len(converted_files_zte) >= 2:
                csv_files_zte = list(converted_files_zte.values())
                csv_all_day_zte = [f for f in csv_files_zte if 'WD' in f][0] if any(
                    'WD' in f for f in csv_files_zte) else csv_files_zte[0]
                csv_busy_hour_zte = [f for f in csv_files_zte if 'BH' in f][0] if any(
                    'BH' in f for f in csv_files_zte) else csv_files_zte[1]

                output_dir_zte = "output_zte"
                os.makedirs(output_dir_zte, exist_ok=True)
                zte_dashboard = processor.create_daily_dashboard_table_ZTE(
                    csv_all_day_zte, csv_busy_hour_zte, output_dir_zte)
                if zte_dashboard:
                    generated_images.append(zte_dashboard)

            # Create RNC dashboards and charts
            if len(converted_files_ericsson) >= 2 and len(converted_files_zte) >= 2:
                rnc_output_dir = "output_rnc_dashboards_improved"
                os.makedirs(rnc_output_dir, exist_ok=True)

                csv_files_ericsson = list(converted_files_ericsson.values())
                csv_files_zte = list(converted_files_zte.values())

                csv_all_day_ericsson = [f for f in csv_files_ericsson if 'WD' in f][0] if any(
                    'WD' in f for f in csv_files_ericsson) else csv_files_ericsson[0]
                csv_bh_ericsson = [f for f in csv_files_ericsson if 'BH' in f][0] if any(
                    'BH' in f for f in csv_files_ericsson) else csv_files_ericsson[1]
                csv_all_day_zte = [f for f in csv_files_zte if 'WD' in f][0] if any(
                    'WD' in f for f in csv_files_zte) else csv_files_zte[0]
                csv_bh_zte = [f for f in csv_files_zte if 'BH' in f][0] if any('BH' in f for f in csv_files_zte) else \
                csv_files_zte[1]

                processor.create_daily_rnc_dashboard(
                    csv_all_day_ericsson=csv_all_day_ericsson,
                    csv_bh_ericsson=csv_bh_ericsson,
                    csv_all_day_zte=csv_all_day_zte,
                    csv_bh_zte=csv_bh_zte,
                    output_dir=rnc_output_dir
                )

                # Collect generated images from RNC output directory
                rnc_images = glob.glob(os.path.join(rnc_output_dir, "*.png"))
                generated_images.extend(rnc_images)

            print(f"Data Visualization completed. Generated {len(generated_images)} images.")
            return generated_images

        except Exception as e:
            print(f"Error in Data Visualization: {e}")
            import traceback
            traceback.print_exc()
            return []

    def run_kpi_dashboard_by_rnc(self):
        """
        Execute 3GKPIDashboardByRNC.py
        """
        print(f"\n[STEP 3] Running 3G KPI Dashboard By RNC")
        print("-" * 50)

        try:
            # Load the module
            kpi_module = self.load_module_from_file("3GKPIDashboardByRNC", "3GKPIDashboardByRNC.py")

            # Use the functions from the module
            # Try BH dashboard first
            try:
                fig_bh = kpi_module.find_and_create_dashboard_from_patterns(".", "Daily 3G KPI Dashboard by RNC", "BH",
                                                                            save_png=True)
                bh_image = f"3G_KPI_Dashboard_BH_{self.target_date}.png"
            except Exception as e:
                print(f"Error creating BH dashboard: {e}")
                bh_image = None

            # Try 24h dashboard
            try:
                fig_24h = kpi_module.find_and_create_dashboard_from_patterns(".", "Daily 3G KPI Dashboard by RNC",
                                                                             "24h", save_png=True)
                h24_image = f"3G_KPI_Dashboard_24h_{self.target_date}.png"
            except Exception as e:
                print(f"Error creating 24h dashboard: {e}")
                h24_image = None

            generated_images = []
            if bh_image and os.path.exists(bh_image):
                generated_images.append(bh_image)
            if h24_image and os.path.exists(h24_image):
                generated_images.append(h24_image)

            print(f"KPI Dashboard By RNC completed. Generated {len(generated_images)} images.")
            return generated_images

        except Exception as e:
            print(f"Error in KPI Dashboard By RNC: {e}")
            import traceback
            traceback.print_exc()
            return []

    def run_count_abnormal_cell(self):
        """
        Execute CountAbnormalCellFor3G.py
        """
        print(f"\n[STEP 4] Running Count Abnormal Cell For 3G")
        print("-" * 50)

        try:
            # Load the module
            count_module = self.load_module_from_file("CountAbnormalCellFor3G", "CountAbnormalCellFor3G.py")

            # Create processor instance
            processor = count_module.CountAbnormalCellFor3G()

            # Find required Excel files
            excel_files_ericsson = {'RTWP_3G.xlsx': 'RTWP_3G_Ericsson.csv'}
            excel_files_zte = {'History Performance_UMTS _RNO_Avg_Mean_RTWP.xlsx': 'RTWP_3G_ZTE.csv'}

            all_abnormal_cells = []

            # Process ZTE files
            for excel_file, csv_file in excel_files_zte.items():
                if os.path.exists(excel_file):
                    df = processor.clean_excel_to_csv_ZTE(excel_file, csv_file)
                    if df is not None and processor.verify_csv_structure(csv_file):
                        abnormal_cells = processor.count_abnormal_cells_zte(csv_file, rtwp_threshold=-95)
                        if abnormal_cells is not None and not abnormal_cells.empty:
                            all_abnormal_cells.append(abnormal_cells)

            # Process Ericsson files
            for excel_file, csv_file in excel_files_ericsson.items():
                if os.path.exists(excel_file):
                    df = processor.clean_excel_to_csv_ericsson(excel_file, csv_file)
                    if df is not None and processor.verify_csv_structure(csv_file):
                        abnormal_cells = processor.count_abnormal_cells_ericsson(csv_file, rtwp_threshold=-95)
                        if abnormal_cells is not None and not abnormal_cells.empty:
                            all_abnormal_cells.append(abnormal_cells)

            generated_images = []

            if all_abnormal_cells:
                combined_df = pd.concat(all_abnormal_cells, ignore_index=True)

                # Create summary table
                summary_table = processor.create_summary_table(combined_df)

                if summary_table is not None:
                    # Export summary table as image
                    table_image = 'rtwp_summary_table.png'
                    processor.export_summary_table_as_image(summary_table, table_image, combined_df)
                    if os.path.exists(table_image):
                        generated_images.append(table_image)

                    # Export to Excel
                    processor.export_summary_to_excel(summary_table, combined_df, 'rtwp_analysis_data.xlsx')

                    # Generate detailed report
                    processor.generate_detailed_report(combined_df, 'rtwp_analysis_report.txt')

                    # Create visualization
                    chart_image = 'rtwp_trend_chart.png'
                    processor.plot_top_provinces(summary_table, top_n=4, save_path=chart_image)
                    if os.path.exists(chart_image):
                        generated_images.append(chart_image)

                    # Create PDF from images
                    try:
                        processor.create_pdf_from_images(
                            image1_path=table_image,
                            image2_path=chart_image,
                            output_pdf_path='rtwp_analysis_report.pdf'
                        )
                    except Exception as e:
                        print(f"Error creating RTWP PDF: {e}")

            print(f"Count Abnormal Cell completed. Generated {len(generated_images)} images.")
            return generated_images

        except Exception as e:
            print(f"Error in Count Abnormal Cell: {e}")
            import traceback
            traceback.print_exc()
            return []

    def collect_all_images(self):
        """
        Collect all generated PNG images from all processes
        """
        print(f"\n[STEP 5] Collecting all generated images")
        print("-" * 50)

        # Define search patterns for images
        image_patterns = [
            "*.png",
            "output_ericsson/*.png",
            "output_zte/*.png",
            "output_rnc_dashboards_improved/*.png",
            "3G_KPI_Dashboard_*.png",
            "rtwp_*.png"
        ]

        all_images = []
        for pattern in image_patterns:
            matches = glob.glob(pattern)
            all_images.extend(matches)

        # Remove duplicates and filter only existing files
        unique_images = []
        seen = set()
        for img in all_images:
            if img not in seen and os.path.exists(img):
                unique_images.append(img)
                seen.add(img)

        print(f"Found {len(unique_images)} unique images:")
        for img in unique_images:
            print(f"  - {img}")

        return unique_images

    def create_comprehensive_pdf_report(self, image_list):
        """
        Create a comprehensive PDF report with all images

        Args:
            image_list: List of image file paths
        """
        print(f"\n[STEP 6] Creating comprehensive PDF report")
        print("-" * 50)

        if not image_list:
            print("No images found for PDF report")
            return None

        # Create PDF filename with timestamp
        pdf_filename = f"Daily_3G_Report_{self.target_date}_{self.timestamp}.pdf"
        pdf_path = os.path.join(self.report_dir, pdf_filename)

        try:
            # Create canvas with A3 page size
            c = canvas.Canvas(pdf_path, pagesize=A3)
            width, height = A3

            # Add title page
            self._create_title_page(c, width, height)
            c.showPage()

            # Add each image as a separate page
            for i, image_path in enumerate(image_list):
                try:
                    print(f"Adding image {i + 1}/{len(image_list)}: {image_path}")
                    self._add_image_page(c, image_path, width, height, i + 1, len(image_list))
                    c.showPage()
                except Exception as e:
                    print(f"Error adding image {image_path}: {e}")
                    continue

            # Save PDF
            c.save()

            print(f"PDF report created successfully: {pdf_path}")
            return pdf_path

        except Exception as e:
            print(f"Error creating PDF report: {e}")
            import traceback
            traceback.print_exc()
            return None

    def _create_title_page(self, canvas, width, height):
        """Create title page for PDF report"""
        current_time = datetime.now()

        # Title
        canvas.setFont("Helvetica-Bold", 24)
        title = "Daily 3G Network Performance Report"
        title_width = canvas.stringWidth(title, "Helvetica-Bold", 24)
        canvas.drawString((width - title_width) / 2, height - 100, title)

        # Date and time
        canvas.setFont("Helvetica", 16)
        date_str = f"Report Date: {self.target_date}"
        date_width = canvas.stringWidth(date_str, "Helvetica", 16)
        canvas.drawString((width - date_width) / 2, height - 150, date_str)

        generated_str = f"Generated: {current_time.strftime('%Y-%m-%d %H:%M:%S')}"
        generated_width = canvas.stringWidth(generated_str, "Helvetica", 16)
        canvas.drawString((width - generated_width) / 2, height - 180, generated_str)

        # Report sections
        canvas.setFont("Helvetica-Bold", 14)
        sections_title = "Report Sections:"
        canvas.drawString(100, height - 250, sections_title)

        canvas.setFont("Helvetica", 12)
        sections = [
            "1. Data Visualization Dashboards",
            "2. Individual Vendor Performance (Ericsson & ZTE)",
            "3. RNC-based KPI Analysis",
            "4. KPI Dashboard by RNC (BH & 24h)",
            "5. Abnormal Cell Analysis (RTWP)",
            "6. Trend Analysis and Charts"
        ]

        y_pos = height - 280
        for section in sections:
            canvas.drawString(120, y_pos, section)
            y_pos -= 25

        # Footer
        canvas.setFont("Helvetica", 10)
        footer = "Generated by Daily 3G Report Generator"
        footer_width = canvas.stringWidth(footer, "Helvetica", 10)
        canvas.drawString((width - footer_width) / 2, 50, footer)

    def _add_image_page(self, canvas, image_path, width, height, page_num, total_pages):
        """Add image page to PDF"""
        try:
            # Open and analyze image
            img = Image.open(image_path)
            img_width, img_height = img.size
            img_aspect = img_width / img_height

            # Calculate available space (leave margins and space for header/footer)
            margin = 50
            header_space = 80
            footer_space = 30

            available_width = width - (2 * margin)
            available_height = height - header_space - footer_space - (2 * margin)

            # Calculate image dimensions to fit page
            if img_aspect > (available_width / available_height):
                # Image is wider, fit to width
                final_width = available_width
                final_height = final_width / img_aspect
            else:
                # Image is taller, fit to height
                final_height = available_height
                final_width = final_height * img_aspect

            # Center the image
            x = (width - final_width) / 2
            y = (height - final_height) / 2

            # Add header with image name and page number
            canvas.setFont("Helvetica-Bold", 12)
            image_name = os.path.basename(image_path)
            canvas.drawString(margin, height - 30, f"Image: {image_name}")

            page_text = f"Page {page_num} of {total_pages}"
            page_width = canvas.stringWidth(page_text, "Helvetica-Bold", 12)
            canvas.drawString(width - margin - page_width, height - 30, page_text)

            # Draw the image
            canvas.drawImage(image_path, x, y, final_width, final_height)

            # Add footer with timestamp
            canvas.setFont("Helvetica", 8)
            footer = f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
            footer_width = canvas.stringWidth(footer, "Helvetica", 8)
            canvas.drawString((width - footer_width) / 2, 20, footer)

        except Exception as e:
            print(f"Error adding image {image_path}: {e}")
            # Add error page
            canvas.setFont("Helvetica", 12)
            error_text = f"Error loading image: {os.path.basename(image_path)}"
            canvas.drawString(100, height / 2, error_text)

    def cleanup_temporary_files(self):
        """Clean up temporary files and organize final outputs"""
        print(f"\n[STEP 7] Organizing output files")
        print("-" * 50)

        try:
            # Copy important images to report directory
            image_list = self.collect_all_images()

            for img in image_list:
                if os.path.exists(img):
                    dest_path = os.path.join(self.report_dir, os.path.basename(img))
                    shutil.copy2(img, dest_path)

            # Copy important data files
            data_files = [
                'rtwp_analysis_data.xlsx',
                'rtwp_analysis_report.txt'
            ]

            for data_file in data_files:
                if os.path.exists(data_file):
                    dest_path = os.path.join(self.report_dir, data_file)
                    shutil.copy2(data_file, dest_path)

            print(f"Output files organized in: {self.report_dir}")

        except Exception as e:
            print(f"Error organizing files: {e}")

    def generate_full_report(self):
        """
        Generate the complete daily 3G report
        """
        print(f"\n{'=' * 60}")
        print(f"STARTING DAILY 3G REPORT GENERATION")
        print(f"{'=' * 60}")

        # Step 1: Check required files
        found_files = self.find_excel_files_by_date(self.target_date)

        if len(found_files) < 4:
            print(f"Warning: Only found {len(found_files)} out of expected 6 files")
            print("Continuing with available files...")

        all_images = []

        # Step 2: Run Data Visualization
        viz_images = self.run_data_visualization()
        all_images.extend(viz_images)

        # Step 3: Run KPI Dashboard By RNC
        kpi_images = self.run_kpi_dashboard_by_rnc()
        all_images.extend(kpi_images)

        # Step 4: Run Count Abnormal Cell
        abnormal_images = self.run_count_abnormal_cell()
        all_images.extend(abnormal_images)

        # Step 5: Collect all images
        final_images = self.collect_all_images()

        # Step 6: Create comprehensive PDF
        pdf_path = self.create_comprehensive_pdf_report(final_images)

        # Step 7: Organize outputs
        self.cleanup_temporary_files()

        # Final summary
        print(f"\n{'=' * 60}")
        print(f"DAILY 3G REPORT GENERATION COMPLETED")
        print(f"{'=' * 60}")
        print(f"Target Date: {self.target_date}")
        print(f"Total Images Generated: {len(final_images)}")
        print(f"Report Directory: {self.report_dir}")
        if pdf_path:
            print(f"PDF Report: {pdf_path}")
        print(f"{'=' * 60}")

        return pdf_path


def main():
    """
    Main function to run the Daily 3G Report Generator
    """
    import argparse

    # Parse command line arguments
    parser = argparse.ArgumentParser(description='Generate Daily 3G Network Report')
    parser.add_argument('--date', '-d', type=str,
                        help='Target date in YYYY-MM-DD format (default: today)')

    args = parser.parse_args()

    # Use provided date or default to today
    target_date = args.date if args.date else datetime.now().strftime('%Y-%m-%d')

    # Validate date format
    try:
        datetime.strptime(target_date, '%Y-%m-%d')
    except ValueError:
        print("Error: Date must be in YYYY-MM-DD format")
        return

    # Create and run report generator
    generator = Daily3GReportGenerator(target_date=target_date)

    try:
        pdf_path = generator.generate_full_report()

        if pdf_path and os.path.exists(pdf_path):
            print(f"\n✓ Report generation successful!")
            print(f"✓ PDF Report: {pdf_path}")
        else:
            print(f"\n✗ Report generation completed but PDF creation failed")

    except KeyboardInterrupt:
        print(f"\n\nReport generation interrupted by user")
    except Exception as e:
        print(f"\n✗ Error during report generation: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    main()