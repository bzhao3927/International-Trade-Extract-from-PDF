"""
UN Contributions Data Extraction Pipeline (2000–2016)

Author: Benjamin Zhao  
Email: bzhao@hamilton.edu  

Description:
This script extracts structured tabular data on Member State contributions
to the UN regular budget from scanned PDF documents using the AgenticDoc
parser. Extracted tables are saved in Excel format, one Excel file per year.
"""

from agentic_doc.parse import parse
import json
import pandas as pd
from pathlib import Path
import re
from typing import List, Dict, Optional
from bs4 import BeautifulSoup
from openpyxl.styles import Font, PatternFill, Alignment, Border, Side
import os

def clean_numeric_value(value: str) -> str:
    if pd.isna(value):
        return ''
    s = str(value).strip()
    s = s.replace(',', '').replace(' ', '')
    s = re.sub(r'[^\d\.-]', '', s)
    if not re.search(r'\d', s):
        return ''
    return s

def save_parse_json(pdf_path: Path, json_output_dir: Path) -> Optional[Path]:
    try:
        results = parse(str(pdf_path), result_save_dir=str(json_output_dir))
        json_path = Path(results[0].result_path)
        print(f"Saved parse JSON to {json_path}")
        return json_path
    except Exception as e:
        print(f"Error parsing {pdf_path}: {e}")
        return None

def extract_tables_from_json(json_file_path: Path) -> List[pd.DataFrame]:
    with open(json_file_path, 'r', encoding='utf-8') as file:
        data = json.load(file)
    
    markdown_content = data.get('markdown', '')
    if not markdown_content:
        print("No 'markdown' key found in JSON file")
        return []
    
    table_pattern = r'<table[^>]*>.*?</table>'
    table_matches = re.findall(table_pattern, markdown_content, re.DOTALL | re.IGNORECASE)
    
    tables = []
    for i, table_html in enumerate(table_matches):
        try:
            soup = BeautifulSoup(table_html, 'html.parser')
            table = soup.find('table')
            if table:
                rows = []
                thead = table.find('thead')
                if thead:
                    for row in thead.find_all('tr'):
                        row_data = []
                        for cell in row.find_all(['th', 'td']):
                            colspan = int(cell.get('colspan', 1))
                            cell_text = cell.get_text(strip=True)
                            row_data.append(cell_text)
                            for _ in range(colspan - 1):
                                row_data.append('')
                        rows.append(row_data)
                tbody = table.find('tbody')
                if tbody:
                    body_rows = tbody.find_all('tr')
                else:
                    body_rows = [tr for tr in table.find_all('tr') if tr.parent.name != 'thead']
                for row in body_rows:
                    row_data = []
                    for cell in row.find_all(['th', 'td']):
                        colspan = int(cell.get('colspan', 1))
                        cell_text = cell.get_text(strip=True)
                        row_data.append(cell_text)
                        for _ in range(colspan - 1):
                            row_data.append('')
                    rows.append(row_data)
                if rows:
                    if len(rows) > 1:
                        df = pd.DataFrame(rows[1:], columns=rows[0])
                    else:
                        df = pd.DataFrame(rows)
                    df = df.replace('', pd.NA)
                    tables.append(df)
                    print(f"Extracted Table {i+1}: {df.shape[0]} rows × {df.shape[1]} columns")
        except Exception as e:
            print(f"Error processing table {i+1}: {str(e)}")
            continue
    return tables

def extract_year_from_filename(filename: str) -> Optional[int]:
    match = re.search(r'(\d{4})', filename)
    return int(match.group(1)) if match else None

def save_tables_to_excel(tables: List[pd.DataFrame], output_file: str, year: int):
    if not tables:
        print("No tables to save")
        return
    
    with pd.ExcelWriter(output_file, engine='openpyxl') as writer:
        for i, df in enumerate(tables):
            sheet_name = f'Table_{i+1}_{year}'
            df.to_excel(writer, sheet_name=sheet_name, index=False)
            
            worksheet = writer.sheets[sheet_name]
            
            header_fill = PatternFill(start_color='366092', end_color='366092', fill_type='solid')
            header_font = Font(bold=True, color='FFFFFF')
            
            for cell in worksheet[1]:
                cell.fill = header_fill
                cell.font = header_font
                cell.alignment = Alignment(horizontal='center', vertical='center')
            
            for column in worksheet.columns:
                max_length = 0
                column_letter = column[0].column_letter
                for cell in column:
                    try:
                        if len(str(cell.value)) > max_length:
                            max_length = len(str(cell.value))
                    except:
                        pass
                adjusted_width = min(max_length + 2, 50)
                worksheet.column_dimensions[column_letter].width = adjusted_width
            
            thin_border = Border(
                left=Side(style='thin'),
                right=Side(style='thin'),
                top=Side(style='thin'),
                bottom=Side(style='thin')
            )
            
            light_fill = PatternFill(start_color='F2F2F2', end_color='F2F2F2', fill_type='solid')
            
            for row_num, row in enumerate(worksheet.iter_rows(min_row=1), 1):
                for cell in row:
                    cell.border = thin_border
                    if row_num > 1 and row_num % 2 == 0:
                        cell.fill = light_fill
    
    print(f"Tables saved to {output_file}")

def process_all_pdfs():
    docs_dir = Path("docs")
    json_dir = Path("json_outputs")
    excel_dir = Path("excel_outputs")
    
    json_dir.mkdir(exist_ok=True)
    excel_dir.mkdir(exist_ok=True)
    
    pdf_files = sorted(docs_dir.glob("*.pdf"))
    
    print(f"Found {len(pdf_files)} PDF files to process")
    
    for pdf_path in pdf_files:
        year = extract_year_from_filename(pdf_path.name)
        
        if not year or year < 2000 or year > 2016:
            print(f"Skipping {pdf_path.name} - year {year} not in range 2000-2016")
            continue
        
        print(f"\n--- Processing {pdf_path.name} (Year: {year}) ---")
        
        json_path = save_parse_json(pdf_path, json_dir)
        if not json_path:
            continue
        
        tables = extract_tables_from_json(json_path)
        if not tables:
            print(f"No tables found in {pdf_path.name}")
            continue
        
        excel_file = excel_dir / f"un_contributions_{year}.xlsx"
        save_tables_to_excel(tables, str(excel_file), year)
        
def process_single_file_debug(pdf_filename: str = "2000.pdf"):
    docs_dir = Path("docs")
    json_dir = Path("json_outputs")
    excel_dir = Path("excel_outputs")
    
    json_dir.mkdir(exist_ok=True)
    excel_dir.mkdir(exist_ok=True)
    
    pdf_path = docs_dir / pdf_filename
    
    if not pdf_path.exists():
        print(f"PDF file not found: {pdf_path}")
        return
    
    year = extract_year_from_filename(pdf_path.name)
    print(f"Processing {pdf_path.name} (Year: {year})")
    
    json_path = save_parse_json(pdf_path, json_dir)
    if not json_path:
        return
    
    tables = extract_tables_from_json(json_path)
    
    if tables:
        print(f"Found {len(tables)} tables")
        
        excel_file = excel_dir / f"debug_{year}.xlsx"
        save_tables_to_excel(tables, str(excel_file), year)
        
        # New code: Read back the saved Excel file and print first 10 rows of first sheet
        print(f"\nReading first 10 rows from saved debug Excel file: {excel_file}")
        try:
            df_check = pd.read_excel(excel_file, sheet_name=0)
            print(df_check.head(10).to_string(index=False))
        except Exception as e:
            print(f"Error reading debug Excel file: {e}")
    else:
        print("No tables found")

if __name__ == "__main__":
    print("UN Contributions PDF Batch Processor")
    print("=" * 50)
    
    print("Step 1: Testing with single file (2000.pdf)...")
    process_single_file_debug("2000.pdf")
    
    print("\nDo you want to continue with full batch processing? (y/n): ")
    response = input().lower().strip()
    
    if response == 'y':
        print("\nStarting full batch processing...")
        process_all_pdfs()
    else:
        print("Debug complete. Check the output files in excel_outputs/ directory.")