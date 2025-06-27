import pandas as pd
import os
import re
import numpy as np

def clean_numeric(x):
    if pd.isna(x):
        return np.nan
    try:
        s = str(x)
        # Keep only digits, dot, minus sign
        s_clean = re.sub(r'[^0-9\.-]', '', s)
        if s_clean == '' or s_clean == '.' or s_clean == '-':
            return np.nan
        return float(s_clean)
    except:
        return np.nan

def process_file(filepath):
    print(f"\nProcessing file: {filepath}")
    year_match = re.search(r'(\d{4})', filepath)
    year = int(year_match.group(1)) if year_match else None
    if not year or not (2000 <= year <= 2016):
        print(f"Skipping {filepath}: invalid year")
        return pd.DataFrame()

    try:
        xls = pd.ExcelFile(filepath)
    except Exception as e:
        print(f"Could not open {filepath}: {e}")
        return pd.DataFrame()

    tables = []
    for sheet in xls.sheet_names:
        print(f" Reading sheet: {sheet}")
        try:
            skip = 1 if year <= 2010 else 0
            df = xls.parse(sheet_name=sheet, skiprows=skip, dtype=str)

            print(f"  Sheet shape: {df.shape}")
            if df.empty or df.shape[1] < 3:
                print("  Skipping sheet: empty or less than 3 columns")
                continue

            df.columns = df.columns.astype(str).str.strip()
            country = df.iloc[:, 0].fillna('').astype(str).str.strip()

            print("  Sample countries:", country.head(10).tolist())

            if year <= 2010:
                annual = df.iloc[:, 5].apply(clean_numeric) if df.shape[1] > 5 else pd.Series(np.nan, index=df.index)
                outstanding = df.iloc[:, 8].apply(clean_numeric) if df.shape[1] > 8 else pd.Series(np.nan, index=df.index)

                both_present = (~annual.isna()) & (~outstanding.isna())
                assessed = pd.Series(np.nan, index=annual.index)
                assessed[both_present] = annual[both_present] + outstanding[both_present]

            elif 2011 <= year <= 2015:
                assessed = df.iloc[:, 4].apply(clean_numeric) if df.shape[1] > 4 else pd.Series(np.nan, index=df.index)
                annual = outstanding = pd.Series(np.nan, index=df.index)

            elif year == 2016:
                assessed = df.iloc[:, 7].apply(clean_numeric) if df.shape[1] > 7 else pd.Series(np.nan, index=df.index)
                annual = outstanding = pd.Series(np.nan, index=df.index)

            else:
                print("  Year not in range after checks, skipping sheet")
                continue

            # DEBUG: Print a few rows before appending
            df_temp = pd.DataFrame({
                'year': year,
                'country': country,
                'annual_contributions': annual,
                'total_outstanding_contributions': outstanding,
                'assessed_contributions': assessed
            })

            print(f"  Appending {df_temp.shape[0]} rows from this sheet, sample:")
            print(df_temp.head(5))

            tables.append(df_temp)

        except Exception as e:
            print(f"  Error in {filepath}, sheet '{sheet}': {e}")
            continue

    if tables:
        return pd.concat(tables, ignore_index=True)
    else:
        print(f"  No valid data found in {filepath}")
        return pd.DataFrame()

def merge_all(folder_path):
    all_data = []
    for file in sorted(os.listdir(folder_path)):
        if file.endswith('.xlsx') and not file.startswith('~$'):
            df = process_file(os.path.join(folder_path, file))
            if not df.empty:
                all_data.append(df)
    if all_data:
        return pd.concat(all_data, ignore_index=True)
    else:
        print("No data merged from any files.")
        return pd.DataFrame()

if __name__ == "__main__":
    folder = 'excel_outputs'
    merged = merge_all(folder)

    if not merged.empty:
        merged = merged[['country', 'year', 'annual_contributions',
                         'total_outstanding_contributions', 'assessed_contributions']]
        # Remove rows where country contains 'total' (case-insensitive)
        merged = merged[~merged['country'].str.lower().str.contains('total')]

        print(f"\nFinal merged data sample:")
        print(merged.head(10))
        merged.to_excel('contributions_2000-2016.xlsx', index=False)
        print("\nSaved merged data to contributions_2000-2016.xlsx")
    else:
        print("No data merged.")

