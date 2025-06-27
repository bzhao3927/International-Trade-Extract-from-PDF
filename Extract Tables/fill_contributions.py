import pandas as pd
import os
import re
import unicodedata

def normalize_country(name):
    if pd.isna(name):
        return ''
    name = str(name).lower()
    # Remove accents: é -> e, ô -> o, etc.
    name = unicodedata.normalize('NFKD', name).encode('ASCII', 'ignore').decode('utf-8')
    # Remove all non-letter characters (keep only a-z)
    name = re.sub(r'[^a-z]', '', name)
    return name

def append_contributions_to_country_files(country_folder, contributions_file):
    contributions = pd.read_excel(contributions_file)
    contributions['country_original'] = contributions['country']
    contributions['country_norm'] = contributions['country'].apply(normalize_country)

    for filename in os.listdir(country_folder):
        if not filename.endswith('.xlsx') or filename.startswith('~$'):
            continue
        
        path = os.path.join(country_folder, filename)
        print(f"\nUpdating: {filename}")
        
        try:
            df = pd.read_excel(path)
            df['country_original'] = df['country'].astype(str)
            df['country_norm'] = df['country_original'].apply(normalize_country)
            df['year'] = df['year'].astype(int)

            # Drop existing contribution columns if present
            cols_to_drop = [col for col in [
                'annual_contributions', 'total_outstanding_contributions', 'assessed_contributions'
            ] if col in df.columns]
            df_cleaned = df.drop(columns=cols_to_drop)

            # Merge on normalized country and year
            df_merged = df_cleaned.merge(
                contributions[['country_norm', 'year', 'annual_contributions', 
                               'total_outstanding_contributions', 'assessed_contributions']],
                on=['country_norm', 'year'],
                how='left'
            )

            # Drop normalized country column before saving, keep original
            df_merged.drop(columns=['country_norm'], inplace=True)

            # If original country name missing, fill from original contribution data
            df_merged['country'] = df_merged['country_original']
            df_merged.drop(columns=['country_original'], inplace=True)

            # Save updated file
            df_merged.to_excel(path, index=False)
            print(f"  ✔ Saved updated file: {filename}")
        
        except Exception as e:
            print(f"  ❌ Failed to update {filename}: {e}")

# Example usage
if __name__ == "__main__":
    country_folder = "/Users/ben/Documents/International-Trade-Extract-from-PDF/countries"
    contributions_file = "contributions_2000-2016.xlsx"
    append_contributions_to_country_files(country_folder, contributions_file)
