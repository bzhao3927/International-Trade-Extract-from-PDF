import os
import re
import json
import pandas as pd
from typing import List, Dict, Any, Optional
from dataclasses import dataclass, asdict
from dotenv import load_dotenv, find_dotenv
import openai
from agentic_doc.parse import parse

# Load environment variables
load_dotenv(find_dotenv())
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
if not OPENAI_API_KEY:
    raise ValueError("OPENAI_API_KEY is not set. Please set it in your .env file.")

# Initialize OpenAI client
client = openai.OpenAI(api_key=OPENAI_API_KEY)

@dataclass
class DelegationInfo:
    country: str
    year: str
    officials: List[str]
    representatives: List[str]
    alternate_representatives: List[str]
    advisers: List[str]
    leader_present: bool
    leader_name: Optional[str] = None
    
    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)\

class OpenAIDelegateExtractor:
    def __init__(self):
        self.client = client
        
    def load_text_from_debug_file(self, year: str) -> str:
        """Load text from debug_raw_text_*.txt files"""
        debug_filename = f"debug_raw_text_{year}.txt"
        debug_path = os.path.join("txt", debug_filename)
        
        if os.path.exists(debug_path):
            print(f"Loading text from {debug_path}")
            try:
                with open(debug_path, 'r', encoding='utf-8') as f:
                    return f.read()
            except Exception as e:
                print(f"Error reading debug text file: {e}")
                return ""
        else:
            print(f"Debug file {debug_path} not found")
            return ""
    
    def extract_text_from_pdf(self, pdf_path: str) -> str:
        """Extract text from PDF using agentic_doc, or load from cache if available"""
        # Create cache directory
        cache_dir = "text_cache"
        os.makedirs(cache_dir, exist_ok=True)
        
        # Generate cache filename
        pdf_filename = os.path.basename(pdf_path)
        cache_filename = os.path.splitext(pdf_filename)[0] + ".txt"
        cache_path = os.path.join(cache_dir, cache_filename)
        
        # Check if cached text exists
        if os.path.exists(cache_path):
            print(f"Loading cached text from {cache_path}")
            try:
                with open(cache_path, 'r', encoding='utf-8') as f:
                    return f.read()
            except Exception as e:
                print(f"Error reading cached text: {e}")
                # Fall through to extract from PDF
        
        # Extract text from PDF
        try:
            print(f"Extracting text from {pdf_path}...")
            results = parse(pdf_path)
            full_text = ""
            for res in results:
                if hasattr(res, 'markdown'):
                    full_text += "\n" + res.markdown
                elif hasattr(res, 'text'):
                    full_text += "\n" + res.text
            
            # Save to cache
            try:
                with open(cache_path, 'w', encoding='utf-8') as f:
                    f.write(full_text)
                print(f"Text cached to {cache_path}")
            except Exception as e:
                print(f"Error saving text to cache: {e}")
            
            return full_text
        except Exception as e:
            print(f"Error extracting text from {pdf_path}: {e}")
            return ""
    
    def clean_and_segment_text(self, text: str) -> List[Dict[str, str]]:
        """Clean text and segment by countries using OpenAI's intelligence"""
        
        # First, let OpenAI clean and identify country sections
        cleanup_prompt = f"""
        Please analyze this extracted PDF text and identify all country delegation sections.
        The text may contain OCR errors, image descriptions, and formatting issues.
        
        Your task:
        1. Identify all country names (they are typically in ALL CAPS)
        2. Extract the delegation information for each country
        3. Ignore image descriptions, page numbers, and other non-delegation content
        4. Return the result as a JSON array where each object has:
           - "country": the country name (cleaned)
           - "raw_text": the delegation text for that country
        
        Text to analyze:
        {text[:50000]}  # Limit to avoid token limits
        
        Return only valid JSON.
        """
        
        try:
            response = self.client.chat.completions.create(
                model="gpt-4o",
                max_tokens=4000,
                temperature=0,
                messages=[{"role": "user", "content": cleanup_prompt}]
            )
            
            # Extract JSON from OpenAI's response
            response_text = response.choices[0].message.content
            
            # Find JSON in the response
            json_match = re.search(r'\[.*\]', response_text, re.DOTALL)
            if json_match:
                countries_data = json.loads(json_match.group())
                return countries_data
            else:
                print("No JSON found in OpenAI's response")
                return []
                
        except Exception as e:
            print(f"Error in text cleanup: {e}")
            return []
    
    def extract_delegation_info(self, country: str, country_text: str, year: str) -> DelegationInfo:
        """Extract structured delegation information using OpenAI"""
        
        extraction_prompt = f"""
        Extract delegation information for {country} from the following text.
        
        Please identify and categorize all people into these groups:
        
        1. **officials**: People listed directly after the country name but BEFORE the "Representatives" heading (or similar). Look for actual person names that appear immediately after the country header. These are typically high-ranking officials like Presidents, Prime Ministers, or Ministers. Ignore OCR artifacts, page numbers, and descriptive text - focus only on finding real person names in this top section.
        
        2. **representatives**: People listed under "Representatives" or similar headings.
        
        3. **alternate_representatives**: People under "Alternate Representatives" or similar headings.
        
        4. **advisers**: People under "Advisers", "Advisors", or similar headings. Also include people under categories like "Experts", "Observers", or any other specialized roles that typically come after the main representative categories - group all of these under advisers.
        
        6. **leader_present**: Set to true ONLY if a Head of State (President, Prime Minister, King, etc.) is explicitly mentioned and was present during the session.
        
        7. **leader_name**: If a leader is present, provide their name.
        
        Rules:
        - Extract only actual names, not titles or positions alone
        - Clean up OCR errors and formatting issues
        - Ignore image descriptions and metadata
        - Ministers are NOT considered leaders unless they are Head of State
        - Look for phrases like "served as Chairman/Chairperson of the Delegation, ex officio, during his/her presence at the session"
        - CRITICAL: Pay special attention to names appearing immediately after the country header and before any section headings - these are officials
        - Carefully examine the text structure: Country Name -> Officials (if any) -> Section Headings (Representatives, etc.)
        - If someone is described as President, Prime Minister, or Head of State and appears in the text, they should be marked as both an official and leader_present should be true
        
        Return the result as a JSON object with these exact keys:
        {{
            "country": "{country}",
            "year": "{year}",
            "officials": [],
            "representatives": [],
            "alternate_representatives": [],
            "advisers": [],
            "leader_present": false,
            "leader_name": null
        }}
        
        Text to analyze:
        {country_text}
        
        Return only valid JSON.
        """
        
        print(f"    🤖 Calling OpenAI API for {country}...")
        try:
            response = self.client.chat.completions.create(
                model="gpt-4o",
                max_tokens=2000,
                temperature=0,
                messages=[{"role": "user", "content": extraction_prompt}]
            )
            print(f"    ✅ OpenAI API response received for {country}")
            
            response_text = response.choices[0].message.content
            
            # Extract JSON from response
            json_match = re.search(r'\{.*\}', response_text, re.DOTALL)
            if json_match:
                data = json.loads(json_match.group())
                return DelegationInfo(**data)
            else:
                print(f"No JSON found in response for {country}")
                return self._create_empty_delegation(country, year)
                
        except Exception as e:
            print(f"Error extracting delegation info for {country}: {e}")
            return self._create_empty_delegation(country, year)
    
    def _create_empty_delegation(self, country: str, year: str) -> DelegationInfo:
        """Create empty delegation info for error cases"""
        return DelegationInfo(
            country=country,
            year=year,
            officials=[],
            representatives=[],
            alternate_representatives=[],
            advisers=[],
            leader_present=False,
            leader_name=None
        )
    
    def process_single_year(self, year: str) -> List[DelegationInfo]:
        """Process a single year using debug_raw_text_*.txt files"""
        debug_filename = f"debug_raw_text_{year}.txt"
        print(f"Processing {debug_filename}...")
        
        print(f"📅 YEAR: {year}")
        
        # Load text from debug file
        text = self.load_text_from_debug_file(year)
        if not text:
            print(f"No text loaded from {debug_filename}")
            return []
        
        # Clean and segment by countries
        countries_data = self.clean_and_segment_text(text)
        if not countries_data:
            print(f"No countries found in {debug_filename}")
            return []
        
        # Extract delegation info for each country
        delegations = []
        print(f"🌍 Found {len(countries_data)} countries to process:")
        for i, country_data in enumerate(countries_data):
            country = country_data.get('country', f'Unknown_{i}')
            country_text = country_data.get('raw_text', '')
            
            print(f"  🇺🇳 Processing country {i+1}/{len(countries_data)}: {country}")
            delegation = self.extract_delegation_info(country, country_text, year)
            delegations.append(delegation)
            print(f"    ✅ {country} processed - {delegation.officials.__len__() + delegation.representatives.__len__() + delegation.alternate_representatives.__len__() + delegation.advisers.__len__()} total attendees")
        
        return delegations

    def process_single_pdf(self, pdf_path: str) -> List[DelegationInfo]:
        """Process a single PDF file"""
        print(f"Processing {pdf_path}...")
        
        # Extract year from filename
        year = self._extract_year_from_filename(pdf_path)
        print(f"📅 YEAR: {year}")
        
        # Extract text
        text = self.extract_text_from_pdf(pdf_path)
        if not text:
            print(f"No text extracted from {pdf_path}")
            return []
        
        # Clean and segment by countries
        countries_data = self.clean_and_segment_text(text)
        if not countries_data:
            print(f"No countries found in {pdf_path}")
            return []
        
        # Extract delegation info for each country
        delegations = []
        print(f"🌍 Found {len(countries_data)} countries to process:")
        for i, country_data in enumerate(countries_data):
            country = country_data.get('country', f'Unknown_{i}')
            country_text = country_data.get('raw_text', '')
            
            print(f"  🇺🇳 Processing country {i+1}/{len(countries_data)}: {country}")
            delegation = self.extract_delegation_info(country, country_text, year)
            delegations.append(delegation)
            print(f"    ✅ {country} processed - {delegation.officials.__len__() + delegation.representatives.__len__() + delegation.alternate_representatives.__len__() + delegation.advisers.__len__()} total attendees")
        
        return delegations
    
    def _extract_year_from_filename(self, filepath: str) -> str:
        """Extract year from filename"""
        filename = os.path.basename(filepath)
        match = re.search(r'(\d{4})', filename)
        return match.group(1) if match else "Unknown"
    
    def process_pdf_folder(self, folder_path: str) -> List[DelegationInfo]:
        """Process all PDFs in a folder"""
        if not os.path.exists(folder_path):
            print(f"Folder {folder_path} does not exist")
            return []
        
        pdf_files = [f for f in os.listdir(folder_path) if f.lower().endswith('.pdf')]
        pdf_files.sort()
        
        print(f"Found {len(pdf_files)} PDF files in {folder_path}")
        
        all_delegations = []
        for i, pdf_file in enumerate(pdf_files):
            print(f"\n{'='*50}")
            print(f"📄 Processing PDF {i+1}/{len(pdf_files)}: {pdf_file}")
            print(f"{'='*50}")
            pdf_path = os.path.join(folder_path, pdf_file)
            delegations = self.process_single_pdf(pdf_path)
            all_delegations.extend(delegations)
            print(f"✅ PDF {pdf_file} complete: {len(delegations)} countries processed")
        
        return all_delegations
    
    def process_years_from_text_files(self, start_year: int = 2000, end_year: int = 2017) -> List[DelegationInfo]:
        """Process all years from debug_raw_text_*.txt files"""
        import time
        start_time = time.time()
        years = list(range(start_year, end_year + 1))
        
        print(f"🚀 STARTING BULK PROCESSING: {len(years)} years ({start_year}-{end_year})")
        print(f"⏰ Started at: {time.strftime('%H:%M:%S')}")
        print("=" * 70)
        
        all_delegations = []
        for i, year in enumerate(years):
            year_start = time.time()
            progress_percent = ((i) / len(years)) * 100
            
            print(f"\n📅 YEAR {i+1}/{len(years)}: {year} | Progress: {progress_percent:.1f}%")
            print(f"⏱️  Elapsed: {time.time() - start_time:.0f}s | Year started: {time.strftime('%H:%M:%S')}")
            
            delegations = self.process_single_year(str(year))
            all_delegations.extend(delegations)
            
            year_time = time.time() - year_start
            total_delegates = sum(len(d.officials) + len(d.representatives) + len(d.alternate_representatives) + len(d.advisers) for d in delegations)
            
            print(f"✅ Year {year}: {len(delegations)} countries, {total_delegates} total delegates ({year_time:.1f}s)")
            print(f"📊 Total so far: {len(all_delegations)} countries processed")
            
            if i < len(years) - 1:
                remaining = len(years) - i - 1
                avg_time = (time.time() - start_time) / (i + 1)
                eta_seconds = remaining * avg_time
                eta_time = time.strftime('%H:%M:%S', time.localtime(time.time() + eta_seconds))
                print(f"⏳ ETA for completion: {eta_time} (approx {eta_seconds/60:.1f} min remaining)")
        
        total_time = time.time() - start_time
        print(f"\n🎉 BULK PROCESSING COMPLETE!")
        print(f"⏰ Total time: {total_time/60:.1f} minutes")
        print(f"📈 Final count: {len(all_delegations)} countries processed")
        
        return all_delegations
    
    def save_to_excel(self, delegations: List[DelegationInfo], output_path: str = None):
        """Save delegation data to Excel"""
        if not delegations:
            print("No delegations to save")
            return
        
        # Create output directory
        output_dir = "excel_outputs"
        os.makedirs(output_dir, exist_ok=True)
        
        if not output_path:
            years = [d.year for d in delegations if d.year != "Unknown"]
            year_range = f"{min(years)}-{max(years)}" if years else "Unknown"
            output_path = os.path.join(output_dir, f"openai_delegation_counts_{year_range}.xlsx")
        
        # Convert to DataFrame
        rows = []
        for delegation in delegations:
            officials_count = len(delegation.officials)
            
            row = {
                "country": delegation.country,
                "year": delegation.year,
                "officials": officials_count,
                "representatives": len(delegation.representatives),
                "alternate_representatives": len(delegation.alternate_representatives),
                "advisers": len(delegation.advisers),
                "total_attendees": (officials_count + 
                                  len(delegation.representatives) + 
                                  len(delegation.alternate_representatives) + 
                                  len(delegation.advisers)),
                "leader_present": int(delegation.leader_present)
            }
            rows.append(row)
        
        df = pd.DataFrame(rows)
        df['year'] = pd.to_numeric(df['year'], errors='coerce')
        
        # Standardize country names to title case
        df['country'] = df['country'].str.title().str.strip()
        df = df.sort_values(["country", "year"], na_position='last')
        
        # Save to Excel
        df.to_excel(output_path, index=False)
        print(f"Results saved to {output_path}")
        
        # Print summary
        print(f"\nSummary:")
        print(f"Total delegations: {len(delegations)}")
        print(f"Countries: {len(df['country'].unique())}")
        print(f"Years: {sorted(df['year'].unique())}")
        print(f"Leaders present: {df['leader_present'].sum()}")
        
        return output_path
    
    def save_detailed_json(self, delegations: List[DelegationInfo], output_path: str = None):
        """Save detailed delegation data to JSON"""
        if not delegations:
            print("No delegations to save")
            return
        
        output_dir = "json_outputs"
        os.makedirs(output_dir, exist_ok=True)
        
        if not output_path:
            years = [d.year for d in delegations if d.year != "Unknown"]
            year_range = f"{min(years)}-{max(years)}" if years else "Unknown"
            output_path = os.path.join(output_dir, f"openai_delegation_details_{year_range}.json")
        
        # Convert to JSON-serializable format
        data = [delegation.to_dict() for delegation in delegations]
        
        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
        
        print(f"Detailed data saved to {output_path}")
        return output_path

def main():
    """Main function to process existing text files"""
    extractor = OpenAIDelegateExtractor()
    
    # Process years from existing debug_raw_text_*.txt files
    delegations = extractor.process_years_from_text_files(2000, 2017)
    
    if delegations:
        # Save results
        extractor.save_to_excel(delegations)
        extractor.save_detailed_json(delegations)
    else:
        print("No delegations extracted")

if __name__ == "__main__":
    main()