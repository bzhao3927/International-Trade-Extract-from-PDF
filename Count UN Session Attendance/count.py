import os
import re
from agentic_doc.parse import parse
from dotenv import load_dotenv, find_dotenv
from pydantic import BaseModel, Field
from typing import List, Optional
import pandas as pd

from langchain_openai import ChatOpenAI
from langchain_core.utils.function_calling import convert_to_openai_function
from langchain.output_parsers.openai_functions import JsonKeyOutputFunctionsParser
from langchain_core.prompts.chat import HumanMessagePromptTemplate, ChatPromptTemplate

# === Load API key ===
_ = load_dotenv(find_dotenv())
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
if not OPENAI_API_KEY:
    raise ValueError("OPENAI_API_KEY is not set.")

# === LangChain model ===
model = ChatOpenAI(model="gpt-4o", temperature=0, api_key=OPENAI_API_KEY)

# === Pydantic schemas ===
class DelegationSession(BaseModel):
    country: str
    officials: Optional[List[str]] = Field(default_factory=list)
    representatives: Optional[List[str]] = Field(default_factory=list)
    alternate_representatives: Optional[List[str]] = Field(default_factory=list)
    advisers: Optional[List[str]] = Field(default_factory=list)
    leader_present: bool
    year: str = "NA"

class DelegationData(BaseModel):
    sessions: List[DelegationSession]

# === Prompt template ===
prompt_template = HumanMessagePromptTemplate.from_template(
    template=(
        "You are extracting UN delegation session data from the following text for the country: {country}.\n\n"
        "Instructions:\n"
        "1. The names listed immediately after the country heading and before the first subheading (such as 'Representatives') should be treated as 'officials'.\n"
        "2. Extract **all** names under each category: officials, representatives, alternate representatives, and advisers.\n"
        "3. The main subheadings in the text are typically: Representatives, Alternate Representatives, and Advisers.\n"
        "4. Treat 'special representatives' as part of the 'alternate representatives' category.\n"
        "5. Group all other attendees — such as experts, observers, interns and similar roles — into the 'advisers' category.\n"
        "   These roles assist or observe but do not participate in voting or speaking during the UN delegation session.\n"
        "6. The lists might be long and span multiple lines. Be exhaustive and include every name.\n"
        "7. If the text mentions 'President' or 'Prime Minister,' or any equivalent title in another language, set leader_present = true.\n\n"
        "Text:\n{text}"
    )
)

# === Build Chain ===
prompt = ChatPromptTemplate.from_messages([prompt_template])
functions = [convert_to_openai_function(DelegationData)]
gpt_func_model = model.bind(functions=functions, function_call={"name": "DelegationData"})
extraction_chain = prompt | gpt_func_model | JsonKeyOutputFunctionsParser(key_name="sessions")

# === Extract text from PDF using agentic_doc.parse ===
def extract_text_from_pdf(pdf_path):
    results = parse(pdf_path)
    full_text = ""
    for res in results:
        full_text += "\n" + res.markdown
    return full_text

# === Normalize country name helper ===
def normalize_country_name(name: str) -> str:
    return re.sub(r'\s+', ' ', name.strip()).upper()

# === Split text by country headings (robust to trailing HTML comments) ===
def split_text_by_country(text):
    chunks = []
    current_country = None
    current_text_lines = []

    for line in text.splitlines():
        line = line.strip()
        if not line:
            continue
        line_clean = re.split(r'<!--', line)[0].strip()

        if re.match(r'^[A-Z][A-Z\s\-&,\'\.]*$', line_clean):
            print(f"Detected country heading: {line_clean}")  # <-- debug here
            if current_country:
                chunks.append((current_country, "\n".join(current_text_lines)))
            current_country = line_clean
            current_text_lines = []
        else:
            current_text_lines.append(line)

    if current_country and current_text_lines:
        chunks.append((current_country, "\n".join(current_text_lines)))

    return chunks

# === Year from filename ===
def get_year_from_filename(filename):
    m = re.search(r'(\d{4})', filename)
    return m.group(1) if m else "NA"

# === Extract sessions from text chunks with GPT chain ===
def extract_sessions_from_text_chunks(chunks, year="NA"):
    sessions = []
    for i, (country, text) in enumerate(chunks):
        print(f"\n[{i+1}/{len(chunks)}] Processing country: {country} ({len(text)} chars)")
        try:
            result = extraction_chain.invoke({"country": country, "text": text})
            if not result:
                print(f"⚠️ GPT returned empty for {country}")
                result = [{
                    "country": country, "year": year, "officials": [], "representatives": [],
                    "alternate_representatives": [], "advisers": [], "leader_present": False
                }]
            for session in result:
                session["year"] = year
                sessions.append(session)
        except Exception as e:
            print(f"❌ Error extracting {country}: {e}")
            sessions.append({
                "country": country, "year": year, "officials": [], "representatives": [],
                "alternate_representatives": [], "advisers": [], "leader_present": False
            })
    return sessions

# === Save all results to Excel ===
# === Save all results to Excel ===
def save_sessions_to_excel(sessions, filename=None):
    folder = "excel_outputs"
    os.makedirs(folder, exist_ok=True)

    years = [int(s["year"]) for s in sessions if s.get("year") and s["year"].isdigit()]
    start_year = min(years) if years else "NA"
    end_year = max(years) if years else "NA"

    if not filename:
        filename = f"delegation_counts_{start_year}-{end_year}.xlsx"
    full_path = os.path.join(folder, filename)

    rows = []
    for s in sessions:
        officials = s.get("officials") or []
        representatives = s.get("representatives") or []
        alternate_reps = s.get("alternate_representatives") or []
        advisers = s.get("advisers") or []

        # Use the ORIGINAL working order
        rows.append({
            "country": s.get("country", "").title(),
            "year": s.get("year", ""),
            "officials": len(officials),
            "representatives": len(representatives),
            "alternate_representatives": len(alternate_reps),
            "advisers": len(advisers),
            "attendees": len(officials) + len(representatives) + len(alternate_reps) + len(advisers),
            "leader_present": int(s.get("leader_present", False)),  # Keep in original position
        })

    df = pd.DataFrame(rows)
    df = df.sort_values(by=["country", "year"])
    
    # NOW reorder the columns to your desired order
    desired_column_order = [
        "country", 
        "year", 
        "officials", 
        "leader_present",  # Move to desired position
        "representatives", 
        "alternate_representatives", 
        "advisers", 
        "attendees"
    ]
    
    # Reorder columns safely
    df = df[desired_column_order]
    
    df.to_excel(full_path, index=False)
    print(f"✅ Excel saved to {full_path}")
    return full_path

# === Main process to handle all PDFs in folder ===
def main():
    folder = "un"
    if not os.path.isdir(folder):
        print(f"❌ Folder not found: {folder}")
        return

    pdf_files = [f for f in os.listdir(folder) if f.lower().endswith(".pdf")]
    print(f"📄 Found {len(pdf_files)} PDF files")

    all_sessions = []

    for i, filename in enumerate(pdf_files, 1):
        path = os.path.join(folder, filename)
        year = get_year_from_filename(filename)
        print(f"\n{'='*50}\n📂 Processing file {i}/{len(pdf_files)}: {filename} (Year: {year})")

        text = extract_text_from_pdf(path)
        if not text:
            print(f"❌ Failed to extract text from {filename}")
            continue

        chunks = split_text_by_country(text)
        print(f"✂️ Split text into {len(chunks)} country chunks")

        sessions = extract_sessions_from_text_chunks(chunks, year)
        all_sessions.extend(sessions)

    if not all_sessions:
        print("⚠️ No sessions extracted from any files.")
        return

    save_sessions_to_excel(all_sessions)
    print(f"\n✅ Done. Extracted data for {len(all_sessions)} sessions across {len(pdf_files)} files.")

if __name__ == "__main__":
    main()
