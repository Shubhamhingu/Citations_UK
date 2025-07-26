import re
import os
import fitz
import sqlite3
from datetime import datetime

PDF_FOLDER = "Jersey"
DB_PATH = PDF_FOLDER + ".db"
REPORTER_DB_PATH = "Reporters.db"

CITATION_PATTERN = (
    r"((?:[A-Z][A-Za-z0-9.,'’()\- &:]+v\.? [A-Z][A-Za-z0-9.,'’()\- &:]+|"
    r"[A-Z][A-Za-z0-9.,'’()\- &:]+))"
    r"\s*(\[\d{4}\](?:\s\(\d{1,4}\))?\s*\d{0,4}\s*[A-Za-z. ]+[ ]+\s*\d{1,4}(?:\s[A-Za-z]+[ ]+\s\d{1,4})?|\(\d{4}\)(?:\s\(\d{1,4}\))?\s*\d{0,4}\s*[A-Za-z.]+[ ]+\s*\d{1,4}(?:\s[A-Za-z]+\s\d{1,4})?)"
)
NO_NAME_PATTERN = r"(\[\d{4}\]\s*\d{0,4}\s*[A-Za-z.]+[ ]+\s*\d{1,4}(?:\s[A-Za-z]+\s\d{1,4})?|\(\d{4}\)\s*\d{0,4}\s*[A-Za-z.]+[ ]+\s*\d{1,4}(?:\s[A-Za-z]+\s\d{1,4})?)"
YEAR_REPORTER_PATTERN = r"\[(?P<year1>\d{4})\]\s*\d*\s(?P<rptr1>[A-Za-z. ]+)\s\d{1,4}|\((?P<year2>\d{4})\)\s*\d*\s(?P<rptr2>[A-Za-z. ]+)\s\d{1,4}"

def connect_db(db_path):
    return sqlite3.connect(db_path)

def create_tables(cur):
    cur.execute("""
    CREATE TABLE IF NOT EXISTS main_paper (
        neutral_citation TEXT PRIMARY KEY,
        name TEXT,
        jurisdiction TEXT,
        judge TEXT,
        judgment_date TEXT,
        reported_in TEXT,
        court TEXT,
        vlex_document_id TEXT,
        link TEXT
    )
    """)
    cur.execute("""
    CREATE TABLE IF NOT EXISTS citations (
        neutral_citation TEXT,
        citation_name TEXT,
        citation TEXT,
        reporter TEXT,
        jurisdiction TEXT,
        year INTEGER,
        FOREIGN KEY (neutral_citation) REFERENCES main_paper(neutral_citation)
    )
    """)

def parse_judgment_date(date_str):
    months = {
        'Jan': 'January', 'Feb': 'February', 'Mar': 'March', 'Apr': 'April',
        'May': 'May', 'Jun': 'June', 'Jul': 'July', 'Aug': 'August',
        'Sep': 'September', 'Oct': 'October', 'Nov': 'November', 'Dec': 'December'
    }
    for abbr, full in months.items():
        date_str = re.sub(rf"\b{abbr}\b", full, date_str, flags=re.IGNORECASE)
    date_str = date_str.strip()
    for fmt in ("%d %B %Y", "%d %b %Y", "%d %B, %Y", "%d %b, %Y"):
        try:
            return datetime.strptime(date_str, fmt).date().isoformat()
        except ValueError:
            continue
    return date_str

def extract_case_name(text):
    pattern = r"Otherwise, distribution or reproduction is not permitted(.*)\sJurisdiction:"
    match = re.search(pattern, text, re.DOTALL)
    if match:
        case_name = match.group(1).strip()
        case_name = re.sub(r"[.,;:!?]$", "", case_name)
        return case_name
    return None

def extract_metadata(text, new_text):
    patterns = {
        "jurisdiction": r"Jurisdiction:\s*(.*)",
        "judge": r"Judge:\s*(.*)",
        "judgment_date": r"Judgment\sDate:\s*(.*)|Date:\s*(.*)",
        "neutral_citation": r"Judgment\scitation\s\(vLex\):\s*(.*)|Neutral\sCitation:\s*(.*)",
        "reported_in": r"Reported\sIn:\s*(.*)",
        "court": r"Court:\s*(.*)Date|Court:\s(.*)",
        "vlex_document_id": r"vLex Document Id:\s*(.*)",
        "link": r"Link:\s*(.*)"
    }
    meta = {}
    meta["name"] = extract_case_name(new_text)
    for key, pat in patterns.items():
        m = re.search(pat, text, re.IGNORECASE)
        value = None
        if m:
            for group in m.groups():
                if group and group.strip():
                    value = group.strip()
                    break
        if key == "judgment_date" and value:
            value = parse_judgment_date(value)
        meta[key] = value
    return meta

def reporter_jurisdiction_dict(reporterdbpath):
    connRep = sqlite3.connect(reporterdbpath)
    curRep = connRep.cursor()
    curRep.execute("""
                   SELECT Reporter_cleaned, Reporter, Jurisdiction FROM jersey_reporters
                   """)
    rows = curRep.fetchall()
    connRep.close()
    return {row[0]: [row[1], row[2]] for row in rows}

def extract_text_from_pdf(pdf_path):
    text = ""
    with fitz.open(pdf_path) as doc:
        for page in doc:
            text += page.get_text()
    new_text = text.replace('\n', ' ').replace('\r', ' ')
    text = re.sub(r'\d{1,2}\s[A-Za-z]{3,4}\s\d{4}\s\d{1,2}:\d{1,2}:\d{1,2}\s\d{1,3}\/\d{1,3}\sUser-generated version[A-Za-z ]+(?:\n(?:\d{1,3}\n)*)?', ' ', text).strip()
    return text, new_text

def clean_string(s):
    return re.sub(r'[^a-zA-Z0-9]', '', s.lower())

def clean_citation_name(name):
    match = re.search(r"(See for example|See generally|See|see|\sin)\s+(.*)", name, flags=re.IGNORECASE)
    if match:
        return match.group(2).strip()
    return name.strip()

def insert_main_paper(cur, meta):
    cur.execute("""
        INSERT OR REPLACE INTO main_paper
        (neutral_citation, name, jurisdiction, judge, judgment_date, reported_in, court, vlex_document_id, link)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (
        meta["neutral_citation"], meta["name"], meta["jurisdiction"], meta["judge"], meta["judgment_date"],
        meta["reported_in"], meta["court"], meta["vlex_document_id"], meta["link"]
    ))

def citation_exists(cur, neutral_citation, actual_citation):
    cur.execute("""
        SELECT citation FROM citations WHERE neutral_citation = ? COLLATE NOCASE
    """, (neutral_citation,))
    rows = cur.fetchall()
    flat_list = [clean_string(row[0]) for row in rows]
    return clean_string(actual_citation) in flat_list

def insert_citation(cur, meta, citation_name, actual_citation, reporter, jurisdiction, year):
    cur.execute("""
        INSERT OR REPLACE INTO citations (neutral_citation, citation_name, citation, reporter, jurisdiction, year)
        VALUES (?, ?, ?, ?, ?, ?)
    """, (meta["neutral_citation"], citation_name, actual_citation, reporter, jurisdiction, year))

def extract_reporter_and_year(actual_citation, yearReporterPattern, reporterJurisdictionDict):
    matchedYearReporter = re.search(yearReporterPattern, actual_citation)
    reporter = ""
    year = None
    reporterJurisdictionVal = ""
    if matchedYearReporter:
        reporter = matchedYearReporter.group("rptr1") or matchedYearReporter.group("rptr2")
        year = matchedYearReporter.group("year1") or matchedYearReporter.group("year2")
        if year:
            year = int(year)
        reporterJurisdictionVal = reporterJurisdictionDict.get(clean_string(reporter), ["", ""])[1]
        reporter = reporterJurisdictionDict.get(clean_string(reporter), ["", ""])[0]
    return reporter, reporterJurisdictionVal, year

def process_citations(cur, meta, text, new_text, citation_pattern, noNamePattern, yearReporterPattern, reporterJurisdictionDict):
    matches = re.finditer(citation_pattern, text)
    noNameMatches = re.finditer(noNamePattern, new_text)
    flat_list = []

    for citation in matches:
        citation_name = clean_citation_name(citation.group(1).replace('\n', ' ').strip())
        NameList = citation_name.split()[-10:]
        for i in range(len(NameList)):
            if NameList[i][0].isupper():
                citation_name = " ".join(NameList[i:])
                break
        actual_citation = citation.group(2).replace('\n', ' ').strip()
        if actual_citation == meta["neutral_citation"] or "Text" in citation_name or "Neutral Citation:" in citation_name or "Reported In:" in citation_name:
            continue
        for identifier in ["at", "dated", "paragraph", "AT"]:
            if identifier in actual_citation:
                actual_citation = actual_citation.split(identifier)[0].strip()
                break
        if citation_exists(cur, meta["neutral_citation"], actual_citation):
            continue
        reporter, reporterJurisdictionVal, year = extract_reporter_and_year(actual_citation, yearReporterPattern, reporterJurisdictionDict)
        insert_citation(cur, meta, citation_name, actual_citation, reporter, reporterJurisdictionVal, year)

    for citationN in noNameMatches:
        actual_citationN = citationN.group(1).replace('\n', ' ').strip()
        citation_nameN = ""
        if actual_citationN == meta["neutral_citation"] or "Text" in citation_nameN:
            continue
        for identifier in ["at", "dated", "paragraph", "AT"]:
            if identifier in actual_citationN:
                actual_citationN = actual_citationN.split(identifier)[0].strip()
                break
        if citation_exists(cur, meta["neutral_citation"], actual_citationN):
            continue
        reporter, reporterJurisdictionVal, year = extract_reporter_and_year(actual_citationN, yearReporterPattern, reporterJurisdictionDict)
        insert_citation(cur, meta, citation_nameN, actual_citationN, reporter, reporterJurisdictionVal, year)

def process_pdf_files(pdf_folder, db_path, reporterdbpath):
    conn = connect_db(db_path)
    cur = conn.cursor()
    create_tables(cur)
    reporterJurisdictionDict = reporter_jurisdiction_dict(reporterdbpath)

    for root, dirs, files in os.walk(pdf_folder):
        for filename in files:
            if filename.lower().endswith(".pdf"):
                pdf_path = os.path.join(root, filename)
                text, new_text = extract_text_from_pdf(pdf_path)
                meta = extract_metadata(text, new_text)
                if meta["neutral_citation"]:
                    insert_main_paper(cur, meta)
                    process_citations(
                        cur, meta, text, new_text,
                        CITATION_PATTERN, NO_NAME_PATTERN, YEAR_REPORTER_PATTERN, reporterJurisdictionDict
                    )
                    conn.commit()
    conn.close()

if __name__ == "__main__":
    process_pdf_files(PDF_FOLDER, DB_PATH, REPORTER_DB_PATH)