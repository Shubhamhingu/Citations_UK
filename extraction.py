import re
import os
import fitz
import sqlite3
from datetime import datetime

# Database setup
pdf_folder = "Jersey"
conn = sqlite3.connect(pdf_folder + ".db")
reporterdbpath = "Reporters.db"
cur = conn.cursor()
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
conn.commit()

def parse_judgment_date(date_str):
    # Normalize month abbreviations and remove extra spaces
    months = {
        'Jan': 'January', 'Feb': 'February', 'Mar': 'March', 'Apr': 'April',
        'May': 'May', 'Jun': 'June', 'Jul': 'July', 'Aug': 'August',
        'Sep': 'September', 'Oct': 'October', 'Nov': 'November', 'Dec': 'December'
    }
    # Replace abbreviations with full names
    for abbr, full in months.items():
        date_str = re.sub(rf"\b{abbr}\b", full, date_str, flags=re.IGNORECASE)
    date_str = date_str.strip()
    # Try parsing with common formats
    for fmt in ("%d %B %Y", "%d %b %Y", "%d %B, %Y", "%d %b, %Y"):
        try:
            return datetime.strptime(date_str, fmt).date().isoformat()
        except ValueError:
            continue
    # If parsing fails, return original string
    return date_str

def extract_case_name(text):
    """
    Extracts all text between the copyright notice and the 'Jurisdiction:' metadata field.
    """
    pattern = r"Otherwise, distribution or reproduction is not permitted(.*)\sJurisdiction:"
    match = re.search(pattern, text, re.DOTALL)
    if match:
        case_name = match.group(1).strip()
        # Remove any trailing punctuation or whitespace
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
    meta = {}
    meta["name"] = extract_case_name(new_text)
    for key, pat in patterns.items():
        m = re.search(pat, text, re.IGNORECASE)
        value = None
        if m:
            # Loop through all groups and pick the first non-empty value
            for group in m.groups():
                if group and group.strip():
                    value = group.strip()
                    break
        if key == "judgment_date" and value:
            value = parse_judgment_date(value)
        meta[key] = value
    return meta


def reporterJurisdiction(reporterdbpath):
    connRep = sqlite3.connect(reporterdbpath)
    curRep = connRep.cursor()
    curRep.execute("""
                   SELECT Reporter_cleaned, Reporter, Jurisdiction FROM jersey_reporters
                   """)
    rows = curRep.fetchall()
    reporterJurisdictionDict = {}
    for row in rows:
        reporterJurisdictionDict[row[0]] = [row[1], row[2]]
    return reporterJurisdictionDict

def extract_text_from_pdf(pdf_path):
    text = ""
    with fitz.open(pdf_path) as doc:
        for page in doc:
            text += page.get_text()
    new_text = text.replace('\n', ' ').replace('\r', ' ')  # Normalize line breaks
    text = re.sub(r'\d{1,2}\s[A-Za-z]{3,4}\s\d{4}\s\d{1,2}:\d{1,2}:\d{1,2}\s\d{1,3}\/\d{1,3}\sUser-generated version[A-Za-z ]+(?:\n(?:\d{1,3}\n)*)?', ' ', text).strip()  # Remove extra spaces
    return text, new_text


def clean_string(s):
    """
    Removes all special characters and spaces from the string,
    leaving only lowercase letters and numbers.
    """
    return re.sub(r'[^a-zA-Z0-9]', '', s.lower())

def clean_citation_name(name):
    """
    Removes leading phrases like 'See', 'See for example', 'See generally' from citation names.
    """
    match = re.search(r"(See for example|See generally|See|see|\sin)\s+(.*)", name, flags=re.IGNORECASE)
    if match:
        return match.group(2).strip()
    return name.strip()

reporterJurisdictionDict = reporterJurisdiction(reporterdbpath)


citation_pattern = (
    r"((?:[A-Z][A-Za-z0-9.,'’()\- &:]+v\.? [A-Z][A-Za-z0-9.,'’()\- &:]+|"
    # r"In (?:the )?matter of [A-Za-z0-9.,'’()\- &:]+|"
    # r"re [A-Za-z0-9.,'’()\- &:]+|"
    # r"[A-Z][A-Za-z0-9.,'’()\- &:]+(?:Law|Rules|Act|case|Applications|Note)[^.\n]*|"
    r"[A-Z][A-Za-z0-9.,'’()\- &:]+))"
    r"\s*(\[\d{4}\](?:\s\(\d{1,4}\))?\s*\d{0,4}\s*[A-Za-z. ]+[ ]+\s*\d{1,4}(?:\s[A-Za-z ]+\s\d{1,4})?|\(\d{4}\)(?:\s\(\d{1,4}\))?\s*\d{0,4}\s*[A-Za-z.]+[ ]+\s*\d{1,4}(?:\s[A-Za-z]+\s\d{1,4})?)"
)

noNamePattern = r"(\[\d{4}\]\s*\d{0,4}\s*[A-Za-z.]+[ ]+\s*\d{1,4}(?:\s[A-Za-z]+\s\d{1,4})?|\(\d{4}\)\s*\d{0,4}\s*[A-Za-z.]+[ ]+\s*\d{1,4}(?:\s[A-Za-z]+\s\d{1,4})?)"
yearReporterPattern = r"\[(?P<year1>\d{4})\]\s*\d*\s(?P<rptr1>[A-Za-z. ]+)\s\d{1,4}|\((?P<year2>\d{4})\)\s*\d*\s(?P<rptr2>[A-Za-z. ]+)\s\d{1,4}"

for root, dirs, files in os.walk(pdf_folder):
    for filename in files:
        if filename.lower().endswith(".pdf"):
            pdf_path = os.path.join(root, filename)
            text, new_text = extract_text_from_pdf(pdf_path)
            meta = extract_metadata(text, new_text)
            flat_list = list()
            # Insert metadata if Neutral Citation exists
            if meta["neutral_citation"]:
                cur.execute("""
                    INSERT OR REPLACE INTO main_paper
                    (neutral_citation, name, jurisdiction, judge, judgment_date, reported_in, court, vlex_document_id, link)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    meta["neutral_citation"], meta["name"], meta["jurisdiction"], meta["judge"], meta["judgment_date"],
                    meta["reported_in"], meta["court"], meta["vlex_document_id"], meta["link"]
                ))
                conn.commit()
                matches = re.finditer(citation_pattern, text)
                noNameMatches = re.finditer(noNamePattern, new_text)
                for citation in matches:
                    citation_name = clean_citation_name(citation.group(1).replace('\n', ' ').strip())
                    NameList = citation_name.split()[-10:]
                    for i in range(len(NameList)):
                        if NameList[i][0].isupper():
                            citation_name = " ".join(NameList[i:])
                            break
                    actual_citation = citation.group(2).replace('\n', ' ').strip()  # Only the citation code
                    if actual_citation == meta["neutral_citation"] or "Text" in citation_name or "Neutral Citation:" in citation_name or "Reported In:" in citation_name:
                        # If the citation matches the neutral citation, skip it
                        continue
                    for identifier in ["at", "dated", "paragraph", "AT"]:
                        if identifier in actual_citation:
                            actual_citation = actual_citation.split(identifier)[0].strip()
                            break
                    cur.execute("""
                        SELECT citation FROM citations WHERE neutral_citation = ? COLLATE NOCASE
                    """, (meta["neutral_citation"],))
                    rows = cur.fetchall()
                    flat_list = [clean_string(row[0]) for row in rows]

                    if rows:
                        if clean_string(actual_citation) in flat_list:
                            continue
                    matchedYearReporter = re.search(yearReporterPattern, actual_citation)
                    if matchedYearReporter:
                        reporter = matchedYearReporter.group("rptr1") or matchedYearReporter.group("rptr2")
                        year = matchedYearReporter.group("year1") or matchedYearReporter.group("year2")
                    if year:
                            year = int(year)
                    reporterJurisdictionVal = reporterJurisdictionDict.get(clean_string(reporter), ["", ""])[1]
                    reporter = reporterJurisdictionDict.get(clean_string(reporter), ["", ""])[0]
                    cur.execute("""
                        INSERT OR REPLACE INTO citations (neutral_citation, citation_name, citation, reporter, jurisdiction, year)
                        VALUES (?, ?, ?, ?, ?, ?)
                    """, (meta["neutral_citation"], citation_name, actual_citation, reporter, reporterJurisdictionVal, year))
                for citationN in noNameMatches:
                    actual_citationN = citationN.group(1).replace('\n', ' ').strip()
                    if flat_list and actual_citationN in flat_list:
                        continue
                    citation_nameN = ""
                    if actual_citationN == meta["neutral_citation"] or "Text" in citation_nameN:
                        # If the citation matches the neutral citation, skip it
                        continue
                    for identifier in ["at", "dated", "paragraph", "AT"]:
                        if identifier in actual_citationN:
                            actual_citationN = actual_citationN.split(identifier)[0].strip()
                            break
                    cur.execute("""
                        SELECT citation FROM citations WHERE neutral_citation = ? COLLATE NOCASE
                    """, (meta["neutral_citation"],))
                    rows = cur.fetchall()
                    flat_list = [clean_string(row[0]) for row in rows]
                    if rows:
                        if clean_string(actual_citationN) in flat_list:
                            continue
                    matchedYearReporter = re.search(yearReporterPattern, actual_citationN)
                    if matchedYearReporter:
                        reporter = matchedYearReporter.group("rptr1") or matchedYearReporter.group("rptr2")
                        year = matchedYearReporter.group("year1") or matchedYearReporter.group("year2")
                        # Keep only the year as an integer (not a string or full date)
                        if year:
                            year = int(year)
                        reporterJurisdictionVal = reporterJurisdictionDict.get(clean_string(reporter), ["", ""])[1]
                        reporter = reporterJurisdictionDict.get(clean_string(reporter), ["", ""])[0]
                    cur.execute("""
                        INSERT OR REPLACE INTO citations (neutral_citation, citation_name, citation, reporter, jurisdiction, year)
                        VALUES (?, ?, ?, ?, ?, ?)
                    """, (meta["neutral_citation"], citation_nameN, actual_citationN, reporter, reporterJurisdictionVal, year))
                conn.commit()
conn.close()