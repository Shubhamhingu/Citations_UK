import pandas as pd
import sqlite3
import string

# Step 1: Load Excel file
df = pd.read_excel("Jersey_reporters.xlsx")

# Step 2: Clean and fill missing data
df["Need to Check"] = df["Need to Check"].fillna("UNKNOWN").astype(str).str.strip()
df["Jurisdiction"] = df["Jurisdiction"].fillna("UNKNOWN").astype(str).str.strip()
df["Reporter"] = df["Reporter"].fillna("").astype(str)

# Step 3: Clean reporter (same as SQL LOWER(REPLACE...))
def clean_reporter(text):
    return ''.join(c.strip() for c in text.lower() if c not in [',','.',':',';','|','(',')','[',']'] and not c.isspace())

df["Reporter_cleaned"] = df["Reporter"].apply(clean_reporter)

# Step 4: Group by (Reporter, Jurisdiction, NeedToCheck), sum Count
df["Count"] = pd.to_numeric(df["Count"], errors="coerce").fillna(0).astype(int)

print(f"Total unique reporters: {df['Reporter_cleaned'].nunique()}")

df_max = (
    df.groupby("Reporter_cleaned", group_keys=False)
    .apply(lambda g: g.loc[g["Count"] == g["Count"].max()].iloc[0])
    .reset_index(drop=True)
)


secTable = df.groupby(["Reporter_cleaned"]).agg({"Count": "sum"}).reset_index()

result = pd.merge(df_max, secTable, on=["Reporter_cleaned"], how="left")
conn = sqlite3.connect("Reporters.db")
cur = conn.cursor()

# Step 8: Create table if not exists
cur.execute("""
CREATE TABLE IF NOT EXISTS jersey_reporters (
    Reporter TEXT,
    Reporter_cleaned TEXT,
    Count INTEGER,
    Jurisdiction TEXT,
    NeedToCheck TEXT,
    PRIMARY KEY (Reporter, Jurisdiction, NeedToCheck)
)
""")

# Step 9: Insert the data
for _, row in result.iterrows():
    cur.execute("""
        INSERT OR REPLACE INTO jersey_reporters (
            Reporter, Reporter_cleaned, Count, Jurisdiction, NeedToCheck
        ) VALUES (?, ?, ?, ?, ?)
    """, (
        row["Reporter"],
        row["Reporter_cleaned"],
        int(row["Count_y"]),
        row["Jurisdiction"],
        row["Need to Check"]
    ))

conn.commit()
conn.close()

print(f"Inserted {len(result)} rows into jersey_reporters.")
