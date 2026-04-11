"""
Add favorite_team column to users table.
Run once: python backend/migrate_favorite_team.py
"""
import os, sys
from dotenv import load_dotenv
import psycopg2

load_dotenv()
DATABASE_URL = os.getenv("DATABASE_URL")
if not DATABASE_URL:
    print("DATABASE_URL not found."); sys.exit(1)

conn = psycopg2.connect(DATABASE_URL)
cur  = conn.cursor()
cur.execute("""
    ALTER TABLE users
    ADD COLUMN IF NOT EXISTS favorite_team TEXT DEFAULT NULL
""")
conn.commit()
cur.close(); conn.close()
print("Done — favorite_team column added to users.")
