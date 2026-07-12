"""Environment diagnostic. Run:  python helloworld.py
Each stage prints BEFORE it does the work, so wherever it stops = the culprit."""
import sys
print("1. python running —", sys.version.split()[0], flush=True)
print("   executable:", sys.executable, flush=True)

print("2. importing dotenv…", flush=True)
from dotenv import load_dotenv
print("   ok", flush=True)

print("3. importing psycopg2…", flush=True)
import psycopg2
print("   ok", flush=True)

import os
print("4. loading .env…", flush=True)
load_dotenv()
url = os.getenv("DATABASE_URL")
print("   DATABASE_URL present:", bool(url), flush=True)

print("5. connecting (10s timeout)…", flush=True)
conn = psycopg2.connect(url, connect_timeout=10)
cur = conn.cursor()
cur.execute("SELECT 1")
print("   connected, SELECT 1 =", cur.fetchone()[0], flush=True)
cur.close(); conn.close()
print("ALL GOOD — environment can run the migration.", flush=True)
