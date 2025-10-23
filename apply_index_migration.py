#!/usr/bin/env python3
"""
Apply index migration: create md5 expression index concurrently, drop old index, rename new index.
Run from project root with Python that has psycopg2 installed.
"""
import psycopg2
import sys

db_config = {
    'dbname': 'trademarks',
    'user': 'postgres',
    'password': '1234',
    'host': 'localhost',
    'port': '5432'
}

conn = None
try:
    conn = psycopg2.connect(**db_config)
    conn.autocommit = True
    cur = conn.cursor()

    print('Creating concurrent MD5 index (may take time)...')
    cur.execute("CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_trademark_case_files_mark_id_md5 ON trademark_case_files (md5(mark_identification));")
    print('Index creation command issued.')

    print('Dropping old index if it exists...')
    cur.execute("DROP INDEX IF EXISTS idx_trademark_case_files_mark_id;")
    print('Old index dropped (if it existed).')

    try:
        print('Renaming new index to original name...')
        cur.execute("ALTER INDEX idx_trademark_case_files_mark_id_md5 RENAME TO idx_trademark_case_files_mark_id;")
        print('Index renamed successfully.')
    except Exception as e:
        print('Rename failed or not necessary:', e)

    cur.close()
    print('Migration finished successfully.')

except Exception as e:
    print('Migration failed:', e)
    sys.exit(1)
finally:
    if conn:
        conn.close()
