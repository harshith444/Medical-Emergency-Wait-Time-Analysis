import requests
import psycopg2
from env import DB_CONFIG
conn = psycopg2.connect(**DB_CONFIG)
cur = conn.cursor()
print("Connected to the database successfully.")
url = "https://data.cms.gov/provider-data/api/1/datastore/sql"
headers = {'accept': 'application/json'}
offset = 0
limit = 500
total_rows = 0
while True:
    params = {
        "query": f"[SELECT * FROM c3c546a5-422b-5b19-a46e-866ea2355fd7][LIMIT {limit} OFFSET {offset}]",
        "show_db_columns": "true"
    }
    response = requests.get(url, headers=headers, params=params)
    if response.status_code != 200:
        print("Error:", response.status_code, response.text)
        break
    data = response.json()
    if not data:
        break
    for idx, record in enumerate(data):
        record_number = offset + idx + 1
        db_record = {
            'record_number': record_number,
            'facility_id': record.get('facility_id'),
            'facility_name': record.get('facility_name'),
            'address': record.get('address'),
            'citytown': record.get('citytown'),
            'state': record.get('state'),
            'zip_code': record.get('zip_code'),
            'countyparish': record.get('countyparish'),
            'telephone_number': record.get('telephone_number'),
            'condition': record.get('condition'),
            'measure_id': record.get('measure_id'),
            'measure_name': record.get('measure_name'),
            'score': record.get('score'),
            'sample': record.get('sample'),
            'footnote': record.get('footnote'),
            'start_date': record.get('start_date'),
            'end_date': record.get('end_date')
        }
        try:
            cur.execute("""
                INSERT INTO hospital_measures (
                    record_number, facility_id, facility_name, address, citytown, state,
                    zip_code, countyparish, telephone_number, condition, measure_id,
                    measure_name, score, sample, footnote, start_date, end_date
                ) VALUES (
                    %(record_number)s, %(facility_id)s, %(facility_name)s, %(address)s, %(citytown)s, %(state)s,
                    %(zip_code)s, %(countyparish)s, %(telephone_number)s, %(condition)s, %(measure_id)s,
                    %(measure_name)s, %(score)s, %(sample)s, %(footnote)s, %(start_date)s, %(end_date)s
                ) ON CONFLICT (record_number) DO NOTHING;
            """, db_record)
            total_rows += 1
        except Exception as e:
            print("Insert error:", e)
            continue
    conn.commit()
    offset += limit
    print("Total number of rows inserted so far:", total_rows)
# cur.execute("SELECT COUNT(*) FROM hospital_measures;")
# count = cur.fetchone()[0]
# print(f"Total number of rows in the table: {count}")
# Create normalized tables if not exist
cur.execute("""
    CREATE TABLE IF NOT EXISTS hospitals (
        id serial PRIMARY KEY,
        facility_id varchar(255) UNIQUE,
        facility_name varchar(255),
        address varchar(255),
        citytown varchar(100),
        state varchar(50),
        zip_code varchar(20),
        countyparish varchar(100),
        telephone_number varchar(20)
    );
""")
cur.execute("""
    CREATE TABLE IF NOT EXISTS measure_metadata (
        id serial PRIMARY KEY,
        measure_id varchar(50) UNIQUE,
        measure_name varchar(255),
        condition varchar(100)
    );
""")
cur.execute("""
    CREATE TABLE IF NOT EXISTS performance_measures (
        record_number int PRIMARY KEY,
        facility_id varchar(255),
        measure_id varchar(50),
        score text,
        sample text,
        footnote text,
        start_date date,
        end_date date
    );
""")
cur.execute("""
    INSERT INTO hospitals (facility_id, facility_name, address, citytown, state, zip_code, countyparish, telephone_number)
    SELECT DISTINCT facility_id, facility_name, address, citytown, state, zip_code, countyparish, telephone_number
    FROM hospital_measures
    ON CONFLICT (facility_id) DO NOTHING;
""")
cur.execute("""
    INSERT INTO measure_metadata (measure_id, measure_name, condition)
    SELECT DISTINCT measure_id, measure_name, condition
    FROM hospital_measures
    ON CONFLICT (measure_id) DO NOTHING;
""")
cur.execute("""
    INSERT INTO performance_measures (record_number, facility_id, measure_id, score, sample, footnote, start_date, end_date)
    SELECT record_number, facility_id, measure_id, score::text, sample::text, footnote, start_date, end_date
    FROM hospital_measures
    ON CONFLICT (record_number) DO NOTHING;
""")
conn.commit()
# cur.execute("SELECT COUNT(*) FROM hospitals;")
# count_hospitals = cur.fetchone()[0]
# print(f"Total number of hospitals: {count_hospitals}")
# cur.execute("SELECT COUNT(*) FROM measure_metadata;")
# count_metadata = cur.fetchone()[0]
# print(f"Total number of measure metadata in the table: {count_metadata}")
# cur.execute("SELECT COUNT(*) FROM performance_measures;")
# count_performance = cur.fetchone()[0]
# print(f"Total number of performance measures in the table: {count_performance}")
cur.close()
conn.close()

