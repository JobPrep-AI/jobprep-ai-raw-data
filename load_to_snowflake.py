import snowflake.connector
import pandas as pd
from snowflake_config import SNOWFLAKE_CONFIG
import glob
from datetime import datetime
import os

def get_latest_csv():
    """Find the most recent interview CSV file (any source)"""
    
    csv_patterns = [
        'tryexponent_*.csv',
        'gfg_companywise_*.csv',
        'geeksforgeeks_*.csv',
        'interviewbit_*.csv',
        'github_*.csv',
        'reddit_*.csv',
        'MASTER_*.csv'
    ]
    
    all_csv_files = []
    for pattern in csv_patterns:
        all_csv_files.extend(glob.glob(pattern))
    
    if not all_csv_files:
        print("✗ No CSV files found!")
        return None
    
    # Show all found files
    print(f"Found {len(all_csv_files)} CSV file(s):")
    for i, f in enumerate(all_csv_files, 1):
        print(f"  {i}. {f}")
    
    # Get the most recently modified file
    latest_file = max(all_csv_files, key=os.path.getmtime)
    print(f"\n✓ Most recent file: {latest_file}")
    
    # Ask user to confirm or choose different file
    choice = input("\nUse this file? (Y/n) or enter file number: ").strip().lower()
    if choice == 'n':
        file_num = input("Enter file number to use: ").strip()
        try:
            latest_file = all_csv_files[int(file_num) - 1]
            print(f"✓ Using: {latest_file}")
        except:
            print("✗ Invalid selection, using most recent")
    
    return latest_file


def load_csv_to_snowflake(csv_file=None):
    """Load interview CSV data into Snowflake"""
    
    print("="*80)
    print("LOADING DATA TO SNOWFLAKE")
    print("="*80)
    
    if csv_file is None:
        csv_file = get_latest_csv()
        if csv_file is None:
            return False
    
    print(f"\n[1/6] Reading CSV file: {csv_file}")
    try:
        df = pd.read_csv(csv_file)
        now_str = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        
        # Replace NaNs with defaults
        df = df.fillna({
            'company_name': 'Unknown',
            'role_name': 'Software Engineer',
            'interview_question': '',
            'difficulty': 'Not Specified',
            'question_url': '',
            'source': 'Unknown',
            'date_collected': now_str
        })
        
        df.drop_duplicates(subset=['company_name','interview_question'], inplace=True)
        
        print(f"✓ Loaded {len(df)} records from CSV")
        print("\nPreview of data:")
        print(df.head(3).to_string())
        
    except Exception as e:
        print(f"✗ Error reading CSV: {e}")
        return False
    
    # Connect to Snowflake
    print("\n[2/6] Connecting to Snowflake...")
    try:
        conn_params = {
            'user': SNOWFLAKE_CONFIG['user'],
            'account': SNOWFLAKE_CONFIG['account'],
            'warehouse': SNOWFLAKE_CONFIG['warehouse'],
            'database': SNOWFLAKE_CONFIG['database'],
            'schema': SNOWFLAKE_CONFIG['schema']
        }
        if 'password' in SNOWFLAKE_CONFIG:
            conn_params['password'] = SNOWFLAKE_CONFIG['password']
            print("\n⚠️  MFA Required!")
            mfa_code = input("Enter your MFA/TOTP code: ").strip()
            if mfa_code:
                conn_params['passcode'] = mfa_code
        if 'authenticator' in SNOWFLAKE_CONFIG:
            conn_params['authenticator'] = SNOWFLAKE_CONFIG['authenticator']
        
        conn = snowflake.connector.connect(**conn_params)
        cursor = conn.cursor()
        print("✓ Connected successfully!")
        
        # Check current record count
        print("\n[3/6] Checking current data...")
        cursor.execute("SELECT COUNT(*) FROM INTERVIEW_QUESTIONS")
        before_count = cursor.fetchone()[0]
        print(f"✓ Current records in Snowflake: {before_count}")
        
        # Prepare data for insertion
        print("\n[4/6] Preparing data for insertion...")
        records = [
            (
                row.get('company_name', 'Unknown'),
                row.get('role_name', 'Software Engineer'),
                row.get('interview_question', ''),
                row.get('difficulty', 'Not Specified'),
                row.get('question_url', ''),
                row.get('source', 'Unknown'),
                row.get('date_collected', now_str)
            )
            for _, row in df.iterrows()
        ]
        print(f"✓ Prepared {len(records)} records for insertion")
        
        # Step 5: Insert using MERGE to handle duplicates
        print("\n[5/6] Inserting data into Snowflake (duplicates handled automatically)...")
        merge_sql = """
        MERGE INTO INTERVIEW_QUESTIONS tgt
        USING VALUES (%s, %s, %s, %s, %s, %s, %s) AS src(
            company_name, role_name, interview_question, difficulty, question_url, source, date_collected
        )
        ON tgt.company_name = src.company_name
           AND tgt.interview_question = src.interview_question
        WHEN NOT MATCHED THEN
          INSERT (company_name, role_name, interview_question, difficulty, question_url, source, date_collected)
          VALUES (src.company_name, src.role_name, src.interview_question, src.difficulty, src.question_url, src.source, src.date_collected);
        """
        cursor.executemany(merge_sql, records)
        conn.commit()
        print("✅ All records inserted successfully (duplicates automatically skipped)")
        
        # Verify insertion
        print("\n[6/6] Verifying insertion...")
        cursor.execute("SELECT COUNT(*) FROM INTERVIEW_QUESTIONS")
        after_count = cursor.fetchone()[0]
        inserted_count = after_count - before_count
        print(f"✓ Records before: {before_count}")
        print(f"✓ Records after: {after_count}")
        print(f"✓ New records inserted: {inserted_count}")
        
        # Show sample data
        print("\n" + "="*80)
        print("SAMPLE DATA FROM SNOWFLAKE:")
        print("="*80)
        cursor.execute("SELECT COMPANY_NAME, DIFFICULTY, INTERVIEW_QUESTION FROM INTERVIEW_QUESTIONS LIMIT 5")
        for row in cursor.fetchall():
            print(f"\n{row[0]} [{row[1]}]")
            print(f"  {row[2]}")
        
        # Show summary by company
        print("\n" + "="*80)
        print("SUMMARY BY COMPANY:")
        print("="*80)
        cursor.execute("""
            SELECT COMPANY_NAME, COUNT(*) as QUESTION_COUNT
            FROM INTERVIEW_QUESTIONS
            GROUP BY COMPANY_NAME
            ORDER BY QUESTION_COUNT DESC
        """)
        for row in cursor.fetchall():
            print(f"{row[0]:<30} {row[1]:>5} questions")
        
        print("\n" + "="*80)
        print("✅ DATA LOADED SUCCESSFULLY!")
        print("="*80)
        
        cursor.close()
        conn.close()
        return True
    
    except Exception as e:
        print(f"\n✗ Error: {e}")
        import traceback
        traceback.print_exc()
        return False


if __name__ == "__main__":
    load_csv_to_snowflake()