import snowflake.connector
import pandas as pd
from snowflake_config import SNOWFLAKE_CONFIG

def query_snowflake_data():
    """
    Query and display data from Snowflake
    """
    
    print("="*80)
    print("QUERYING SNOWFLAKE DATA")
    print("="*80)
    
    try:
        # Connect
        print("\nConnecting to Snowflake...")
        
        # Build connection parameters dynamically
        conn_params = {
            'user': SNOWFLAKE_CONFIG['user'],
            'account': SNOWFLAKE_CONFIG['account'],
            'warehouse': SNOWFLAKE_CONFIG['warehouse'],
            'database': SNOWFLAKE_CONFIG['database'],
            'schema': SNOWFLAKE_CONFIG['schema']
        }
        
        # Add password if it exists
        if 'password' in SNOWFLAKE_CONFIG:
            conn_params['password'] = SNOWFLAKE_CONFIG['password']
            
            # Ask for MFA code if password authentication is being used
            print("\n⚠️  MFA Required!")
            mfa_code = input("Enter your MFA/TOTP code from your authenticator app: ").strip()
            if mfa_code:
                conn_params['passcode'] = mfa_code
        
        # Add authenticator if it exists (for externalbrowser)
        if 'authenticator' in SNOWFLAKE_CONFIG:
            conn_params['authenticator'] = SNOWFLAKE_CONFIG['authenticator']
        
        conn = snowflake.connector.connect(**conn_params)
        
        print("✓ Connected!")
        
        # Total count
        print("\n" + "="*80)
        print("TOTAL RECORDS")
        print("="*80)
        
        df = pd.read_sql("SELECT COUNT(*) as TOTAL FROM INTERVIEW_QUESTIONS", conn)
        print(f"Total questions in database: {df['TOTAL'][0]}")
        
        # By company
        print("\n" + "="*80)
        print("QUESTIONS BY COMPANY")
        print("="*80)
        
        df = pd.read_sql("""
            SELECT COMPANY_NAME, COUNT(*) as COUNT
            FROM INTERVIEW_QUESTIONS
            GROUP BY COMPANY_NAME
            ORDER BY COUNT DESC
        """, conn)
        print(df.to_string(index=False))
        
        # By difficulty
        print("\n" + "="*80)
        print("QUESTIONS BY DIFFICULTY")
        print("="*80)
        
        df = pd.read_sql("""
            SELECT DIFFICULTY, COUNT(*) as COUNT
            FROM INTERVIEW_QUESTIONS
            GROUP BY DIFFICULTY
            ORDER BY COUNT DESC
        """, conn)
        print(df.to_string(index=False))
        
        # Sample questions
        print("\n" + "="*80)
        print("SAMPLE QUESTIONS (Random 10)")
        print("="*80)
        
        df = pd.read_sql("""
            SELECT COMPANY_NAME, DIFFICULTY, INTERVIEW_QUESTION
            FROM INTERVIEW_QUESTIONS
            SAMPLE (10 ROWS)
        """, conn)
        
        for idx, row in df.iterrows():
            print(f"\n{row['COMPANY_NAME']} [{row['DIFFICULTY']}]:")
            print(f"  {row['INTERVIEW_QUESTION']}")
        
        # Search by company
        print("\n" + "="*80)
        company = input("Enter company name to see questions (or press Enter to skip): ").strip()
        
        if company:
            df = pd.read_sql(f"""
                SELECT DIFFICULTY, INTERVIEW_QUESTION
                FROM INTERVIEW_QUESTIONS
                WHERE UPPER(COMPANY_NAME) LIKE UPPER('%{company}%')
                LIMIT 20
            """, conn)
            
            if len(df) > 0:
                print(f"\n{company} Questions:")
                print("="*80)
                for idx, row in df.iterrows():
                    print(f"\n[{row['DIFFICULTY']}] {row['INTERVIEW_QUESTION']}")
            else:
                print(f"\nNo questions found for {company}")
        
        conn.close()
        
    except Exception as e:
        print(f"\n✗ Error: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    query_snowflake_data()