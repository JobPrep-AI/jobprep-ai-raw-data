import snowflake.connector
from snowflake_config import SNOWFLAKE_CONFIG

def setup_snowflake_database():
    """
    Create database, schema, and table in Snowflake
    Run this ONCE to set up your environment
    """
    
    print("="*80)
    print("SNOWFLAKE SETUP - Creating Database and Tables")
    print("="*80)
    
    try:
        # Connect to Snowflake
        print("\n[1/5] Connecting to Snowflake...")
        
        # Build connection parameters dynamically
        conn_params = {
            'user': SNOWFLAKE_CONFIG['user'],
            'account': SNOWFLAKE_CONFIG['account'],
            'warehouse': SNOWFLAKE_CONFIG['warehouse']
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
        
        cursor = conn.cursor()
        print("✓ Connected successfully!")
        
        # Create database
        print("\n[2/5] Creating database...")
        cursor.execute(f"CREATE DATABASE IF NOT EXISTS {SNOWFLAKE_CONFIG['database']}")
        cursor.execute(f"USE DATABASE {SNOWFLAKE_CONFIG['database']}")
        print(f"✓ Database '{SNOWFLAKE_CONFIG['database']}' created/verified")
        
        # Create schema
        print("\n[3/5] Creating schema...")
        cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {SNOWFLAKE_CONFIG['schema']}")
        cursor.execute(f"USE SCHEMA {SNOWFLAKE_CONFIG['schema']}")
        print(f"✓ Schema '{SNOWFLAKE_CONFIG['schema']}' created/verified")
        
        # Create table
        print("\n[4/5] Creating table...")
        create_table_sql = """
        CREATE TABLE IF NOT EXISTS INTERVIEW_QUESTIONS (
            ID NUMBER AUTOINCREMENT PRIMARY KEY,
            COMPANY_NAME VARCHAR(255),
            ROLE_NAME VARCHAR(255),
            INTERVIEW_QUESTION TEXT,
            DIFFICULTY VARCHAR(50),
            QUESTION_URL TEXT,
            SOURCE VARCHAR(100),
            DATE_COLLECTED TIMESTAMP_NTZ,
            CREATED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
        )
        """
        cursor.execute(create_table_sql)
        print("✓ Table 'INTERVIEW_QUESTIONS' created/verified")
        
        # Verify setup
        print("\n[5/5] Verifying setup...")
        cursor.execute("SELECT COUNT(*) FROM INTERVIEW_QUESTIONS")
        count = cursor.fetchone()[0]
        print(f"✓ Current row count: {count}")
        
        print("\n" + "="*80)
        print("✅ SETUP COMPLETED SUCCESSFULLY!")
        print("="*80)
        print(f"\nYour Snowflake environment is ready:")
        print(f"  • Database: {SNOWFLAKE_CONFIG['database']}")
        print(f"  • Schema: {SNOWFLAKE_CONFIG['schema']}")
        print(f"  • Table: INTERVIEW_QUESTIONS")
        print(f"  • Current records: {count}")
        print("\nNext step: Run 'load_to_snowflake.py' to load your CSV data!")
        
        cursor.close()
        conn.close()
        
        return True
        
    except snowflake.connector.errors.ProgrammingError as e:
        print(f"\n✗ Error: {e}")
        print("\nTroubleshooting:")
        print("  1. Check your credentials in snowflake_config.py")
        print("  2. Verify your account identifier is correct")
        print("  3. Make sure your warehouse exists and is running")
        return False
        
    except Exception as e:
        print(f"\n✗ Unexpected error: {e}")
        import traceback
        traceback.print_exc()
        return False


if __name__ == "__main__":
    setup_snowflake_database()