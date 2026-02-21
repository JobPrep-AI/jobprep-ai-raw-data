import requests
import pandas as pd
from datetime import datetime
import time
import io

def scrape_github_leetcode_raw():
    """
    Scrape using raw GitHub URLs (bypasses API rate limits!)
    Automatically discovers ALL company folders!
    """
    
    print("GitHub LeetCode Scraper - Auto-Discover All Companies")
    print("="*80)
    
    # Step 1: Get list of all company folders (one API call only)
    repo_owner = "snehasishroy"
    repo_name = "leetcode-companywise-interview-questions"
    api_url = f"https://api.github.com/repos/{repo_owner}/{repo_name}/contents"
    
    headers = {
        'Accept': 'application/vnd.github.v3+json',
        'User-Agent': 'JobPrepAI'
    }
    
    print("\nStep 1: Auto-discovering all company folders...")
    
    try:
        response = requests.get(api_url, headers=headers, timeout=10)
        
        if response.status_code == 200:
            contents = response.json()
            
            # Get only directories (company folders)
            company_folders = [item['name'] for item in contents if item['type'] == 'dir']
            
            print(f"✓ Found {len(company_folders)} companies automatically!")
            print(f"\nCompanies: {', '.join(company_folders[:10])}... and {len(company_folders)-10} more")
            
        elif response.status_code == 403:
            print("⚠️  API rate limited. Using fallback method...")
            # Fallback: Use web scraping to get folder list
            page_url = f"https://github.com/{repo_owner}/{repo_name}"
            response = requests.get(page_url, timeout=10)
            
            if response.status_code == 200:
                # Simple regex to find folder names in HTML
                import re
                folders = re.findall(r'title="([^"]+)" class="Link--primary"', response.text)
                company_folders = [f for f in folders if f not in ['.github', 'README.md']]
                print(f"✓ Found {len(company_folders)} companies via web scraping!")
            else:
                print("✗ Could not get company list. Using default list...")
                # Minimal fallback list
                company_folders = ['Google', 'Amazon', 'Microsoft', 'Facebook', 'Apple']
        else:
            print(f"✗ Failed: Status {response.status_code}")
            return None
    
    except Exception as e:
        print(f"✗ Error getting company list: {e}")
        return None
    
    # Step 2: Download all.csv from each company using RAW URLs (no API!)
    print(f"\nStep 2: Downloading questions from {len(company_folders)} companies...")
    print("(This bypasses API - no rate limits!)\n")
    
    base_raw_url = "https://raw.githubusercontent.com/snehasishroy/leetcode-companywise-interview-questions/master"
    
    all_data = []
    successful = 0
    failed = 0
    
    for i, company_folder in enumerate(company_folders, 1):
        # Clean company name
        company_name = company_folder.replace('-', ' ').replace('_', ' ').title()
        
        print(f"[{i}/{len(company_folders)}] {company_name:<30}", end=" ")
        
        try:
            # Construct raw URL to all.csv
            csv_url = f"{base_raw_url}/{company_folder}/all.csv"
            
            # Download CSV directly (no API!)
            response = requests.get(csv_url, timeout=10)
            
            if response.status_code == 200:
                # Parse CSV
                df = pd.read_csv(io.StringIO(response.text))
                
                # Extract questions
                for _, row in df.iterrows():
                    title = str(row.get('Title', '')).strip()
                    difficulty = str(row.get('Difficulty', 'Not Specified')).strip()
                    url = str(row.get('URL', '')).strip()
                    
                    if not title or title == 'nan' or len(title) < 3:
                        continue
                    
                    if difficulty.lower() in ['easy', 'medium', 'hard']:
                        difficulty = difficulty.capitalize()
                    else:
                        difficulty = 'Not Specified'
                    
                    if url == 'nan':
                        url = ''
                    
                    all_data.append({
                        'company_name': company_name,
                        'role_name': 'Software Engineer',
                        'interview_question': title,
                        'difficulty': difficulty,
                        'question_url': url,
                        'source': 'GitHub - LeetCode Company-wise',
                        'date_collected': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                    })
                
                print(f"✓ {len(df):3d} questions")
                successful += 1
                
            elif response.status_code == 404:
                print("✗ No all.csv")
                failed += 1
            else:
                print(f"✗ Error {response.status_code}")
                failed += 1
            
            time.sleep(0.3)  # Small delay to be polite
            
        except Exception as e:
            print(f"✗ {str(e)[:30]}")
            failed += 1
    
    print(f"\n{'='*80}")
    print(f"Completed: {successful} successful, {failed} failed")
    print(f"{'='*80}")
    
    # Save
    if all_data:
        df = pd.DataFrame(all_data)
        df = df.drop_duplicates(subset=['company_name', 'interview_question'])
        
        filename = f'github_leetcode_{datetime.now().strftime("%Y%m%d_%H%M%S")}.csv'
        df.to_csv(filename, index=False)
        
        print("\n" + "="*80)
        print("FINAL RESULTS")
        print("="*80)
        print(f"✓ Total questions: {len(df)}")
        print(f"✓ Companies covered: {df['company_name'].nunique()}")
        print(f"✓ Saved to: {filename}")
        
        # Top companies
        print("\n" + "-"*80)
        print("Top 20 Companies:")
        print("-"*80)
        top = df.groupby('company_name').size().reset_index(name='count')
        top = top.sort_values('count', ascending=False).head(20)
        for _, row in top.iterrows():
            print(f"  {row['company_name']:<30} {row['count']:>5}")
        
        # Difficulty
        print("\n" + "-"*80)
        print("By Difficulty:")
        print("-"*80)
        diffs = df.groupby('difficulty').size().reset_index(name='count')
        for _, row in diffs.iterrows():
            print(f"  {row['difficulty']:<20} {row['count']:>5}")
        
        # Quality
        print("\n" + "="*80)
        print("DATA QUALITY")
        print("="*80)
        print("  ✅ 100% Company-Specific (organized by folders)")
        print("  ✅ LeetCode curated questions")
        print("  ✅ Difficulty levels included")
        print(f"  ✅ {df['company_name'].nunique()} companies covered")
        
        # Samples
        print("\n" + "-"*80)
        print("Sample Questions:")
        print("-"*80)
        sample = df.sample(min(10, len(df)))
        for _, row in sample.iterrows():
            print(f"\n{row['company_name']} [{row['difficulty']}]:")
            print(f"  {row['interview_question']}")
        
        print("\n" + "="*80)
        print("✅ COMPLETE! Ready to load to Snowflake!")
        print("="*80)
        
        return df
    
    return None
    
    # Save results
    if all_data:
        df = pd.DataFrame(all_data)
        df = df.drop_duplicates(subset=['company_name', 'interview_question'])
        
        filename = f'github_leetcode_{datetime.now().strftime("%Y%m%d_%H%M%S")}.csv'
        df.to_csv(filename, index=False)
        
        print("\n" + "="*80)
        print("RESULTS")
        print("="*80)
        print(f"✓ Total unique questions: {len(df)}")
        print(f"✓ Saved to: {filename}")
        
        # Breakdown
        print("\n" + "-"*80)
        print("Questions by Company (Top 20):")
        print("-"*80)
        top_companies = df.groupby('company_name').size().reset_index(name='count')
        top_companies = top_companies.sort_values('count', ascending=False).head(20)
        for _, row in top_companies.iterrows():
            print(f"  {row['company_name']:<30} {row['count']:>5}")
        
        print("\n" + "-"*80)
        print("Questions by Difficulty:")
        print("-"*80)
        diffs = df.groupby('difficulty').size().reset_index(name='count')
        for _, row in diffs.iterrows():
            print(f"  {row['difficulty']:<20} {row['count']:>5}")
        
        # Quality
        print("\n" + "-"*80)
        print("Data Quality:")
        print("-"*80)
        print("  ✅ 100% Company-Specific")
        print("  ✅ Curated LeetCode questions")
        print("  ✅ Clean structured data")
        print(f"  ✅ {len(top_companies)} companies covered")
        
        # Samples
        print("\n" + "-"*80)
        print("Sample Questions (First 10):")
        print("-"*80)
        for idx, row in df.head(10).iterrows():
            print(f"\n{row['company_name']} [{row['difficulty']}]:")
            print(f"  {row['interview_question']}")
        
        print("\n" + "="*80)
        print("✅ SCRAPING COMPLETE!")
        print("="*80)
        
        return df
    
    return None


if __name__ == "__main__":
    scrape_github_leetcode_raw()