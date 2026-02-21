import requests
from bs4 import BeautifulSoup
import pandas as pd
from datetime import datetime
import re

def scrape_geeksforgeeks_correct():
    """
    Scrape GeeksforGeeks company-wise questions
    Based on actual page structure with company headings
    """
    
    print("Starting GeeksforGeeks Company-wise Scraper")
    print("="*80)
    
    url = "https://www.geeksforgeeks.org/blogs/must-coding-questions-company-wise/"
    
    headers = {
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
    }
    
    all_data = []
    
    try:
        print(f"\nFetching: {url}")
        response = requests.get(url, headers=headers, timeout=15)
        
        if response.status_code == 200:
            print(f"✓ Page loaded (Status: {response.status_code})\n")
            
            soup = BeautifulSoup(response.content, 'html.parser')
            
            # Find the main content area (exclude footer, sidebar, etc.)
            content = soup.find('article') or soup.find('div', class_='entry-content') or soup.find('main')
            
            # If we can't find the main content, try to exclude common footer/header elements
            if content is None:
                content = soup
                # Remove footer, header, nav, sidebar
                for unwanted in content.find_all(['footer', 'header', 'nav', 'aside']):
                    unwanted.decompose()
            
            # Strategy: Find all headings and their following content
            current_company = None
            current_difficulty = None
            
            # Get all elements in order
            elements = content.find_all(['h2', 'h3', 'h4', 'h5', 'p', 'ul', 'li', 'strong'])
            
            for element in elements:
                text = element.get_text().strip()
                
                # Check if this is a company heading
                # Pattern: "Amazon Interview Coding Questions:" or "Microsoft Interview"
                if ('interview' in text.lower() and 
                    ('coding' in text.lower() or 'questions' in text.lower()) and
                    len(text) < 100):
                    
                    # Extract company name
                    company_match = re.match(r'([A-Za-z\s]+)\s+Interview', text)
                    if company_match:
                        current_company = company_match.group(1).strip()
                        print(f"\n📌 Found Company: {current_company}")
                        current_difficulty = None
                
                # Check if this is a difficulty level
                if text in ['Easy:', 'Medium:', 'Hard:']:
                    current_difficulty = text.rstrip(':')
                    print(f"   └─ Difficulty: {current_difficulty}")
                
                # Extract question links
                if current_company:
                    links = element.find_all('a', href=True)
                    
                    for link in links:
                        question_text = link.get_text().strip()
                        href = link['href']
                        
                        # Exclude common footer/navigation links
                        exclude_keywords = [
                            'about', 'contact', 'privacy', 'terms', 'cookie',
                            'newsletter', 'subscribe', 'login', 'sign up', 'careers',
                            'advertise', 'write for us', 'explore more', 'corporate',
                            'legal', 'sitemap', 'help', 'support', 'faq'
                        ]
                        
                        # Check if this looks like a footer/nav link
                        is_footer_link = any(keyword in question_text.lower() for keyword in exclude_keywords)
                        is_footer_link = is_footer_link or any(keyword in href.lower() for keyword in exclude_keywords)
                        
                        # Filter for actual question links
                        if (len(question_text) > 5 and 
                            len(question_text) < 300 and
                            'geeksforgeeks.org' in href and
                            ('/problems/' in href or '/practice/' in href or question_text.endswith('?')) and
                            not is_footer_link):
                            
                            all_data.append({
                                'company_name': current_company,
                                'role_name': 'Software Engineer',
                                'interview_question': question_text,
                                'difficulty': current_difficulty if current_difficulty else 'Not Specified',
                                'question_url': href,
                                'source': 'GeeksforGeeks',
                                'date_collected': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                            })
            
            print(f"\n✓ Total questions extracted: {len(all_data)}")
            
        else:
            print(f"✗ Failed to load page: Status {response.status_code}")
            return None
            
    except Exception as e:
        print(f"✗ Error: {e}")
        import traceback
        traceback.print_exc()
        return None
    
    # Process and save data
    if all_data:
        df = pd.DataFrame(all_data)
        
        # Remove duplicates
        df = df.drop_duplicates(subset=['company_name', 'interview_question'])
        
        # Save to CSV
        filename = f'gfg_companywise_{datetime.now().strftime("%Y%m%d_%H%M%S")}.csv'
        df.to_csv(filename, index=False)
        
        print("\n" + "="*80)
        print("RESULTS SUMMARY")
        print("="*80)
        print(f"✓ Total unique questions: {len(df)}")
        print(f"✓ Saved to: {filename}")
        
        # Company breakdown
        print("\n" + "="*80)
        print("QUESTIONS BY COMPANY:")
        print("="*80)
        company_counts = df.groupby('company_name').size().reset_index(name='count')
        company_counts = company_counts.sort_values('count', ascending=False)
        print(company_counts.to_string(index=False))
        
        # Difficulty breakdown
        print("\n" + "="*80)
        print("QUESTIONS BY DIFFICULTY:")
        print("="*80)
        difficulty_counts = df.groupby('difficulty').size().reset_index(name='count')
        print(difficulty_counts.to_string(index=False))
        
        # Sample questions by company
        print("\n" + "="*80)
        print("SAMPLE QUESTIONS:")
        print("="*80)
        
        companies = df['company_name'].unique()[:5]  # First 5 companies
        for company in companies:
            company_df = df[df['company_name'] == company].head(5)
            print(f"\n{company}:")
            for idx, row in company_df.iterrows():
                print(f"  [{row['difficulty']}] {row['interview_question']}")
        
        return df
        
    else:
        print("\n✗ No data collected")
        return None


if __name__ == "__main__":
    df = scrape_geeksforgeeks_correct()
    
    if df is not None:
        print("\n" + "="*80)
        print("✅ SCRAPING COMPLETED SUCCESSFULLY!")
        print("="*80)
        print("\nNext Step: Load this data into Snowflake!")
    else:
        print("\n" + "="*80)
        print("❌ SCRAPING FAILED")
        print("="*80)