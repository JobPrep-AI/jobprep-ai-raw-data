from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
import pandas as pd
from datetime import datetime
import time

def scrape_interviewbit_complete():
    """
    Scrape ALL InterviewBit questions with aggressive scrolling
    """
    
    print("InterviewBit Complete Scraper - Aggressive Scroll Mode")
    print("="*80)
    
    url = "https://www.interviewbit.com/coding-interview-questions/"
    
    # Company mapping from CSS classes
    company_mapping = {
        'bloomberg': 'Bloomberg', 'google': 'Google', 'amazon': 'Amazon',
        'microsoft': 'Microsoft', 'facebook': 'Facebook', 'meta': 'Meta',
        'apple': 'Apple', 'netflix': 'Netflix', 'adobe': 'Adobe',
        'uber': 'Uber', 'linkedin': 'LinkedIn', 'twitter': 'Twitter',
        'goldman-sachs': 'Goldman Sachs', 'goldman': 'Goldman Sachs',
        'goldmann-sachs': 'Goldman Sachs', 'morgan-stanley': 'Morgan Stanley',
        'morgan': 'Morgan Stanley', 'salesforce': 'Salesforce', 'oracle': 'Oracle',
        'vmware': 'VMware', 'cisco': 'Cisco', 'paypal': 'PayPal',
        'ebay': 'eBay', 'airbnb': 'Airbnb', 'flipkart': 'Flipkart',
        'walmart': 'Walmart', 'yahoo': 'Yahoo', 'samsung': 'Samsung',
        'intel': 'Intel', 'tesla': 'Tesla', 'de-shaw': 'DE Shaw',
        'directi': 'Directi', 'tower-research-capital': 'Tower Research',
        'epic-systems': 'Epic Systems', 'nobrokercom': 'NoBroker',
        'lyft': 'Lyft', 'intuit': 'Intuit', 'nvidia': 'NVIDIA',
        'qualcomm': 'Qualcomm', 'visa': 'Visa', 'jpmorgan': 'JPMorgan',
        'spotify': 'Spotify', 'stripe': 'Stripe', 'snowflake': 'Snowflake'
    }
    
    all_data = []
    driver = None
    
    try:
        # Initialize browser
        print("\n[1/6] Starting Chrome browser...")
        
        options = webdriver.ChromeOptions()
        # Run visible so you can see it working
        # options.add_argument('--headless')  # Commented out - visible mode
        options.add_argument('--no-sandbox')
        options.add_argument('--disable-dev-shm-usage')
        options.add_argument('--window-size=1920,1080')
        options.add_argument('--disable-blink-features=AutomationControlled')
        options.add_experimental_option("excludeSwitches", ["enable-automation"])
        options.add_experimental_option('useAutomationExtension', False)
        
        driver = webdriver.Chrome(
            service=Service(ChromeDriverManager().install()),
            options=options
        )
        print("✓ Chrome initialized")
        
        # Load page
        print(f"\n[2/6] Loading page...")
        driver.get(url)
        time.sleep(5)
        print("✓ Page loaded")
        
        # Aggressive scrolling
        print("\n[3/6] Aggressive scrolling to load all questions...")
        print("(This takes 2-3 minutes - watch Chrome window scroll!)\n")
        
        previous_count = 0
        no_change_count = 0
        max_no_change = 5
        
        for iteration in range(50):
            # AGGRESSIVE MULTI-SCROLL (based on your observation)
            # Do 5 rapid scrolls to trigger lazy loading
            for rapid in range(5):
                # Scroll to absolute bottom
                driver.execute_script("""
                    window.scrollTo({
                        top: document.body.scrollHeight,
                        behavior: 'smooth'
                    });
                """)
                time.sleep(0.4)
                
                # Backup scroll method
                driver.execute_script("window.scrollBy(0, 10000);")
                time.sleep(0.4)
            
            # Wait for new content
            time.sleep(3)
            
            # Check tile count
            tiles = driver.find_elements(By.CLASS_NAME, "pl-problem-tile")
            current_count = len(tiles)
            
            print(f"  Iteration {iteration + 1:2d}: {current_count:3d} questions", end="")
            
            if current_count == previous_count:
                no_change_count += 1
                print(f" (no change #{no_change_count})")
                
                if no_change_count >= max_no_change:
                    print(f"\n✓ Finished! No new content after {max_no_change} attempts")
                    break
            else:
                added = current_count - previous_count
                no_change_count = 0
                print(f" (+{added} new) ✓")
            
            previous_count = current_count
        
        final_count = len(driver.find_elements(By.CLASS_NAME, "pl-problem-tile"))
        print(f"\n✓ Total questions loaded: {final_count}")
        
        # Extract data
        print(f"\n[4/6] Extracting data from {final_count} questions...")
        
        problem_tiles = driver.find_elements(By.CLASS_NAME, "pl-problem-tile")
        processed = set()
        
        for i, tile in enumerate(problem_tiles):
            try:
                # Get question
                link = tile.find_element(By.CLASS_NAME, "pl-problem-tile__statement")
                title = link.text.strip()
                url_q = link.get_attribute('href')
                
                if not title or title in processed:
                    continue
                processed.add(title)
                
                # Get difficulty
                difficulty = 'Not Specified'
                try:
                    diff = tile.find_element(By.CSS_SELECTOR, "[class*='difficulty-level']")
                    diff_text = diff.text.strip().lower()
                    if 'easy' in diff_text:
                        difficulty = 'Easy'
                    elif 'medium' in diff_text:
                        difficulty = 'Medium'
                    elif 'hard' in diff_text:
                        difficulty = 'Hard'
                except:
                    pass
                
                # Get companies
                comp_list = []
                try:
                    sprites = tile.find_elements(By.CSS_SELECTOR, "[class*='ib-company-sprites']")
                    for sprite in sprites:
                        classes = sprite.get_attribute('class').split()
                        for cls in classes:
                            if cls.startswith('ib-') and cls != 'ib-company-sprites':
                                key = cls.replace('ib-', '')
                                if key in company_mapping:
                                    comp_list.append(company_mapping[key])
                except:
                    pass
                
                comp_list = list(set(comp_list))
                if not comp_list:
                    comp_list = ['Multiple Companies']
                
                # Add entries
                for comp in comp_list:
                    all_data.append({
                        'company_name': comp,
                        'role_name': 'Software Engineer',
                        'interview_question': title,
                        'difficulty': difficulty,
                        'question_url': url_q,
                        'source': 'InterviewBit',
                        'date_collected': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                    })
                
                if (i + 1) % 50 == 0:
                    print(f"  → Processed {i + 1}/{len(problem_tiles)}...")
            
            except:
                continue
        
        print(f"✓ Extracted {len(all_data)} question-company pairs")
        
        # Save HTML
        print(f"\n[5/6] Saving debug file...")
        with open('interviewbit_final.html', 'w', encoding='utf-8') as f:
            f.write(driver.page_source)
        print("✓ Saved to 'interviewbit_final.html'")
        
        # Close browser
        print(f"\n[6/6] Closing browser...")
        driver.quit()
        print("✓ Done")
        
    except Exception as e:
        print(f"\n✗ Error: {e}")
        import traceback
        traceback.print_exc()
        if driver:
            try:
                driver.quit()
            except:
                pass
        return None
    
    # Save results
    if all_data:
        df = pd.DataFrame(all_data)
        df = df.drop_duplicates(subset=['company_name', 'interview_question'])
        
        filename = f'interviewbit_full_{datetime.now().strftime("%Y%m%d_%H%M%S")}.csv'
        df.to_csv(filename, index=False)
        
        print("\n" + "="*80)
        print("FINAL RESULTS")
        print("="*80)
        print(f"✓ Total unique pairs: {len(df)}")
        print(f"✓ Saved to: {filename}")
        
        print("\n" + "-"*80)
        print("Top Companies:")
        print("-"*80)
        top = df.groupby('company_name').size().reset_index(name='count')
        top = top.sort_values('count', ascending=False).head(15)
        for _, r in top.iterrows():
            print(f"  {r['company_name']:<25} {r['count']:>4}")
        
        print("\n" + "-"*80)
        print("By Difficulty:")
        print("-"*80)
        diffs = df.groupby('difficulty').size().reset_index(name='count')
        for _, r in diffs.iterrows():
            print(f"  {r['difficulty']:<15} {r['count']:>4}")
        
        print("\n" + "="*80)
        print("✅ COMPLETE!")
        print("="*80)
        
        return df
    
    return None


if __name__ == "__main__":
    scrape_interviewbit_complete()