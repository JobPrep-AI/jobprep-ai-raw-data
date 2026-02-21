import requests
import pandas as pd
from datetime import datetime
import time
import re

def scrape_reddit_technical_questions():
    """
    Fixed Reddit scraper - only technical questions with proper company extraction
    """
    
    print("Reddit Technical Interview Questions Scraper (Fixed)")
    print("="*80)
    
    all_data = []
    
    # Focus on technical subreddits only
    subreddits = [
        'csinterviewproblems',  # Most technical
        'leetcode',              # Coding focused
        'ExperiencedDevs'        # Professional discussions
    ]
    
    # More specific search queries
    search_queries = [
        'asked to implement',
        'asked to design',
        'asked to code',
        'technical question',
        'algorithm question',
        'coding question asked'
    ]
    
    headers = {
        'User-Agent': 'JobPrepAI Scraper v2.0'
    }
    
    print(f"\nSearching {len(subreddits)} technical subreddits...")
    print("(Takes ~2 minutes)\n")
    
    for subreddit in subreddits:
        print(f"📍 r/{subreddit}")
        print("-"*60)
        
        for query in search_queries:
            try:
                url = f'https://www.reddit.com/r/{subreddit}/search.json'
                params = {
                    'q': query,
                    'restrict_sr': 'true',
                    'sort': 'relevance',  # Changed to relevance
                    'limit': 50,          # Increased limit
                    't': 'all'            # All time for more data
                }
                
                response = requests.get(url, headers=headers, params=params, timeout=10)
                
                if response.status_code == 200:
                    data = response.json()
                    posts = data.get('data', {}).get('children', [])
                    
                    questions_found = 0
                    
                    for post in posts:
                        post_data = post.get('data', {})
                        title = post_data.get('title', '')
                        selftext = post_data.get('selftext', '')
                        permalink = post_data.get('permalink', '')
                        
                        # Extract company (more conservative)
                        company = extract_company_conservative(title, selftext)
                        
                        # Only process if we found a real company
                        if company != 'SKIP':
                            # Extract technical questions only
                            questions = extract_technical_questions(title, selftext)
                            
                            for q in questions:
                                all_data.append({
                                    'company_name': company,
                                    'role_name': 'Software Engineer',
                                    'interview_question': q,
                                    'difficulty': 'Not Specified',
                                    'question_url': f"https://reddit.com{permalink}",
                                    'source': f'Reddit - r/{subreddit}',
                                    'date_collected': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                                })
                                questions_found += 1
                    
                    if questions_found > 0:
                        print(f"  '{query[:25]}...' → {questions_found} technical questions")
                
                time.sleep(2)
                
            except Exception as e:
                print(f"  ✗ Error: {e}")
    
    # Save results
    if all_data:
        df = pd.DataFrame(all_data)
        df = df.drop_duplicates(subset=['interview_question'])
        
        filename = f'reddit_technical_{datetime.now().strftime("%Y%m%d_%H%M%S")}.csv'
        df.to_csv(filename, index=False)
        
        print("\n" + "="*80)
        print("RESULTS")
        print("="*80)
        print(f"✓ Total technical questions: {len(df)}")
        print(f"✓ Saved to: {filename}")
        
        # Quality metrics
        total = len(df)
        specific = len(df[df['company_name'] != 'Multiple Companies'])
        
        print("\n" + "-"*80)
        print("Quality Metrics:")
        print("-"*80)
        print(f"  Company-Specific:   {specific:4d} ({specific/total*100:.1f}%)")
        print(f"  Multiple Companies: {total-specific:4d} ({(total-specific)/total*100:.1f}%)")
        
        # Company breakdown
        if specific > 0:
            print("\n" + "-"*80)
            print("Questions by Company:")
            print("-"*80)
            companies = df.groupby('company_name').size().reset_index(name='count')
            companies = companies.sort_values('count', ascending=False).head(15)
            for _, row in companies.iterrows():
                print(f"  {row['company_name']:<25} {row['count']:>4}")
        
        # Show samples
        print("\n" + "-"*80)
        print("Sample Technical Questions (First 10):")
        print("-"*80)
        for idx, row in df.head(10).iterrows():
            print(f"\n{row['company_name']}:")
            print(f"  {row['interview_question'][:120]}")
        
        print("\n" + "="*80)
        
        # Decision helper
        if specific/total >= 0.5:
            print("✅ GOOD DATA - 50%+ company-specific!")
            print("   Recommended: Load to Snowflake")
        elif specific/total >= 0.3:
            print("⚠️  MODERATE - 30-50% company-specific")
            print("   Your call: Load or skip")
        else:
            print("❌ POOR DATA - Less than 30% company-specific")
            print("   Recommended: Skip this source")
        
        print("="*80)
        
        return df
    
    return None


def extract_company_conservative(title, text):
    """
    Conservative company extraction - only if very confident
    Returns 'SKIP' if uncertain (better to skip than wrong)
    """
    
    # Known tech companies (expanded list)
    known_companies = {
        'google': 'Google', 'amazon': 'Amazon', 'microsoft': 'Microsoft',
        'facebook': 'Facebook', 'meta': 'Meta', 'apple': 'Apple',
        'netflix': 'Netflix', 'adobe': 'Adobe', 'uber': 'Uber',
        'lyft': 'Lyft', 'airbnb': 'Airbnb', 'linkedin': 'LinkedIn',
        'twitter': 'Twitter', 'tesla': 'Tesla', 'spacex': 'SpaceX',
        'stripe': 'Stripe', 'square': 'Square', 'bloomberg': 'Bloomberg',
        'goldman sachs': 'Goldman Sachs', 'morgan stanley': 'Morgan Stanley',
        'jpmorgan': 'JPMorgan', 'oracle': 'Oracle', 'salesforce': 'Salesforce',
        'ibm': 'IBM', 'cisco': 'Cisco', 'intel': 'Intel', 'nvidia': 'NVIDIA',
        'amd': 'AMD', 'qualcomm': 'Qualcomm', 'paypal': 'PayPal',
        'visa': 'Visa', 'mastercard': 'Mastercard', 'doordash': 'DoorDash',
        'instacart': 'Instacart', 'robinhood': 'Robinhood', 'coinbase': 'Coinbase',
        'databricks': 'Databricks', 'snowflake': 'Snowflake', 'mongodb': 'MongoDB',
        'shopify': 'Shopify', 'spotify': 'Spotify', 'pinterest': 'Pinterest',
        'snap': 'Snapchat', 'roblox': 'Roblox', 'epic games': 'Epic Games',
        'riot games': 'Riot Games', 'twitch': 'Twitch', 'discord': 'Discord',
        'plaid': 'Plaid', 'ramp': 'Ramp', 'brex': 'Brex', 'chime': 'Chime',
        'affirm': 'Affirm', 'klarna': 'Klarna', 'figma': 'Figma',
        'notion': 'Notion', 'airtable': 'Airtable', 'asana': 'Asana'
    }
    
    combined_text = f"{title} {text}".lower()
    
    # Check for known companies ONLY
    for key, name in known_companies.items():
        if key in combined_text:
            # Verify it's in interview context
            context_words = ['interview', 'asked', 'offered', 'onsite', 'phone screen']
            if any(word in combined_text for word in context_words):
                return name
    
    # If no known company found, skip this post
    return 'SKIP'


def extract_technical_questions(title, text):
    """
    Extract ONLY technical/coding interview questions
    Filter out career advice and meta discussions
    """
    
    questions = []
    combined = f"{title}\n{text}"
    
    # Split into sentences
    sentences = re.split(r'[.!?\n]+', combined)
    
    for sentence in sentences:
        sentence = sentence.strip()
        
        # Must be a question (ends with ? or contains question keywords)
        if not ('?' in sentence or 
                any(word in sentence.lower() for word in ['how to', 'how would', 'what is'])):
            continue
        
        # Must be reasonable length
        if len(sentence) < 20 or len(sentence) > 300:
            continue
        
        # SKIP career advice / meta questions
        skip_patterns = [
            r'\b(should i|would you|is it worth|anyone else|does anyone|has anyone)\b',
            r'\b(advice|tips|suggestions|recommend|opinion|thoughts)\b',
            r'\b(job market|career|salary|offer|negotiate|quit|switch)\b',
            r'\b(resume|cv|linkedin|apply|application)\b',
            r'\b(imposter|burnout|toxic|manager|team|culture)\b'
        ]
        
        if any(re.search(pattern, sentence.lower()) for pattern in skip_patterns):
            continue
        
        # MUST have technical keywords
        technical_keywords = [
            # Data structures
            'array', 'list', 'tree', 'graph', 'hash', 'stack', 'queue',
            'heap', 'trie', 'matrix', 'linked list',
            # Algorithms
            'sort', 'search', 'binary search', 'dfs', 'bfs', 'dynamic programming',
            'greedy', 'backtrack', 'recursion', 'iterate',
            # Actions
            'implement', 'design', 'optimize', 'reverse', 'merge', 'find',
            'calculate', 'compute', 'solve', 'write a function', 'write code',
            # Concepts
            'time complexity', 'space complexity', 'algorithm', 'data structure',
            'leetcode', 'coding problem', 'technical question',
            # System design
            'system design', 'architecture', 'scalability', 'database design',
            'api design', 'cache', 'load balancer', 'microservice'
        ]
        
        has_technical = any(keyword in sentence.lower() for keyword in technical_keywords)
        
        if has_technical:
            # Clean up the question
            question = sentence.strip()
            # Remove "Question:" prefix if present
            question = re.sub(r'^(Q:|Question:|q:)\s*', '', question, flags=re.IGNORECASE)
            questions.append(question)
    
    return questions


if __name__ == "__main__":
    scrape_reddit_technical_questions()