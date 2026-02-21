from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager
import pandas as pd
from datetime import datetime
import time


def scrape_tryexponent_updated():
    """
    Stable TryExponent scraper using direct page navigation
    """

    print("TryExponent Scraper - STABLE VERSION")
    print("=" * 80)

    base_url = "https://www.tryexponent.com/questions?page={}"
    max_pages = 221
    all_data = []

    driver = None

    try:
        # ----------------------
        # 1️⃣ Setup Chrome
        # ----------------------
        print("\n[1/5] Starting Chrome...")

        options = webdriver.ChromeOptions()
        options.add_argument("--headless")
        options.add_argument("--no-sandbox")
        options.add_argument("--disable-dev-shm-usage")
        options.add_argument("--window-size=1920,1080")
        options.add_argument("--disable-blink-features=AutomationControlled")

        driver = webdriver.Chrome(
            service=Service(ChromeDriverManager().install()),
            options=options
        )

        wait = WebDriverWait(driver, 15)

        print("✓ Browser Ready")

        # ----------------------
        # 2️⃣ Scrape Pages
        # ----------------------
        print("\n[2/5] Scraping ALL pages...")
        print("This will take ~15 minutes ☕\n")

        for page in range(1, max_pages + 1):

            print(f"\nPage {page}/{max_pages}")

            driver.get(base_url.format(page))

            # Wait until questions load
            wait.until(EC.presence_of_element_located((By.TAG_NAME, "li")))

            question_lis = driver.find_elements(By.TAG_NAME, "li")

            print(f"Found {len(question_lis)} <li> elements")

            extracted_this_page = 0

            for li in question_lis:
                try:
                    links = li.find_elements(By.TAG_NAME, "a")

                    question_link = None

                    for link in links:
                        href = link.get_attribute("href") or ""

                        if (
                            "/questions/" in href
                            and "?company=" not in href
                            and "/questions?" not in href
                        ):
                            question_link = link
                            break

                    if not question_link:
                        continue

                    question_title = question_link.text.strip()
                    question_url = question_link.get_attribute("href")

                    if not question_title or len(question_title) < 10:
                        continue

                    # Extract companies
                    companies = []

                    for link in links:
                        href = link.get_attribute("href") or ""

                        if "?company=" in href or "&company=" in href:
                            comp_name = link.text.strip()
                            if comp_name:
                                companies.append(comp_name)

                    if not companies:
                        companies = ["Multiple Companies"]

                    # Detect role
                    li_text = li.text
                    role = "Software Engineer"

                    if "Product Manager" in li_text:
                        role = "Product Manager"
                    elif "Machine Learning Engineer" in li_text or "ML Engineer" in li_text:
                        role = "ML Engineer"
                    elif "Technical Program Manager" in li_text or "TPM" in li_text:
                        role = "Technical Program Manager"

                    for company in companies:
                        all_data.append({
                            "company_name": company,
                            "role_name": role,
                            "interview_question": question_title,
                            "difficulty": "Not Specified",
                            "question_url": question_url,
                            "source": "TryExponent",
                            "date_collected": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                        })

                    extracted_this_page += 1

                except Exception:
                    continue

            print(f"Extracted {extracted_this_page} questions")

            # Light throttle to avoid bot detection
            time.sleep(2)

        # ----------------------
        # 3️⃣ Close Browser
        # ----------------------
        print("\n[3/5] Closing browser...")
        driver.quit()

        print("✓ Browser Closed")

    except Exception as e:
        print(f"\n✗ Error: {e}")
        if driver:
            driver.quit()
        return None

    # ----------------------
    # 4️⃣ Save Data
    # ----------------------
    print("\n[4/5] Processing Data...")

    if not all_data:
        print("No data extracted.")
        return None

    df = pd.DataFrame(all_data)

    df = df.drop_duplicates(
        subset=["company_name", "interview_question"]
    )

    filename = f"tryexponent_updated_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"

    df.to_csv(filename, index=False)

    # ----------------------
    # 5️⃣ Summary
    # ----------------------
    print("\n" + "=" * 80)
    print("RESULTS")
    print("=" * 80)

    print(f"Total Questions: {len(df)}")
    print(f"Saved to: {filename}")

    specific = len(df[df["company_name"] != "Multiple Companies"])
    total = len(df)

    print("\nCompany Coverage:")
    print(f"Company-Specific: {specific} ({specific/total*100:.1f}%)")
    print(f"Multiple Companies: {total-specific} ({(total-specific)/total*100:.1f}%)")

    print("\nTop 10 Companies:")
    companies = (
        df.groupby("company_name")
        .size()
        .reset_index(name="count")
        .sort_values("count", ascending=False)
        .head(10)
    )

    print(companies)

    print("\nScraping Complete 🚀")
    print("=" * 80)

    return df


if __name__ == "__main__":
    scrape_tryexponent_updated()