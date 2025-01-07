import requests
from bs4 import BeautifulSoup
import pymongo
import os
import random
import time

# Define base URL and parameters
base_url = "https://www.screener.in/screen/raw/"
params = {
    "sort": "Market Capitalization",
    "order": "desc",
    "source_id": "343087",
    "query": "Current price >0",
    "page": 1
}

# Headers to mimic a browser
headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
    "Referer": "https://www.screener.in/login/"  # Adjust Referer if needed for CSRF validation
}

# Initialize MongoDB client
client = pymongo.MongoClient("mongodb://localhost:27017/")
db = client["scraped_data"]
companies_collection = db["companies"]
company_data_collection = db["company_data"]

# Define a function to scrape data from a single page
def scrape_page(session, page):
    params["page"] = page
    params["source_id"] = str(random.randint(100000, 999999))  # Randomize source_id

    try:
        # Retry logic for handling 429 errors
        for attempt in range(5):  # Retry up to 5 times
            response = session.get(base_url, params=params, headers=headers)
            if response.status_code == 429:
                print(f"429 Too Many Requests on page {page}, attempt {attempt + 1}. Retrying...")
                time.sleep(5 * (attempt + 1))  # Exponential backoff
            else:
                break

        response.raise_for_status()

        # Parse the content
        soup = BeautifulSoup(response.content, "html.parser")

        # Extract company links and save to the database
        companies = soup.select("body > main > div.card.card-large > div.responsive-holder.fill-card-width > table > tbody > tr > td:nth-child(2) > a")
        for company in companies:
            company_name = company.text.strip()
            company_link = company["href"]

            # Insert company data into MongoDB
            company_doc = {"name": company_name, "url": company_link}
            company_id = companies_collection.insert_one(company_doc).inserted_id

            # Navigate to the company's page
            company_url = f"https://www.screener.in{company_link}"
            company_response = session.get(company_url, headers=headers)
            company_response.raise_for_status()
            company_soup = BeautifulSoup(company_response.content, "html.parser")

            # Extract additional data from the company's page
            details_table = company_soup.find("table")
            if details_table:
                rows = details_table.find_all("tr")
                for row in rows:
                    columns = row.find_all("td")
                    if len(columns) == 2:  # Assuming key-value pairs
                        header = columns[0].text.strip()
                        value = columns[1].text.strip()

                        # Insert detailed data into MongoDB
                        company_data_collection.insert_one({
                            "company_id": company_id,
                            "header": header,
                            "value": value
                        })

        print(f"Extracted and saved data for page {page}.")

        # Add a delay to avoid hitting rate limits
        time.sleep(random.uniform(2, 5))  # Random delay between 2-5 seconds

    except Exception as e:
        print(f"Failed to process page {page}: {e}")

# Create a session to manage login and requests
with requests.Session() as session:
    # Log in to the site (update login URL and credentials accordingly)
    login_url = "https://www.screener.in/login/"
    credentials = {
        "username": "singhanand98@yahoo.com",  # Replace with your email
        "password": "anandsin"           # Replace with your password
    }

    # Get login page to fetch CSRF token
    login_page = session.get(login_url, headers=headers)
    soup = BeautifulSoup(login_page.content, "html.parser")
    csrf_token = soup.find("input", {"name": "csrfmiddlewaretoken"})
    if csrf_token:
        credentials["csrfmiddlewaretoken"] = csrf_token["value"]

    # Log in
    login_response = session.post(login_url, data=credentials, headers=headers)
    if login_response.url != login_url:
        print("Logged in successfully!")

        # Loop through pages and scrape data
        for page in range(1, 98):
            scrape_page(session, page)
    else:
        print("Login failed. Please check your credentials.")

print("All pages processed and data saved to MongoDB.")