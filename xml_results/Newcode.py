import requests
from bs4 import BeautifulSoup
import pandas as pd
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

# List to hold all extracted rows
company_names = []
data_rows = []
table_headers = []

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

        # Extract company names
        companies = soup.select("body > main > div.card.card-large > div.responsive-holder.fill-card-width > table > tbody > tr > td:nth-child(2) > a")
        for company in companies:
            company_names.append(company.text.strip())

        # Extract table data (if present)
        table = soup.find("table")
        if table:
            table_headers.clear()
            table_headers.extend([header.text.strip() for header in table.find_all("th")])
            rows = table.find_all("tr")
            for row in rows:
                columns = row.find_all("td")
                if columns:
                    data_rows.append([col.text.strip() for col in columns])
            print(f"Extracted data for page {page}.")
        else:
            print(f"No table found on page {page}.")

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
        for page in range(1, 5):
            scrape_page(session, page)

        # Save the company names to a CSV file
        if company_names:
            company_file = "company_names.csv"
            pd.DataFrame(company_names, columns=["Company Name"]).to_csv(company_file, index=False)
            print(f"Company names saved to {company_file}.")

        # Save the table data to a CSV file
        if data_rows:
            data_file = "extracted_data.csv"
            df = pd.DataFrame(data_rows, columns=headers)
            df.to_csv(data_file, index=False)
            print(f"Data saved to {data_file}.")
        else:
            print("No data extracted.")
    else:
        print("Login failed. Please check your credentials.")

print("All pages processed.")
