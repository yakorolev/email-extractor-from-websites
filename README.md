# university-email-extractor-and-dataset

Email scraping and extraction tool using Scrapy.  
Supports domain crawling, email filtering, and CSV result merging.  
Originally developed for academic websites.
In addition to the email extractor, this repo contains a validated list of 13,000+ universities worldwide.

---

📊 Full University Dataset
This repository includes a full CSV dataset of global universities with the following fields:
Column Name	Description
country_code	ISO 3166-1 alpha-2 country code (e.g., AE, US)
Name	Official name of the university
web_pages	Main university website URL
url_status_codes	Result of HTTP validation for the URL (e.g. [200], or ['ERROR: SSLError'])
all_urls_valid	Boolean indicator (TRUE/FALSE) of URL validity
📁 File: full_universities_list.csv
This file was used to validate which university websites are reachable and suitable for crawling.

---

## 📁 Project Structure

parser/ # Scrapy spider
postprocess/ # CSV merger script
docker/ # Dockerfile
examples/ # Example CSV files

---

## 📂 CSV Format

All CSV files are in UTF-8, comma-separated.

### `universities.csv` (input) 

country_code,university_name,url
DE,LMU Munich,https://www.lmu.de/

### `input_urls.csv` (input) 

url
https://www.lmu.de/

### `results_emails.csv` (from parser) in the next step we will use the file name "emails"

url,emails
https://www.lmu.de/,"info@lmu.de"

### `final_emails.csv` (merged)

country_code,university_name,university_url,sourse_url,emails
DE,LMU Munich,https://www.lmu.de/,https://www.lmu.de/phd, "phd@lmu.de"

---

## 🚀 Usage

1. Run the Scrapy spider to extract emails from all URLs in `universities.csv`.
2. The crawler writes results to `emails.csv`.
3. Run `emails_merge.py` to join with the original university data.
4. Final output is saved as `final_emails.csv`.

---

## ⚠️ Legal Notice

- This tool only collects emails from publicly accessible web pages.
- Do not use for unsolicited messages or spam.
- The author is not responsible for misuse or legal violations.

---

## 📄 License

MIT
