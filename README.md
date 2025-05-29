# email-extractor-from-websites

Email scraping and extraction tool using Scrapy.  
Supports domain crawling, email filtering, and CSV result merging.  
Originally developed for academic websites.

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
