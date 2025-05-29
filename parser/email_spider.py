import scrapy
import re
import boto3
import os
import csv
from urllib.parse import urlparse

class EmailSpider(scrapy.Spider):
    name = "email_spider"

    custom_settings = {
        'DEPTH_LIMIT': 5,
        'ROBOTSTXT_OBEY': False,
        'DOWNLOAD_DELAY': 0.1,
        'CONCURRENT_REQUESTS': 24,
        'CONCURRENT_REQUESTS_PER_DOMAIN': 8,
        'DOWNLOAD_TIMEOUT': 30,
        'RETRY_TIMES': 3,
        'RETRY_HTTP_CODES': [500, 502, 503, 504, 522, 524, 408, 429],
        'AUTOTHROTTLE_ENABLED': True,
        'AUTOTHROTTLE_START_DELAY': 0.25,
        'AUTOTHROTTLE_MAX_DELAY': 5,
        'AUTOTHROTTLE_TARGET_CONCURRENCY': 6.0,
        'DOWNLOAD_FAIL_ON_DATALOSS': False,
        'USER_AGENT': 'Mozilla/5.0 (compatible; EmailScraper/1.0; +http://example.com/bot)'
    }

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.bucket = os.getenv('AWS_BUCKET_NAME', 'email-scraper-university')
        self.input_file = os.getenv('INPUT_CSV_FILENAME', 'input_urls.csv')
        self.output_file = os.getenv('OUTPUT_FILENAME', 'results_emails.csv')
        self.s3 = boto3.client('s3')
        self.crawled_data = []
        self.processed_count = 0
        self.allowed_domains = set()
        self.found_emails = set()
        self.error_counts = {}
        self.error_threshold = 3  # Maximum 3 errors per domain

    def start_requests(self):
        local_file = '/tmp/input_urls.csv'
        self.s3.download_file(self.bucket, self.input_file, local_file)

        with open(local_file, newline='', encoding='utf-8') as csvfile:
            reader = csv.reader(csvfile)
            next(reader)
            for row in reader:
                url = row[0].strip()
                domain = domain_from_url(url)
                self.allowed_domains.add(domain)

                yield scrapy.Request(
                    url=url,
                    callback=self.parse,
                    errback=self.errback_http,
                    meta={'original_domain': domain, 'depth': 0}
                )

    def parse(self, response):
        domain = response.meta['original_domain']
        current_depth = response.meta.get('depth', 0)

        if not response.body:
            return

        content = response.text
        emails = set(re.findall(r"\b[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}\b", content))
        new_emails = emails - self.found_emails
        if new_emails:
            self.found_emails.update(new_emails)
            self.crawled_data.append({'url': response.url, 'emails': list(new_emails)})

        links = response.css('a::attr(href)').getall()
        for link in links:
            next_url = response.urljoin(link)
            next_domain = domain_from_url(next_url)

            if next_domain not in self.allowed_domains:
                continue

            if is_unwanted_url(next_url) or is_login_page(next_url):
                continue

            if self.error_counts.get(next_domain, 0) >= self.error_threshold:
                continue

            normalized_url = next_url

            yield scrapy.Request(
                url=normalized_url,
                callback=self.parse,
                errback=self.errback_http,
                dont_filter=False,
                meta={'original_domain': domain, 'depth': current_depth + 1}
            )

        self.processed_count += 1
        if self.processed_count % 50 == 0:
            self.save_results()

    def errback_http(self, failure):
        url = failure.request.url
        domain = domain_from_url(url=url)
        self.error_counts[domain] = self.error_counts.get(domain, 0) + 1

        if self.error_counts[domain] == self.error_threshold:
            self.logger.warning(f"Excluding domain {domain} due to repeated request failures.")

    def save_results(self):
        results_file = '/tmp/results_emails.csv'

        with open(results_file, 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.writer(csvfile)
            writer.writerow(['url', 'emails'])
            for item in self.crawled_data:
                writer.writerow([item['url'], ', '.join(item['emails'])])

        self.s3.upload_file(results_file, self.bucket, self.output_file)

    def closed(self, reason):
        self.save_results()


def domain_from_url(url):
    parsed_uri = urlparse(url)
    domain = parsed_uri.netloc.lower()
    parts = domain.split('.')
    return '.'.join(parts[-2:])


def is_unwanted_url(url):
    unwanted_patterns = [
        'mailto:', 'javascript:', '.jpg', '.jpeg', '.png', '.gif', '.svg', '.webp',
        '.pdf', '.doc', '.docx', '.xls', '.xlsx', '.zip', '.rar', '.exe', '.tar', '.gz'
    ]
    return any(pattern in url.lower() for pattern in unwanted_patterns)


def is_login_page(url):
    login_patterns = ['/login', '/signin', '/register', '/auth', '/account']
    return any(pattern in url.lower() for pattern in login_patterns)
