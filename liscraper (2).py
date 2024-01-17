from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.common.exceptions import NoSuchElementException
import csv
import time

webdriver_path = "C:/chromedriver/chromedriver.exe"
MAX_CONCURRENT_REQUESTS = 5
RETRY_LIMIT = 3

urls = []
failed_urls = []
retry_queue = []

# Read URLs from the input CSV
with open('profileurl.csv', 'r') as file:
    reader = csv.reader(file)
    next(reader)  # Skip the header row
    for row in reader:
        urls.append(row[0])  # Assuming URL is the first column

# Open the output CSV file for writing
with open('output.csv', 'w', newline='', encoding='utf-8') as output_file:
    writer = csv.writer(output_file)
    writer.writerow(['URL', 'Name', 'Job Title', 'Location', 'Followers', 'Current Company',
                     'Current Company Profile URL', 'About'])

    # Iterate over the URLs
    for index, url in enumerate(urls, start=1):
        print(f"Crawling URL {index}/{len(urls)}: {url}")

        options = Options()
        options.add_argument("--incognito")
        options.add_argument("--headless") 

        driver = webdriver.Chrome(webdriver_path, options=options)

        try:
            retries = 0
            success = False

            while retries < RETRY_LIMIT:
                driver.get(url)
                time.sleep(5)  # Optional: wait for 5 seconds for the page to fully load

                # Here you can add your own scraping logic.
                # Selenium provides a way to locate elements, similar to Puppeteer.
                try:
                    name = driver.find_element(By.CSS_SELECTOR, '.top-card-layout__title').get_attribute(
                        'innerText').strip()
                    job_title = driver.find_element(By.CSS_SELECTOR, '.top-card-layout__headline').get_attribute(
                        'innerText').strip()
                    location = driver.find_element(By.CSS_SELECTOR, '.top-card__subline-item').get_attribute(
                        'innerText').strip()
                    followers = driver.find_element(By.CSS_SELECTOR, '.top-card__subline-item:nth-child(2)').get_attribute(
                        'innerText').strip()
                    current_company = driver.find_element(By.CSS_SELECTOR, '.top-card-link__description').get_attribute(
                        'innerText').strip()
                    current_company_profile_url = driver.find_element(By.CSS_SELECTOR,
                                                                      '.top-card-link--link').get_attribute('href')
                    website = driver.find_element(By.CSS_SELECTOR, '.top-card-link__description:last-child').get_attribute(
                        'innerText').strip()
                    about = driver.find_element(By.CSS_SELECTOR, 'h3.top-card-layout__first-subline').get_attribute(
                        'innerText').strip()

                    # Write the scraped data to the output CSV
                    writer.writerow([url, name, job_title, location, followers, current_company,
                                     current_company_profile_url, about])
                    success = True
                    break
                except NoSuchElementException as e:
                    print("Error processing {}: {}".format(url, str(e)))
                    retries += 1

            if not success:
                failed_urls.append(url)
            else:
                # Reset the retry count if scraping is successful
                retries = 0

        finally:
            driver.quit()  # This will close the browser window and clear all session data

            # Add the URL to the retry queue if retries are remaining
            if retries < RETRY_LIMIT:
                retry_queue.append(url)

        # Check if the retry queue has URLs and the maximum concurrent requests limit is reached
        if retry_queue and len(retry_queue) % MAX_CONCURRENT_REQUESTS == 0:
            print("Retrying failed URLs (Retry {}/{}):".format(retries + 1, RETRY_LIMIT))
            for retry_url in retry_queue:
                print("Crawling URL: {}".format(retry_url))
                # Perform the retry using the same logic as above

            retry_queue.clear()  # Clear the retry queue after performing retries

        # Flush the output file buffer to ensure immediate writing
        output_file.flush()

# Write the failed URLs to a separate CSV file
with open('failed.csv','w', newline='', encoding='utf-8') as failed_file:
    writer = csv.writer(failed_file)
    writer.writerows([[url] for url in failed_urls])
