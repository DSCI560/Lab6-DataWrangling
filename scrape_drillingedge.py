import mysql.connector
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.firefox.service import Service
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.firefox import GeckoDriverManager

db_config = {
    "host": "localhost",
    "user": "root",
    "password": "admin123",
    "database": "oil_wells"
}

search_url = "https://www.drillingedge.com/search"

def get_all_apis():
    conn = mysql.connector.connect(**db_config)
    cursor = conn.cursor()
    cursor.execute("SELECT api FROM wells WHERE api IS NOT NULL")
    rows = cursor.fetchall()
    cursor.close()
    conn.close()
    return [r[0] for r in rows if r[0]]

def create_driver():
    options = webdriver.FirefoxOptions()
    options.set_preference(
        "general.useragent.override",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:117.0) Gecko/20100101 Firefox/117.0"
    )
    service = Service(GeckoDriverManager().install())
    driver = webdriver.Firefox(service=service, options=options)
    return driver

def search_api(driver, api_number):
    wait = WebDriverWait(driver, 30)

    driver.get(search_url)

    api_input = wait.until(
        EC.presence_of_element_located((By.NAME, "api_no"))
    )

    api_input.clear()
    api_input.send_keys(api_number)

    submit_btn = driver.find_element(
        By.XPATH, "//input[@type='submit' and @value='Search Database']"
    )
    submit_btn.click()

    wait.until(
        EC.presence_of_element_located(
            (By.XPATH, "//table[contains(@class,'interest_table')]//tr[td]")
        )
    )

    rows = driver.find_elements(
        By.XPATH, "//table[contains(@class,'interest_table')]//tr"
    )

    headers = [h.text.strip() for h in rows[0].find_elements(By.TAG_NAME, "th")]
    print(" | ".join(headers))

    for r in rows[1:]:
        cols = [c.text.strip() for c in r.find_elements(By.TAG_NAME, "td")]
        if cols:
            print(" | ".join(cols))

    print("-" * 80)

def main():
    apis = get_all_apis()
    print(f"Found {len(apis)} APIs in database\n")

    driver = create_driver()


    try:
        for api in apis:
            print(f"Searching API: {api}")
            search_api(driver, api)
    finally:
        driver.quit()

if __name__ == "__main__":
    main()