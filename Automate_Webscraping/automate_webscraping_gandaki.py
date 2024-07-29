# This script extracts voter data of Gandaki Province. Change values in state_index for other provinces. For e.g. range(1, 2) selects Koshi Province.

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import Select
from selenium.common.exceptions import NoSuchElementException
from bs4 import BeautifulSoup
import pandas as pd
from io import StringIO

# Initialize WebDriver
# driver = webdriver.Firefox()
driver = webdriver.Chrome()
driver.get("https://voterlist.election.gov.np/bbvrs1/index_2.php")
driver.implicitly_wait(0.2)

df = pd.DataFrame()

def scraper():
    global df 
    page_content = driver.page_source
    soup = BeautifulSoup(page_content, 'html.parser')
    table = soup.find('table', class_='bbvrs_data dataTable')
    converters = {'मतदाता नं': str}
    data = pd.read_html(StringIO(str(table)), converters=converters)[0]
    df = pd.concat([df, data], ignore_index=True)

def post_submit_actions():
    driver.execute_script("document.querySelector('select[name=\"tbl_data_length\"] option[value=\"100\"]').value = '10000';")
    entries_dropdown = driver.find_element(By.XPATH, '//select[@name="tbl_data_length"]')
    Select(entries_dropdown).select_by_value("10000")

    scraper()

# Loop through each combination 
for state_index in range(4, 5):  #(1,7+1) 
    try:
        for dist_index in range(1, 9): #(1, 14+1)
            try:
                for mun_index in range(1, 19): #(1, 18+1)
                    try:
                        for ward_index in range(1, 26): #(1, 33+1)
                            try:
                                for reg_centre_index in range(1, 10): #(1, 9+1)
                                    try:
                                        state_dropdown = driver.find_element(By.XPATH, '//select[@id="state"]')
                                        dist_dropdown = driver.find_element(By.XPATH, '//select[@id="district"]')
                                        mun_dropdown = driver.find_element(By.XPATH, '//select[@id="vdc_mun"]')
                                        ward_dropdown = driver.find_element(By.XPATH, '//select[@id="ward"]')
                                        reg_centre_dropdown = driver.find_element(By.XPATH, '//select[@id="reg_centre"]')
                                        submit_button = driver.find_element(By.ID, "btnSubmit")

                                        Select(state_dropdown).select_by_index(state_index)
                                        Select(dist_dropdown).select_by_index(dist_index)
                                        Select(mun_dropdown).select_by_index(mun_index)
                                        Select(ward_dropdown).select_by_index(ward_index)
                                        Select(reg_centre_dropdown).select_by_index(reg_centre_index)
                                        submit_button.click()

                                        post_submit_actions()
                                        
                                        go_back = driver.find_element(By.CLASS_NAME, "a_back")
                                        go_back.click()
                                        
                                    except NoSuchElementException:
                                        break
                            except NoSuchElementException:
                                break
                    except NoSuchElementException:
                        break
            except NoSuchElementException:
                break
    except NoSuchElementException:
        break

df.to_csv('gandaki.csv',  header=True,  index=False)
driver.quit()
