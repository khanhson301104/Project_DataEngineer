from pandas import DataFrame
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import pandas as pd
import datetime, time

driver_path = "D:\Project_DataEngineer\driver_seleinum\chromedriver-win64\chromedriver.exe"

service = Service(driver_path)
driver = webdriver.Chrome(service=service)
url = "https://www.pnj.com.vn/site/gia-vang?srslit=AfmBOorrbRBQlPRh1h17fC95Lvr5amO92vXloKMkSQdwkhY2GdSKyIM6"
driver.get(url)
time.sleep(10)
wait = WebDriverWait(driver, 10)

now = datetime.datetime.now()
today_str = f"{now.day:02d}/{now.month:02d}/{now.year}"


# city = ["Hà Nội,....."]

gold_date = driver.find_element(By.CSS_SELECTOR, "p.text-sm.text-gray-400.mt-1")
today_gold_date = gold_date.text[15:25]

def get_gold_price(city:str) -> DataFrame:
    table = driver.find_element(By.CSS_SELECTOR, "table.w-full.text-left.border-collapse")
    rows = table.find_elements(By.TAG_NAME, "tr")
    data = []
    for row in rows[1:]:
        cols = row.find_elements(By.TAG_NAME, "td")
        if len(cols) == 3:
            data.append([
                cols[0].text.strip(),  # Loại vàng
                cols[1].text.strip(),  # Giá mua
                cols[2].text.strip(),  # Giá bán
            ])
    df = pd.DataFrame(data, columns=["Loại vàng", "Giá mua", "Giá bán"])
    df["Date"] = today_gold_date
    df["Thành Phố"] = city
    return df


if today_str == today_gold_date:
    hcm_gold_data = get_gold_price("Hồ Chí Minh")

    dropdown = wait.until(EC.element_to_be_clickable((By.CSS_SELECTOR, "div.MuiInputBase-root")))
    dropdown.click()
    time.sleep(1)

    ha_noi_option = wait.until(EC.element_to_be_clickable((By.XPATH, "//li[contains(text(), 'Hà Nội')]")))
    ha_noi_option.click()
    time.sleep(20)

    hanoi_gold_data = get_gold_price("Hà Nội")

else:
    print("Invalid Date")



