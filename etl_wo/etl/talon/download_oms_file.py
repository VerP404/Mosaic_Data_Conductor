import os
import glob
import json
import shutil
import time
from datetime import datetime, timedelta
from dagster import asset, OpExecutionContext, Field, StringSource

from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

from etl_wo.config.config import config as env_config


def get_default_dates():
    """
    start_date: вчерашняя дата (дд-мм-гг)
    start_date_treatment: 01-01 текущего года (дд-мм-гг)
    """
    now = datetime.now()
    yesterday = now - timedelta(days=1)
    start_date = yesterday.strftime('%d-%m-%y')
    start_date_treatment = datetime(now.year, 1, 1).strftime('%d-%m-%y')
    return start_date, start_date_treatment


def wait_for_loading(driver):
    """
    Ждём, пока пропадёт надпись "Пожалуйста, подождите..."
    """
    from bs4 import BeautifulSoup
    while True:
        time.sleep(2)
        soup = BeautifulSoup(driver.page_source, 'html.parser')
        elem = soup.find('h2', {'class': 'jss170 jss176'})
        if not elem or elem.get_text().strip() != 'Пожалуйста, подождите...':
            break


def set_date_input(driver, xpath, date_value):
    """
    Пытается установить значение date_value в поле по xpath.
    Если значение не устанавливается, делает до 3 попыток.
    Возвращает True, если значение установлено корректно, иначе False.
    """
    input_elem = driver.find_element(By.XPATH, xpath)
    attempts = 0
    success = False
    while attempts < 3:
        input_elem.clear()
        input_elem.send_keys(date_value)
        time.sleep(1)
        actual = input_elem.get_attribute("value").strip()
        if actual == date_value:
            success = True
            break
        attempts += 1
    return success


def selenium_download_oms(username, password, start_date, start_date_treatment, browser, download_folder):
    if browser.lower() == 'chrome':
        from webdriver_manager.chrome import ChromeDriverManager
        from selenium.webdriver.chrome.options import Options as ChromeOptions
        from selenium.webdriver.chrome.service import Service as ChromeService
        from selenium import webdriver

        options = ChromeOptions()
        options.headless = True
        prefs = {
            "download.default_directory": download_folder,
            "download.prompt_for_download": False,
            "download.directory_upgrade": True,
            "safebrowsing.enabled": True
        }
        options.add_experimental_option("prefs", prefs)
        service = ChromeService(ChromeDriverManager().install())
        driver = webdriver.Chrome(service=service, options=options)
    elif browser.lower() == 'firefox':
        from webdriver_manager.firefox import GeckoDriverManager
        from selenium.webdriver.firefox.options import Options as FirefoxOptions
        from selenium.webdriver.firefox.service import Service as FirefoxService
        from selenium import webdriver

        options = FirefoxOptions()
        options.headless = True
        # Устанавливаем предпочтения для загрузки файла в указанную директорию
        options.set_preference("browser.download.folderList", 2)
        options.set_preference("browser.download.dir", download_folder)
        options.set_preference("browser.helperApps.neverAsk.saveToDisk",
                               "text/csv, application/csv, application/octet-stream")
        options.set_preference("pdfjs.disabled", True)
        service = FirefoxService(GeckoDriverManager().install())
        driver = webdriver.Firefox(options=options, service=service)
    else:
        raise ValueError("Поддерживаются только 'chrome' и 'firefox'")

    driver.implicitly_wait(10)
    driver.get('http://10.36.0.142:9000/')

    # Авторизация
    driver.find_element(By.XPATH, '/html/body/div/form/input[1]').clear()
    driver.find_element(By.XPATH, '/html/body/div/form/input[1]').send_keys(username)
    driver.find_element(By.XPATH, '/html/body/div/form/input[2]').clear()
    driver.find_element(By.XPATH, '/html/body/div/form/input[2]').send_keys(password)
    driver.find_element(By.XPATH, '/html/body/div/form/input[2]').send_keys(Keys.ENTER)
    time.sleep(5)
    wait = WebDriverWait(driver, 120)

    # Ввод даты начала лечения (start_date_treatment)
    treatment_xpath = '//*[@id="menu"]/div/div[1]/div/div[1]/div/div[2]/div[1]/div[1]/div/div/div/input'
    if not set_date_input(driver, treatment_xpath, start_date_treatment):
        context_message = f"Не удалось установить start_date_treatment. Ожидалось: {start_date_treatment}"
        # Выдаем предупреждение, но продолжаем
        print(context_message)

    # Открытие фильтра
    driver.find_element(By.XPATH, '/html/body/div[1]/div/div[2]/div[2]/div[1]/div/div[2]/div/div/div[1]/div[2]').click()
    driver.find_element(By.XPATH,
                        '/html/body/div[1]/div/div[2]/div[2]/div[1]/div/div[2]/div/div/div[2]/div/div/div/div/div[1]/div/div[1]/div[2]').click()

    # Ввод даты изменения (start_date)
    start_date_xpath = '//*[@id="menu"]/div/div[2]/div/div/div[2]/div/div/div/div/div[1]/div/div[2]/div/div/div/div/div/div/div[1]/div[7]/div/div[2]/div[1]/div/div/div/input'
    if not set_date_input(driver, start_date_xpath, start_date):
        context_message = f"Не удалось установить start_date. Ожидалось: {start_date}"
        print(context_message)
        # Вместо ошибки можно продолжить – либо использовать значение по умолчанию
    time.sleep(1)

    # Нажимаем "Найти"
    driver.find_element(By.XPATH, '//*[@id="menu"]/div/div[1]/div/div[4]/div/div[4]/div/button').click()
    wait_for_loading(driver)
    time.sleep(5)
    wait.until(EC.invisibility_of_element_located((By.XPATH,
                                                   '//h2[contains(@class, "jss170") and contains(@class, "jss176") and text()="Пожалуйста, подождите..."]')))
    time.sleep(5)

    # Выбираем все результаты
    driver.find_element(By.XPATH,
                        '//*[@id="root"]/div/div[2]/div[2]/div[2]/div/div[2]/table[1]/thead/tr[1]/th[1]/span').click()

    # Нажимаем кнопку скачивания
    driver.find_element(By.XPATH, '//*[@id="menu"]/div/div[3]/div/div/div[5]/a/button').click()
    time.sleep(5)

    # Ищем скачанный файл
    file_pattern = os.path.join(download_folder, "journal_*.csv")
    files = glob.glob(file_pattern)
    if files:
        latest_file = max(files, key=os.path.getctime)
        # Переименовываем файл с использованием текущей временной метки
        timestamp = time.strftime("%Y%m%d_%H%M%S")
        new_filename = f"journal_{timestamp}.csv"
        new_file_path = os.path.join(download_folder, new_filename)
        shutil.move(latest_file, new_file_path)
        driver.quit()
        return True, new_file_path
    else:
        driver.quit()
        return False, None


@asset(
    config_schema={
        "download_mode": Field(StringSource, default_value="auto", is_required=False),
        "browser": Field(StringSource, default_value="chrome", is_required=False),
        "organization": Field(StringSource, default_value="default", is_required=False)
    }
)
def talon_download_oms_file(context: OpExecutionContext) -> str:
    """
    1) Перед запуском очищаем папку etl_wo/data
    2) Если download_mode == "manual", ищем journal_*.csv в папке
    3) Если "auto", запускаем Selenium и скачиваем файл (переименовываем в journal_YYYYMMDD_HHMMSS.csv)
    4) Возвращаем путь к файлу
    """
    data_folder = os.path.join(os.getcwd(), "etl_wo", "data")
    if not os.path.exists(data_folder):
        os.makedirs(data_folder)

    # Получаем организацию с дефолтным значением "local"
    organizations = env_config.get("organizations", {})
    org = context.op_config.get("organization", "default")
    local_config = organizations.get(org, {})
    selenium_conf = local_config.get("selenium", {})

    download_mode = selenium_conf.get("download_mode", "auto")
    browser = selenium_conf.get("browser", "chrome")
    oms_username = selenium_conf.get("oms_username")
    oms_password = selenium_conf.get("oms_password")

    if download_mode == "manual":
        # Ручной режим: ищем уже имеющийся файл journal_*.csv
        file_pattern = os.path.join(data_folder, "journal_*.csv")
        files = glob.glob(file_pattern)
        if not files:
            text_value = "Файл не найден в etl_wo/data по шаблону 'journal_*.csv'."
            context.log.info(text_value)
            raise ValueError(text_value)
        latest_file = max(files, key=os.path.getctime)
        return latest_file
    else:
        # (1) Очищаем папку data
        for f in os.listdir(data_folder):
            fp = os.path.join(data_folder, f)
            if os.path.isfile(fp):
                os.remove(fp)
        # Автоматический режим: скачиваем файл через Selenium
        start_date, start_date_treatment = get_default_dates()
        success, file_path = selenium_download_oms(
            oms_username, oms_password,
            start_date, start_date_treatment,
            browser, data_folder
        )
        if not success or not file_path:
            text_value = "Selenium не смог скачать файл или папка пуста."
            context.log.info(text_value)
            raise ValueError(text_value)

        return file_path
