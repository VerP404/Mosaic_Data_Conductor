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
from selenium.webdriver import ActionChains

from etl_wo.config.config import config as env_config


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


def selenium_download_people_iszl(username, password, browser, download_folder):
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
    driver.get('http://10.36.29.2:8087/')

    def click_xpath(xpath):
        perehod = driver.find_element(By.XPATH, xpath)
        perehod.click()

    def input_enter_xpath(xpath, text):
        perehod = driver.find_element(By.XPATH, xpath)
        perehod.send_keys(text, Keys.ENTER)

    def autorizacia():
        # ввод логина и пароля
        login_input = driver.find_element(By.XPATH, '//*[@id="tbLogin"]')
        login_input.clear()
        login_input.send_keys('360025-04')
        try:
            try:
                input_enter_xpath('//*[@id="tbPwd"]', 'b#v+r|@c')
            except:
                input_enter_xpath('//*[@id="tbPwd"]', 'b#v+r|@c')
        except:
            try:
                input_enter_xpath('//*[@id="tbPwd"]', 'b#v+r|@c')
            except:
                input_enter_xpath('//*[@id="tbPwd"]', 'b#v+r|@c')

    autorizacia()
    time.sleep(5)
    wait = WebDriverWait(driver, 120)

    # Открываем меню и выбираем нужный пункт
    clickable = driver.find_element(By.XPATH, '//*[@id="mnuAtt"]/ul/li[1]')
    ActionChains(driver) \
        .click_and_hold(clickable) \
        .perform()
    # Находим родительский элемент и наводим курсор, чтобы отобразилось подменю
    clickable = driver.find_element(By.XPATH, '//*[@id="mnuAtt"]/ul/li[1]')
    ActionChains(driver).move_to_element(clickable).perform()

    # Ждем, пока элемент подменю станет кликабельным
    wait = WebDriverWait(driver, 10)
    submenu = wait.until(EC.element_to_be_clickable((By.XPATH, '//*[@id="mnuAtt:submenu:2"]/li[3]/a')))
    submenu.click()
    time.sleep(1)

    # Переход во внутренний фрейм
    perehod_v_frame = driver.find_element(By.XPATH, '//*[@id="ifMain"]')
    driver.switch_to.frame(perehod_v_frame)
    time.sleep(1)

    # Открываем форму выгрузки данных
    click_xpath('//*[@id="lbtnExternalData"]')
    time.sleep(2)

    # Переходим во фрейм модального окна
    perehod_v_frame = driver.find_element(By.XPATH, '//*[@id="ifDic"]')
    driver.switch_to.frame(perehod_v_frame)
    time.sleep(1)

    # Нажимаем Экспорт данных
    click_xpath('//*[@id="lbtnExport"]')

    # Нажимаем Получить файл
    click_xpath('//*[@id="lbtnFile"]')

    # Ищем скачанный файл
    file_pattern = os.path.join(download_folder, "Att_MO_36*.csv")
    files = glob.glob(file_pattern)
    if files:
        latest_file = max(files, key=os.path.getctime)
        # Переименовываем файл с использованием текущей временной метки
        timestamp = time.strftime("%Y%m%d_%H%M%S")
        new_filename = f"Att_MO_36{timestamp}.csv"
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
def download_pzl_people_file(context: OpExecutionContext) -> str:
    """
    1) Перед запуском очищаем папку etl_wo/data/people
    2) Если download_mode == "manual", ищем Att_MO_36*.csv в папке
    3) Если "auto", запускаем Selenium и скачиваем файл (переименовываем в Att_MO_36_YYYYMMDD_HHMMSS.csv)
    4) Возвращаем путь к файлу
    """
    data_folder = os.path.join(os.getcwd(), "etl_wo", "data", "people")
    if not os.path.exists(data_folder):
        os.makedirs(data_folder)

    # Получаем организацию с дефолтным значением "local"
    organizations = env_config.get("organizations", {})
    org = context.op_config.get("organization", "default")
    local_config = organizations.get(org, {})
    selenium_conf = local_config.get("selenium", {})

    download_mode = selenium_conf.get("download_mode", "auto")
    browser = selenium_conf.get("browser", "chrome")
    oms_username = selenium_conf.get("pzl_username")
    oms_password = selenium_conf.get("pzl_password")

    if download_mode == "manual":
        # Ручной режим: ищем уже имеющийся файл Att_MO_36*.csv
        file_pattern = os.path.join(data_folder, "Att_MO_36*.csv")
        files = glob.glob(file_pattern)
        if not files:
            text_value = "Файл не найден в etl_wo/data по шаблону 'Att_MO_36*.csv'."
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
        success, file_path = selenium_download_people_iszl(
            oms_username, oms_password,
            browser, data_folder
        )
        if not success or not file_path:
            text_value = "Selenium не смог скачать файл или папка пуста."
            context.log.info(text_value)
            raise ValueError(text_value)

        return file_path
