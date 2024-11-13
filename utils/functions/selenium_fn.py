from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service

def create_chromedriver(webdriver_path: str):
    """
    Arguments:
    - webdriver_path: path to webdriver

    Returns selenium webdriver
    """

    # set up options for headless mode
    webdriver_options = Options()
    webdriver_options.add_argument("--headless")  # run in headless mode
    webdriver_options.add_argument("--no-sandbox")  # overcome limited resource problems
    webdriver_options.add_argument("--disable-dev-shm-usage")  # overcome limited resource problems
    webdriver_options.add_argument("--dns-prefetch-disable")  # Disable DNS prefetching to reduce resolution issues
    webdriver_options.add_argument("--disable-gpu")  # Optional, sometimes helpful in headless mode


    
    # set up webdriver path
    webdriver_service = Service(executable_path=webdriver_path)

    # create webdriver
    driver = webdriver.Chrome(
        service=webdriver_service,
        options=webdriver_options
    )

    return driver