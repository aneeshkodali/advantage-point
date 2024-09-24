from selenium import webdriver
from selenium.webdriver.chrome.service import Service

def create_webdriver():
    
    # set up driver
    service = Service(executable_path='./webdriver/chromedriver.exe')
    driver = webdriver.Chrome(service=service)

    return driver