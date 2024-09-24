from utils.functions.selenium_fn import create_webdriver
import logging

def main():

    # initialize driver
    driver = create_webdriver()

    logging.info("Driver created.")

    # open url in browser
    base_url = 'https://www.minorleaguesplits.com/tennisabstract/cgi-bin/jstourneys'
    driver.get(base_url)

    # close driver
    driver.quit()

if __name__ == "__main__":
    main()