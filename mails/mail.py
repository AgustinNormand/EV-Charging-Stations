from selenium import webdriver
import time
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.by import By
from webdriver_manager.chrome import ChromeDriverManager

driver = webdriver.Chrome(ChromeDriverManager().install())
driver.get("https://signup.mail.com/#.7518-header-signup1-1")

time.sleep(20)

elem = driver.find_element_by_xpath('/html/body/onereg-app/div/onereg-form/div/div/form/section/section[1]/onereg-alias/fieldset/onereg-progress-meter/div[2]/div[2]/div/pos-input[1]/input')
elem.send_keys("AgustinAutomatedYelp1")

"""
driver.find_element(By.ID, "wIdUsuario").send_keys(config["USER"])
elem = driver.find_element(By.ID, "wClaveUsuarioPlana")
elem.send_keys(config["PASSWORD"])
elem.send_keys(Keys.ENTER)
driver.find_element(By.ID, "link-1331").click()
driver.find_element_by_xpath('//a[@href="webmail.cgi?id_curso=1331"]').click()
driver.find_elements_by_class_name("lista_mails")[0].click()
for value in driver.find_elements_by_class_name("mensaje"):
    cleaned_text = str(value.get_attribute('innerHTML')).replace("<p>", "").replace("</p>", "").replace("&nbsp;", "").replace("<span>", "").replace("</span>", "")
    print("Mensaje del profesor: {}".format(cleaned_text))
    palabra = "nota"
    if palabra in cleaned_text.lower():
        print("Contiene la palabra {}".format(palabra))
    else:
        print("No contiene la palabra {}".format(palabra))
"""

driver.quit()
