import pickle
import selenium.webdriver

cookies = pickle.load(open("cookies2.pkl", "rb"))
for cookie in cookies:
    print("a")
    print(cookie)

cookies = pickle.load(open("cookies3.pkl", "rb"))
for cookie in cookies:
    print("b")
    print(cookie)
