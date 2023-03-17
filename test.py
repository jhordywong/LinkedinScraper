import undetected_chromedriver as uc
from time import sleep
from linkedin_scraper import Person, actions, JobSearch, Job
from selenium import webdriver

# driver = webdriver.Chrome()
driver = uc.Chrome()
all_users = {}
email = "jhoewong49@gmail.com"
password = "ikacantik2302"
actions.login(
    driver, email, password, timeout=50
)  # if email and password isnt given, it'll prompt in terminal
sleep(0.5)
line = "https://www.linkedin.com/in/kaitki-agarwal-4685942/"
job = Job(
    "https://www.linkedin.com/jobs/search/?currentJobId=3500726464&geoId=102436504&location=Z%C3%BCrich%2C%20Schweiz&refresh=true&sortBy=R",
    driver=driver,
    close_on_complete=False,
)
# person = Person(line, driver=driver, scrape=False)
# person.scrape(close_on_complete=False)
# d = person.__dict__.copy()
# del d["driver"]
# d["experiences"] = [experience.__dict__ for experience in person.experiences]
# d["educations"] = [education.__dict__ for education in person.educations]
# all_users[person.name] = d  # saves it all to one giant dict
print(job)
# print(all_users)

