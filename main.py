import requests
import pandas as pd
from requests.cookies import cookiejar_from_dict
from time import sleep
import random
from services import logger
import sqlite3
from urllib.parse import urlencode
import undetected_chromedriver as uc
from bs4 import BeautifulSoup
import pickle
from fake_useragent import UserAgent
from typing import List
import csv
from playsound import playsound
import re
from operator import itemgetter
import datetime
import json
from linkedin_scraper import Person, actions
import ast
from seleniumbase import Driver
import random
from fake_useragent import UserAgent
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from threading import Thread
import uuid
import multiprocessing

CREDS = {"username": "jhordy@delman.io", "password": "delman12"}
# {"username": "ph.rk.h.rap.hr@gmail.com", "password": "delman12"}
# {"username": "jhordy@delman.io", "password": "delman12"}
username_list = ["izyklveeuy@bloheyz.com", "cu3yevow3l@zipcatfish.com"]


class LinkedinScraper:
    def __init__(self):
        self._MAX_SEARCH_COUNT = 49  # max seems to be 49, and min seems to be 2
        self._MAX_REPEATED_REQUESTS = (
            200  # VERY conservative max requests count to avoid rate-limit
        )
        self.username = CREDS["username"]
        self.password = CREDS["password"]
        self.uname = username_list
        self.num_of_worker = len(self.uname)
        self.data = pd.read_excel("LinkedIn_RACollection_3045_RA.xlsx").to_dict(
            "records"
        )

    def _db_engine(self):
        def dict_factory(cursor, row):
            d = {}
            for idx, col in enumerate(cursor.description):
                d[col[0]] = row[idx]
            return d

        conn = sqlite3.connect("linkedin.db")
        conn.row_factory = dict_factory
        cursor = conn.cursor()
        cursor.execute(
            """CREATE TABLE IF NOT EXISTS ids (
            organization_name DATATYPE TEXT,
            startup_uuid DATATYPE TEXT,
            founder_name DATATYPE TEXT,
            linkedin_url DATATYPE TEXT,
            is_scrapped DATATYPE INTEGER,
            is_scrapped_profile DATATYPE INTEGER
        )"""
        )
        cursor.execute(
            """CREATE TABLE IF NOT EXISTS profiles_raw (
            organization_name DATATYPE TEXT,
            startup_uuid DATATYPE TEXT,
            name_from_col_e DATATYPE TEXT,
            linkedin_name DATATYPE TEXT,
            experience DATATYPE TEXT,
            education DATATYPE TEXT,
            profile_image DATATYPE TEXT,
            linkedin_url DATATYPE TEXT
        )"""
        )
        cursor.execute(
            """CREATE TABLE IF NOT EXISTS deleted_account (
            organization_name DATATYPE TEXT,
            startup_uuid DATATYPE TEXT,
            founder_name DATATYPE TEXT,
            linkedin_url DATATYPE TEXT,
            is_scrapped DATATYPE INTEGER,
            is_scrapped_profile DATATYPE INTEGER
        )"""
        )
        return conn, cursor

    def _save_to_csv(self, fieldnames: List, data: List, filename: str):
        # open the CSV file for writing
        with open(filename, "w", newline="", encoding="utf-8") as csvfile:
            # create a CSV writer object
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            # write the header row
            writer.writeheader()
            # write the data rows
            for row in data:
                writer.writerow(row)

    def _driver(self):
        chrome_options = uc.ChromeOptions()
        return uc.Chrome(options=chrome_options)

    def _get_linkedin_from_crunchbase(self, insert_raw_data=False):
        if insert_raw_data:
            # insert founders to DB
            rows = [
                (d["Organization Name"], d["startup_uuid"], founder, "empty", 0, 0)
                for d in self.data
                for founder in d["Founders"].split(", ")
            ]
            logger.info(rows)
            self.cursor.executemany("INSERT INTO ids VALUES(?,?,?,?,?,?);", rows)
            self.conn.commit()
            logger.info("DONE UPLOADING RAW DATA")
            return
        # get scrapped id
        self.cursor.execute(f"SELECT * from ids where is_scrapped=1")
        scrapped_id = self.cursor.fetchall()
        # logger.info(scrapped_id)
        fields = [
            "organization_name",
            "startup_uuid",
            "founder_name",
            "linkedin_url",
            "is_scrapped",
            "is_scrapped_profile",
        ]
        # save to csv
        self._save_to_csv(fields, scrapped_id, "scrapped_id.csv")
        scrapped_startup_uuid = [i["startup_uuid"] for i in scrapped_id]
        logger.info(scrapped_id)
        webcache_url = "https://webcache.googleusercontent.com/search?q=cache:"
        founder_details = []

        # check unscrapped user
        self.cursor.execute("SELECT * from ids where is_scrapped=0")
        unscrapped_id = self.cursor.fetchall()
        unscrapped_companies = [i["organization_name"] for i in unscrapped_id]
        logger.info(list(set(unscrapped_companies)))
        logger.info(len(unscrapped_id))

        # init session
        client = requests.Session()
        cookies = cookiejar_from_dict(
            {
                "GOOGLE_ABUSE_EXEMPTION": "ID=6237f06690b38ef4:TM=1678848583:C=r:IP=139.193.66.215-:S=QbamS_Bpks8BydOmmGH7Ngw"
            }
        )
        google_abuse_url = ""
        for i in self.data:
            startup_id = i["startup_uuid"]
            # skip scrape if its already scrapped before
            if startup_id in scrapped_startup_uuid:
                continue
            organization_name = i["Organization Name"]
            # skip scrape on company with no linkedin founder
            if organization_name in [
                "Circuit of The Americas",
                "Real Estate Elevated",
                "Adaptive US Inc.",
                "Oseberg",
                "POMCO",
                "Constellation Pharmaceuticals",
                "Postup",
                "Bia",
                "UZURV, LLC",
                "Represent",
                "Iterable",
                "Bellhop",
                "Vera",
                "Skorpios Technologies",
                "Headspace",
                "Alloy Digital",
                "Compound Photonics",
                "Eldridge Industries",
                "Hanger",
                "Essential",
                "SEM RPM",
                "Beats Music",
                "Sera Prognostics",
                "TutorMe",
                "BrideClick",
                "Temboo",
                "Simtek",
            ]:
                continue
            founder_details_raw = []
            crunchbase_url = (
                webcache_url + i["Organization Name URL"] + google_abuse_url
            )
            html = client.get(crunchbase_url, cookies=cookies).text
            # Throe exception if captcha detected
            if "About this page" in html:
                playsound("F:\KERJA\BOT\LinkedInScraper\LinkedinScraper\song.mp3",)
                raise Exception("CAPTCHA DETECTED")
            logger.info(f"Scrapping {organization_name}")
            soup = BeautifulSoup(html, "html.parser")
            founders_list = []
            for li in soup.select('li:-soup-contains("Founders")'):
                for a in li.select("a.link-accent[href]"):
                    founder_url = (
                        webcache_url
                        + "https://www.crunchbase.com"
                        + a["href"]
                        + google_abuse_url
                    )
                    founder_html = client.get(founder_url, cookies=cookies).text
                    if "About this page" in founder_html:
                        logger.info("CAPTCHA DETECTED")
                        playsound(
                            "F:\KERJA\BOT\LinkedInScraper\LinkedinScraper\song.mp3",
                        )
                        raise Exception("CAPTCHA DETECTED")

                    soup = BeautifulSoup(founder_html, "html.parser")
                    founder_linkedin_url_list = []
                    for li in soup.select('li:-soup-contains("View on LinkedIn")'):
                        founders_list.append(a.text.strip())
                        for a in li.select("a.link-accent[href]"):
                            founder_linkedin_url = a["href"]
                            founder_details_raw.append(
                                {
                                    "organization_name": i["Organization Name"],
                                    "startup_uuid": i["startup_uuid"],
                                    "founder_name": founders_list,
                                    "linkedin_url": founder_linkedin_url,
                                    "is_scrapped": 1,
                                    "is_scrapped_profile": 0,
                                }
                            )
            logger.info(founder_details_raw)
            # drop duplicates name in founder_details
            processes_founder_details = []
            founder_names = set()
            linkedin_urls = set()
            for item in founder_details_raw:
                for name in item["founder_name"]:
                    if (
                        name not in founder_names
                        and item["linkedin_url"] not in linkedin_urls
                    ):
                        processes_founder_details.append(
                            {
                                "organization_name": item["organization_name"],
                                "startup_uuid": item["startup_uuid"],
                                "founder_name": name,
                                "linkedin_url": item["linkedin_url"],
                                "is_scrapped": item["is_scrapped"],
                                "is_scrapped_profile": item["is_scrapped_profile"],
                            }
                        )
                        founder_names.add(name)
                        linkedin_urls.add(item["linkedin_url"])
            # insert data to after scrapping linkedin URL
            rows = [
                (
                    d["organization_name"],
                    d["startup_uuid"],
                    d["founder_name"],
                    d["linkedin_url"],
                    1,
                    0,
                )
                for d in processes_founder_details
            ]
            self.cursor.executemany("INSERT INTO ids VALUES(?,?,?,?,?,?);", rows)
            self.conn.commit()
            founder_details += processes_founder_details

        self.cursor.execute(f"SELECT * from ids where is_scrapped=1")
        scrapped_id = self.cursor.fetchall()
        logger.info(scrapped_id)
        unscrapped_companies = [i["organization_name"] for i in scrapped_id]

    def load(self, driver):
        driver.execute_cdp_cmd("Network.enable", {})
        with open(self.filename, mode="rb") as f:
            for cookie in pickle.load(f):
                driver.execute_cdp_cmd("Network.setCookie", cookie)
        driver.execute_cdp_cmd("Network.disable", {})

    def _scrape_profile(self, linkedin_url: str, driver=None) -> dict:
        """Fetch data for a given LinkedIn profile.

        :param linkedin_url: LinkedIn URL for a profile

        :return: Profile data
        :rtype: dict
        """
        # driver = uc.Chrome()
        # actions.login(driver, self.username, self.password, timeout=30)
        sleep(0.5)
        person = Person(linkedin_url, driver=driver, scrape=False, contacts=[])
        person.scrape(close_on_complete=False)
        d = person.__dict__.copy()
        if not person.name:
            return None
        del d["driver"]
        d["experiences"] = [experience.__dict__ for experience in person.experiences]
        d["educations"] = [education.__dict__ for education in person.educations]
        name_raw = person.name.split("\n")[0]
        name = re.sub(r"(.)([A-Z][a-z]+)", r"\1 \2", name_raw)
        results = {
            "name": name,
            "experiences": d["experiences"],
            "educations": d["educations"],
            "profile_dp_link": d["profile_picture"],
        }
        return results

    def _get_profile_id(self):
        self.proxies = self._get_proxy_list()
        # people with invalid LinkedIn URL
        exclude = [
            "http://www.linkedin.com/in/johnayers1",
            "https://www.linkedin.com/in/petercli89",
            "https://www.linkedin.com/in/cedric-mcdougal-he-him-2b99021a",
            "https://www.linkedin.com/in/dalecheney",
            "https://www.linkedin.com/in/stephen-h-gordon-7740ab167",
            "https://www.linkedin.com/in/sumitkn",
            "https://www.linkedin.com/in/david-chou-51921a37/",
            "https://www.linkedin.com/in/shuey-robert-86167651",
            "https://www.linkedin.com/in/xantanner/",
            "http://www.linkedin.com/pub/david-carel/76/abb/270",
            "https://www.linkedin.com/in/barrett-glasauer-91a8844a/",
            "https://in.linkedin.com/in/amrita-jash-27358270",
            "http://www.linkedin.com/pub/quinn-hu/61/417/641",
            "http://www.linkedin.com/in/kirkgreen01",
            "http://www.linkedin.com/in/carfreebrad",
            "https://www.linkedin.com/in/khiladi-gurjar-732aaa178",
            "https://www.linkedin.com/in/mike-la-monica-34562b27/",
            "http://www.linkedin.com/in/jstnc",
            "https://www.linkedin.com/in/eddie-reyes/",
            "https://hr.linkedin.com/in/sandromur",
            "https://www.linkedin.com/in/midge-seltzer-049b79ab/",
            "https://linkedin.com/in/huntermckinley",
            "https://www.linkedin.com/in/jtnelson1",
            "http://www.linkedin.com/in/kumarshiv89",
            "http://www.linkedin.com/in/scottswanson77",
            "https://www.linkedin.com/in/martinstroka/",
            "http://www.linkedin.com/in/bwanbo",
            "https://www.linkedin.com/in/erika-jensen-06598260/",
            "http://www.linkedin.com/in/cluefulsoftwareengineer",
            "https://www.linkedin.com/in/jonathanmarsico",
            "https://www.linkedin.com/in/ken-baker-758bbab/",
            "https://www.linkedin.com/in/crescojoe/",
            "https://www.linkedin.com/in/xeeton/",
            "http://www.linkedin.com/in/casperisto",
            "http://in.linkedin.com/in/sriharimaneru/en",
            "https://www.linkedin.com/in/nathansri1",
            "https://www.linkedin.com/in/tayo-ademuyiwa-md-345923187",
            "http://www.linkedin.com/pub/brendan-duhamel/98/3b6/4ba",
            "https://www.linkedin.com/in/mark-wahlberg-1a8424219",
            "https://www.linkedin.com/in/john-birkmeyer-md-5413b038/",
            "https://www.linkedin.com/in/evsharp",
            "https://www.linkedin.com/in/matthewpattoli",
            "https://www.linkedin.com/in/bcmsbond/",
            "https://www.linkedin.com/in/entic/",
            "https://www.linkedin.com/in/vinod-dham-b07a7935",
            "http://www.linkedin.com/pub/evan-huang/44/6a1/99a",
            "https://www.linkedin.com/in/matthew-schulman-15911861/",
            "http://www.linkedin.com/pub/dan-kaminsky/0/614/532",
            "https://www.linkedin.com/in/lydiafayal",
            "http://www.linkedin.com/in/stephenchristopherliu",
            "https://www.linkedin.com/in/michaelbdagostino/",
            "https://www.linkedin.com/in/vladimir-hruda-1939502/",
            "https://www.linkedin.com/in/ramiro-almeida-7975b363",
            "https://www.linkedin.com/",
        ]
        # Get latest data from DB
        self.cursor.execute(f"SELECT * from ids where is_scrapped_profile=0")
        scrapped_id = self.cursor.fetchall()
        filtered_scrapped_id = [
            i for i in scrapped_id if i["linkedin_url"] not in exclude
        ]

        # Create webdriver with first proxy in list
        self.proxies = self._get_proxy_list()
        proxy_index = 0
        logger.info(self.proxies)
        proxy = self.proxies[proxy_index]

        # Use proxies in rotation with each request
        results = []
        random_loop = random.randint(200, 400)
        counter = 0
        sleep_counter = 0
        driver = None
        for i in range(10000):
            logger.info(f"SCRAPING {random_loop} account with proxy {proxy}")
            if driver:
                driver.quit()
            driver = Driver(uc=True, incognito=True, proxy=proxy)
            actions.login(
                driver, self.username, self.password, timeout=1000
            )  # if email and password isnt given, it'll prompt in terminal
            for i in filtered_scrapped_id[:random_loop]:
                linkedin_url = i["linkedin_url"]
                logger.info(f"SCRAPPING {linkedin_url}")
                id_details = self._scrape_profile(linkedin_url, driver)
                if not id_details:
                    row = tuple(i.values())
                    self.cursor.execute(
                        "INSERT INTO deleted_account VALUES(?,?,?,?,?,?);", row
                    )
                    self.conn.commit()
                    counter += 1
                    continue
                result = {
                    "Organization Name(Column A)": i["organization_name"],
                    "uuid (Column B)": i["startup_uuid"],
                    "Name from Column E": i["founder_name"],
                    "LinkedIn Name": id_details["name"],
                    "experience": str(id_details["experiences"]),
                    "education": str(id_details["educations"]),
                    "Profile Image URL": id_details["profile_dp_link"],
                    "Linkedin Link": i["linkedin_url"],
                }

                # update scrapped data to DB
                row = tuple(result.values())
                self.cursor.execute(
                    "INSERT INTO profiles_raw VALUES(?,?,?,?,?,?,?,?);", row
                )
                self.conn.commit()
                sleep(0.3)
                url = i["linkedin_url"]
                self.cursor.execute(
                    "UPDATE ids SET is_scrapped_profile = 1 WHERE linkedin_url = ?",
                    (url,),
                )
                self.conn.commit()
                sleep(0.3)
                results.append(result)
                counter += 1
                sleep_counter += 1
                logger.info(f"TOTAL DATA {sleep_counter}")
                # if sleep_counter % 10 == 0:
                #     logger.info("SLEEPING FOR 3 MINS EVERY 10 PROFILE SCRAPPED")
                #     logger.info(f"TOTAL ACCOUNT SCRAPPED {sleep_counter}")
                #     sleep(180)
            logger.info(f"COUNT {counter}")
            if counter == random_loop:
                counter = 0
                # Update webdriver options with new proxy
                proxy_index += 1
                if proxy_index == len(self.proxies):
                    proxy_index = 0
                proxy = self.proxies[proxy_index]

                # update latest filtered id
                self.cursor.execute(f"SELECT * from ids where is_scrapped_profile=0")
                scrapped_f = self.cursor.fetchall()
                filtered_scrapped_id = [
                    i for i in scrapped_f if i["linkedin_url"] not in exclude
                ]
                logger.info(f"FILTER {filtered_scrapped_id[0]}")
                random_loop = random.randint(200, 400)
                sleep(0.2)
                driver.quit()

        logger.info(results)
        with open("data.json", "w") as f:
            json.dump(results, f)
        return results

    def extract_time_period(self, timeperiod: dict):
        start_date_year = timeperiod["startDate"]["year"]
        start_date_month = ""
        if timeperiod.get("startDate") and timeperiod.get("startDate").get("month"):
            start_date_month = (
                datetime.date(1900, timeperiod["startDate"]["month"], 1).strftime("%B")
                + "/"
            )
        start_date = start_date_month + str(start_date_year) + " - "
        is_end_date_exist = timeperiod.get("endDate", None)
        if is_end_date_exist:
            end_date_year = timeperiod["endDate"]["year"]
            end_date_month = ""
            if timeperiod.get("endDate") and timeperiod.get("endDate").get("month"):
                end_date_month = month = (
                    datetime.date(1900, timeperiod["endDate"]["month"], 1).strftime(
                        "%B"
                    )
                    + "/"
                )
            end_date = end_date_month + str(end_date_year)
            period1 = start_date + end_date
            period2 = str(end_date_year - start_date_year) + " yrs"
        else:
            period1 = start_date
            period2 = "Still Working"
        return period1, period2

    def _process_scrapped_profile(self):
        self.conn, self.cursor = self._db_engine()
        self.cursor.execute("SELECT * from profiles_raw")
        scrapped_profile = self.cursor.fetchall()
        f = open("data.json")
        results = []
        for i in scrapped_profile:
            base_result = []
            exp_list = []
            experiences = ast.literal_eval(i["experience"])
            for exp in experiences:
                start_date = exp.get("from_date")
                end_date = exp.get("to_date")
                duration = exp.get("duration")
                if start_date and end_date:
                    period1 = (
                        (start_date + " To " + end_date.replace("Saat ini", "Present"))
                        .replace("ini", "Present")
                        .replace("Ini", "Present")
                    )
                elif start_date and not end_date:
                    period1 = start_date
                duration = exp.get("duration")
                if not duration and start_date and end_date:
                    start_date_num = re.findall(r"\d+", start_date)
                    end_date_num = re.findall(r"\d+", end_date)
                    if end_date == "Present":
                        end_date_num = [2023]
                    if end_date_num:
                        duration = (
                            str(int(end_date_num[0]) - int(start_date_num[0])) + " yrs"
                        )
                if duration:
                    duration = duration.replace("thn", "yrs").replace("bln", "mos")
                if duration and start_date and not end_date:
                    duration_num = re.findall(r"\d+", duration)
                    start_date_num = re.findall(r"\d+", start_date)
                    if duration_num:
                        end_date = int(start_date_num[0]) + int(duration_num[0])
                        period1 = str(start_date_num[0]) + " To " + str(end_date)
                if period1:
                    check_period1_valid = re.findall(r"\d+", period1)
                    if not check_period1_valid:
                        period1 = None
                company_name = exp.get("institution_name", None)
                if company_name:
                    company_name = company_name.replace("\n", " ").replace("\r", "")
                    if "·" in company_name:
                        # remove unwanted char
                        company_name = company_name[: company_name.rfind("·")]
                    # remove duplicate sub string
                    com_name_in_list = company_name.split()
                    company_name = " ".join(
                        sorted(set(com_name_in_list), key=com_name_in_list.index)
                    )
                company_loc = exp.get("location", None)
                if company_loc:
                    is_num = re.findall(r"\d+", company_loc)
                    if len(is_num) > 1:
                        period1_n_duration = company_loc.split("·")
                        if not period1 and not duration:
                            period1 = period1_n_duration[0].strip().replace("Â", "")
                            if len(period1_n_duration) > 1:
                                duration = period1_n_duration[1].strip()
                            company_loc = None
                    if company_loc:
                        if "·" in company_loc:
                            company_loc = company_loc[: company_loc.rfind("·")]

                result = {
                    "Company Name": company_name,
                    "Company Location": company_loc,
                    "Company URL": exp.get("linkedin_url", None),
                    "Position(Job Title)": exp.get("position_title", None),
                    "Period 1": period1,
                    "Period 2": duration,
                }
                exp_list.append(result)
            edu_list = []
            educations = ast.literal_eval(i["education"])
            for edu in educations:
                start_date = edu.get("from_date")
                end_date = edu.get("to_date")
                degree = edu.get("degree")
                duration = None
                if start_date and end_date:
                    period1 = (
                        start_date
                        + " To "
                        + end_date.replace("Saat ini", "Present")
                        .replace("Ini", "Present")
                        .replace("ini", "Present")
                    )
                    start_date_num = re.findall(r"\d+", start_date)
                    end_date_num = re.findall(r"\d+", end_date)
                    duration = (
                        str(int(end_date_num[0]) - int(start_date_num[0])) + " yrs"
                    )
                elif start_date and not end_date:
                    period1 = start_date
                else:
                    period1 = None
                if degree and not start_date and not end_date:
                    num = re.findall(r"\d+", degree)
                    if len(num) > 1:
                        period1 = degree
                        duration = str(int(num[1]) - int(num[0])) + " yrs"
                        degree = None

                result = {
                    "Education": edu.get("institution_name", None),
                    "Period 1 Edu": period1,
                    "Period 2 Edu": duration,
                    "Degree": degree,
                }
                edu_list.append(result)
            # build base for exp and edu
            if len(exp_list) > len(edu_list):
                longer_list = exp_list
                shorter_list = edu_list
            else:
                longer_list = edu_list
                shorter_list = exp_list
            for d in range(len(longer_list)):
                if d < len(shorter_list):
                    longer_list[d].update(shorter_list[d])
                else:
                    if shorter_list:
                        shorter_list = [{key: None for key in d} for d in shorter_list]
                        longer_list[d].update(shorter_list[0])

            for x in longer_list:
                profile_image = i["profile_image"]
                if "data:image/gif" in profile_image:
                    profile_image = "No Profile Image"
                base_dict = {
                    "Organization Name(Column A)": i["organization_name"],
                    "uuid (Column B)": i["startup_uuid"],
                    "Name from Column E": i["name_from_col_e"],
                    "LinkedIn Name": i["linkedin_name"],
                    "Profile Image URL": profile_image,
                }
                base_dict.update(x)
                base_result.append(base_dict)
            # append linkedin_url
            for data in base_result:
                data.update({"Linkedin Link": i["linkedin_url"]})

            results += base_result

        # write to csv
        df = pd.DataFrame(results)
        # order company name by ordered value from original data
        self.cursor.execute(
            f"SELECT organization_name from ids where is_scrapped_profile=1"
        )
        scrapped_profile = self.cursor.fetchall()
        ordered_company_name = [i["organization_name"] for i in scrapped_profile]
        ordered_company_name = list(set(ordered_company_name))
        df["Organization Name(Column A)"] = pd.Categorical(
            df["Organization Name(Column A)"],
            ordered=True,
            categories=ordered_company_name,
        )
        # df = df.sort_values("Organization Name(Column A)")
        df.to_csv("scrapped_profiles.csv", index=False)
        # write to csv
        # fields = list(results[0].keys())
        # self._save_to_csv(fields, results, "scrapped_profiles.csv")

    def _get_proxy_list(self):
        proxy_file = "proxies.txt"
        proxies = []
        with open(proxy_file) as f:
            for line in f:
                proxies.append(line.strip())
        working_proxy = []
        logger.info("CHECKING WORKING PROXIES")
        for proxy in proxies:
            try:
                # skip chinese proxy
                if proxy.startswith("45"):
                    continue
                response = requests.get(
                    "http://example.com",
                    proxies={"http": proxy, "https": proxy},
                    timeout=0.5,
                )
                if response.status_code == 200:
                    working_proxy.append(proxy)
                    logger.info(len(working_proxy))
                if len(working_proxy) == 20:
                    break
            except Exception as e:
                logger.info(f"ERROR ON {proxy}: {e}")
                continue

        if not working_proxy:
            logger.info("NO PROXY IS WORKING")
        logger.info(f"WORKING PROXY {working_proxy}")
        return working_proxy

    def worker(self, proxy, username, filtered_scrapped_id):
        results = []
        num_loops = 23
        # Create a new webdriver instance with the given proxy
        driver = Driver(uc=True, incognito=True, proxy=proxy)
        actions.login(
            driver, username, "delman12", timeout=10000
        )  # if email and password isnt given, it'll prompt in terminal
        # Loop your scrape function for num_loops iterations
        try:
            for i in filtered_scrapped_id[:num_loops]:
                linkedin_url = i["linkedin_url"]
                logger.info(f"SCRAPPING {linkedin_url}")
                id_details = self._scrape_profile(linkedin_url, driver)
                if not id_details:
                    continue
                result = {
                    "Organization Name(Column A)": i["organization_name"],
                    "uuid (Column B)": i["startup_uuid"],
                    "Name from Column E": i["founder_name"],
                    "LinkedIn Name": id_details["name"],
                    "experience": str(id_details["experiences"]),
                    "education": str(id_details["educations"]),
                    "Profile Image URL": id_details["profile_dp_link"],
                    "Linkedin Link": i["linkedin_url"],
                }
                sleep(0.3)
                results.append(result)
            file_name = uuid.uuid4()
            with open(f"{file_name}.json", "w") as f:
                logger.info(f"SAVING DATA TO {file_name}.json ")
                json.dump(results, f)
        except Exception as e:
            logger.info(f"ERROR {e}")
            file_name = uuid.uuid4()
            logger.info(f"ERROR OCCURED SAVING DATA TO {file_name}.json")
            with open(f"{file_name}.json", "w") as f:
                json.dump(results, f)
        driver.quit()

    def _scrape_linkedin_profile(self, num_of_worker: int = None):
        self.proxies = self._get_proxy_list()
        num_of_worker = num_of_worker or self.num_of_worker
        # Create webdriver with first proxy in list
        proxy_index = 0
        proxies = self.proxies[:num_of_worker]
        # Use proxies in rotation with each request
        results = []
        processes = []
        uname = self.uname
        with open("filtered_scrapped.json", "r") as f:
            filtered_scrapped_id = json.load(f)
        batched_subList = self.batch_list_of_dict(filtered_scrapped_id, 33)
        logger.info(len(batched_subList))
        logger.info(len(uname))
        logger.info(len(proxies))

        def process_worker(proxy, username, profile):
            self.worker(proxy, username, profile)

        with multiprocessing.Pool(processes=len(proxies)) as pool:
            pool.starmap(self.worker, zip(proxies, uname, batched_subList))

    def batch_list_of_dict(self, lst, batch_size=23):
        # Pad the list with empty dictionaries to make its length a multiple of the batch size
        num_missing_items = batch_size - (len(lst) % batch_size)
        lst += [{}] * num_missing_items

        # Create batches of the desired size
        num_batches = len(lst) // batch_size
        batches = [
            lst[i * batch_size : (i + 1) * batch_size] for i in range(num_batches)
        ]

        return batches

    def _update_db_data(self, conn, cursor, file_names: List):
        logger.info(f"UPDATING LATEST DATA TO DB....")
        for file in file_names:
            with open(f"{file}.json", "r") as f:
                data = json.load(f)
            # update scrapped data to DB
            row = [
                (
                    d["Organization Name(Column A)"],
                    d["uuid (Column B)"],
                    d["Name from Column E"],
                    d["LinkedIn Name"],
                    d["experience"],
                    d["education"],
                    d["Profile Image URL"],
                    d["Linkedin Link"],
                )
                for d in data
            ]
            cursor.executemany("INSERT INTO profiles_raw VALUES(?,?,?,?,?,?,?,?);", row)
            conn.commit()
            sleep(0.3)
            url = [(1, i["Linkedin Link"]) for i in data]
            cursor.executemany(
                "UPDATE ids SET is_scrapped_profile = ? WHERE linkedin_url = ?", url
            )
            conn.commit()

            # Get latest data from DB
            cursor.execute(f"SELECT * from ids where is_scrapped_profile=0")
            scrapped_id = cursor.fetchall()

            exclude = [
                "http://www.linkedin.com/in/johnayers1",
                "https://www.linkedin.com/in/petercli89",
                "https://www.linkedin.com/in/cedric-mcdougal-he-him-2b99021a",
                "https://www.linkedin.com/in/dalecheney",
                "https://www.linkedin.com/in/stephen-h-gordon-7740ab167",
                "https://www.linkedin.com/in/sumitkn",
                "https://www.linkedin.com/in/david-chou-51921a37/",
                "https://www.linkedin.com/in/shuey-robert-86167651",
                "https://www.linkedin.com/in/xantanner/",
                "http://www.linkedin.com/pub/david-carel/76/abb/270",
                "https://www.linkedin.com/in/barrett-glasauer-91a8844a/",
                "https://in.linkedin.com/in/amrita-jash-27358270",
                "http://www.linkedin.com/pub/quinn-hu/61/417/641",
                "http://www.linkedin.com/in/kirkgreen01",
                "http://www.linkedin.com/in/carfreebrad",
                "https://www.linkedin.com/in/khiladi-gurjar-732aaa178",
                "https://www.linkedin.com/in/mike-la-monica-34562b27/",
                "http://www.linkedin.com/in/jstnc",
                "https://www.linkedin.com/in/eddie-reyes/",
                "https://hr.linkedin.com/in/sandromur",
                "https://www.linkedin.com/in/midge-seltzer-049b79ab/",
                "https://linkedin.com/in/huntermckinley",
                "https://www.linkedin.com/in/jtnelson1",
                "http://www.linkedin.com/in/kumarshiv89",
                "http://www.linkedin.com/in/scottswanson77",
                "https://www.linkedin.com/in/martinstroka/",
                "http://www.linkedin.com/in/bwanbo",
                "https://www.linkedin.com/in/erika-jensen-06598260/",
                "http://www.linkedin.com/in/cluefulsoftwareengineer",
                "https://www.linkedin.com/in/jonathanmarsico",
                "https://www.linkedin.com/in/ken-baker-758bbab/",
                "https://www.linkedin.com/in/crescojoe/",
                "https://www.linkedin.com/in/xeeton/",
                "http://www.linkedin.com/in/casperisto",
                "http://in.linkedin.com/in/sriharimaneru/en",
                "https://www.linkedin.com/in/nathansri1",
                "https://www.linkedin.com/in/tayo-ademuyiwa-md-345923187",
                "http://www.linkedin.com/pub/brendan-duhamel/98/3b6/4ba",
                "https://www.linkedin.com/in/mark-wahlberg-1a8424219",
                "https://www.linkedin.com/in/john-birkmeyer-md-5413b038/",
                "https://www.linkedin.com/in/evsharp",
                "https://www.linkedin.com/in/matthewpattoli",
                "https://www.linkedin.com/in/bcmsbond/",
                "https://www.linkedin.com/in/entic/",
                "https://www.linkedin.com/in/vinod-dham-b07a7935",
                "http://www.linkedin.com/pub/evan-huang/44/6a1/99a",
                "https://www.linkedin.com/in/matthew-schulman-15911861/",
                "http://www.linkedin.com/pub/dan-kaminsky/0/614/532",
                "https://www.linkedin.com/in/lydiafayal",
                "http://www.linkedin.com/in/stephenchristopherliu",
                "https://www.linkedin.com/in/michaelbdagostino/",
                "https://www.linkedin.com/in/vladimir-hruda-1939502/",
                "https://www.linkedin.com/in/ramiro-almeida-7975b363",
                "https://www.linkedin.com/in/lscott3/",
                "https://www.linkedin.com/in/chrisloefflerii",
                "https://www.linkedin.com/in/peter-szulczewski-b711221",
                "https://www.linkedin.com/in/vincentyangeverstring/",
                "https://www.linkedin.com/in/james-smith-81b9766/",
                "https://www.linkedin.com/in/jessica-gordon-43b03359/",
                "https://www.linkedin.com/in/wrong9999999",
                "https://sg.linkedin.com/pub/iris-sangalang-ramos/10/b45/678",
                "http://www.linkedin.com/pub/tom-lorimor/5a/115/598",
                "https://www.linkedin.com/in/richardcline",
                "https://www.linkedin.com/in/ilya-kupershmidt",
                "https://www.linkedin.com/in/sanjayjain/",
                "https://www.linkedin.com/in/jameslim1",
                "https://www.linkedin.com/in/catherine-baker-b1860365",
                "https://www.linkedin.com/in/jennifer-igartua-a3aba016/",
                "https://www.linkedin.com/in/jason-meltzer-6231932",
                "https://www.linkedin.com/in/wencesc/",
                "https://www.linkedin.com/in/wencesc/",
                "https://www.linkedin.com/in/bradbao"
                "https://www.linkedin.com/in/cooperkathy/",
                "https://www.linkedin.com/in/jennifer-m-grigsby-cpa-cgma-mba-22320196/",
                "https://www.linkedin.com/in/caencontee",
                "https://linkedin.com/in/rpatools",
                "https://www.linkedin.com/in/iqram-magdon-ismail-803511184/",
                "http://www.linkedin.com/in/suneliot",
                "https://www.linkedin.com/in/jasonv2",
                "https://www.linkedin.com/in/cooperkathy/",
                "https://www.linkedin.com/in/mahadikvinay/",
                "https://www.linkedin.com/in/jasonreichl/",
            ]
            filtered_scrapped_id = [
                i for i in scrapped_id if i["linkedin_url"] not in exclude
            ]
            file_name = "filtered_scrapped"
            with open(f"{file_name}.json", "w") as f:
                json.dump(filtered_scrapped_id, f)

    def _get_invalid_url(self):
        conn, cursor = self._db_engine()
        cursor.execute(f"SELECT * from ids where is_scrapped_profile=0")
        scrapped_id = cursor.fetchall()
        df = pd.DataFrame(scrapped_id)
        df.to_csv("invalid_urls.csv", index=False)


if __name__ == "__main__":
    ls = LinkedinScraper()
    # GET LINKEDIN URLS FROM CRUNCHBASE
    # ls._get_linkedin_from_crunchbase()

    # SCRAPE PROFILE DETAILS WITHOUT MULTIPROCESSING
    # profile_id = ls._get_profile_id()

    # SCRAPE PROFILE DETAILS WITH MULTIPROCESSING
    # ls._scrape_linkedin_profile()

    # UPDATE SCRAPED DATA INTO DB
    # conn, cursor = ls._db_engine()
    # file_names = [
    #     "2450e1d1-da32-4d94-a4e6-73cd84652a72",
    # ]
    # ls._update_db_data(conn, cursor, file_names)

    # SAVE PROCESSES DATA INTO CSV
    # ls._process_scrapped_profile()

    # GET INVALID URLS
    # ls._get_invalid_url()

