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
from datetime import datetime
import os
from captcha_solver import click_checkbox, request_audio_version, solve_audio_captcha


class LinkedinScraper:
    def __init__(self):
        self._MAX_SEARCH_COUNT = 49  # max seems to be 49, and min seems to be 2
        self._MAX_REPEATED_REQUESTS = (
            200  # VERY conservative max requests count to avoid rate-limit
        )
        self.username = CREDS["username"]
        self.password = CREDS["password"]
        self.uname = self._get_accounts()
        self.num_of_worker = len(self.uname)
        self.data = pd.read_csv(BASE_DATA).to_dict("records")
        self.bypass_proxy = BYPASS_PROXY

    def _db_engine(self):
        def dict_factory(cursor, row):
            d = {}
            for idx, col in enumerate(cursor.description):
                d[col[0]] = row[idx]
            return d

        conn = sqlite3.connect(DB_NAME)
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

    def _get_linkedin_from_crunchbase(self):
        self.conn, self.cursor = self._db_engine()
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

        # init session
        client = requests.Session()
        cookies = cookiejar_from_dict(
            {
                "GOOGLE_ABUSE_EXEMPTION": "ID=4054c68c6aa230f7:TM=1679746947:C=r:IP=139.193.66.215-:S=3MCFnoc1wscnuxJ84t5B5WU"
            }
        )
        google_abuse_url = ""
        for i in self.data:
            startup_id = i["startup_uuid"]
            # skip scrape if its already scrapped before
            if len(scrapped_startup_uuid) > 1 and startup_id in scrapped_startup_uuid:
                continue
            organization_name = i["Organization Name"]
            # skip scrape on company with no linkedin founder
            if organization_name in [
                "12th Wonder",
                "3-dB Networks",
                "97 Display",
                "Akyumen Technologies Corp.",
                "ALECIA",
                "All Def Digital",
                "All Star Code",
                "Alpha Connect",
            ]:
                continue
            founder_details_raw = []
            crunchbase_url = (
                webcache_url + i["Organization Name URL"] + google_abuse_url
            )
            html = client.get(crunchbase_url, cookies=cookies).text
            # Throe exception if captcha detected
            if "About this page" in html:
                # playsound("F:\KERJA\BOT\LinkedInScraper\LinkedinScraper\song.mp3",)
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
                        # playsound(
                        #     "F:\KERJA\BOT\LinkedInScraper\LinkedinScraper\song.mp3",
                        # )
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

        # self.cursor.execute(f"SELECT * from ids where is_scrapped=1")
        # scrapped_id = self.cursor.fetchall()
        # logger.info(scrapped_id)
        # unscrapped_companies = [i["organization_name"] for i in scrapped_id]

    def _update_linkedin_url_json_scrapped_data(self, close_conn=False):
        logger.info("UPDATING LINKEDIN_URLS JSON SCRAPPED DATA")
        self.conn, self.cursor = self._db_engine()
        self.cursor.execute(f"SELECT * from ids where is_scrapped=1")
        scrapped_id = self.cursor.fetchall()
        file_name = "scrapped_linkedin_urls"
        with open(f"{file_name}.json", "w") as f:
            json.dump(scrapped_id, f)
        self.conn.close()

    def _linkedin_urls_worker(self, proxy, data_to_scrape):
        # init session
        proxy = []
        if not proxy:
            driver = Driver(uc=True, incognito=True)
        else:
            driver = Driver(uc=True, incognito=True, proxy=proxy)
        timeout = 100000
        client = requests.Session()
        cookies = cookiejar_from_dict({"GOOGLE_ABUSE_EXEMPTION": ""})
        google_abuse_url = ""
        webcache_url = "https://webcache.googleusercontent.com/search?q=cache:"
        founder_details = []
        try:
            for idx, i in enumerate(data_to_scrape):
                if not i:
                    continue
                organization_name = i["Organization Name"]
                # skip scrape on company with no linkedin founder
                # if organization_name in [
                # ]:
                #     continue
                founder_details_raw = []
                crunchbase_url = (
                    webcache_url + i["Organization Name URL"] + google_abuse_url
                )
                html = driver.get(crunchbase_url)
                invalid_company = False
                if (
                    "Your client does not have permission to get URL"
                    in driver.page_source
                ):
                    raise Exception(f"INVALID PROXIES WITH PROXY {proxy}")
                # if "About this page" in driver.page_source:
                #     logger.info(
                #         f"CAPTCHA DETECTED AT COMPANY {organization_name} at {crunchbase_url}"
                #     )
                # click_checkbox(driver)
                # try:
                #     request_audio_version(driver)
                #     solve_audio_captcha(driver)
                # except Exception as e:
                #     logger.info(f"ERROR SOLVING CAPTCHA {e}")
                #     pass
                title = driver.title
                if "About this page" not in driver.page_source:
                    response = requests.get(crunchbase_url)
                    if response.status_code == 404 or "Error 404" in title:
                        # logger.info(driver.page_source)
                        logger.info("MASUK")
                        logger.info(
                            f"INVALID COMPANY URL{organization_name} at {crunchbase_url}"
                        )
                        invalid_company = True
                # try:
                #     element = WebDriverWait(driver, timeout).until(
                #         EC.presence_of_element_located(
                #             (By.CLASS_NAME, "header-container")
                #         )
                #     )
                #     if element:
                #         invalid_company = False
                # except Exception as e:
                #     logger.info(f"ERROR REACHING {crunchbase_url}")
                if not invalid_company:
                    # Wait element to solve captcha manually
                    element = WebDriverWait(driver, timeout).until(
                        EC.presence_of_element_located(
                            (By.CLASS_NAME, "header-container")
                        )
                    )
                    html = driver.page_source
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
                            founder_html = driver.get(founder_url)
                            title = driver.title
                            if "About this page" not in driver.page_source:
                                response = requests.get(founder_url)
                                if response.status_code == 404 or "Error 404" in title:
                                    # logger.info(driver.page_source)
                                    logger.info(
                                        f"INVALID COMPANY URL 2 {organization_name} at {founder_url}"
                                    )
                                    continue
                            # if "About this page" in driver.page_source:
                            #     logger.info(
                            #         f"CAPTCHA DETECTED AT COMPANY {invalid_company} at {crunchbase_url}"
                            #     )
                            # click_checkbox(driver)
                            # try:
                            #     request_audio_version(driver)
                            #     solve_audio_captcha(driver)
                            # except Exception as e:
                            #     logger.info(f"ERROR SOLVING CAPTCHA {e}")
                            #     pass
                            element = WebDriverWait(driver, timeout).until(
                                EC.presence_of_element_located(
                                    (By.CLASS_NAME, "header-container")
                                )
                            )
                            founder_html = driver.page_source
                            soup = BeautifulSoup(founder_html, "html.parser")
                            founder_linkedin_url_list = []
                            for li in soup.select(
                                'li:-soup-contains("View on LinkedIn")'
                            ):
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
                    # logger.info(f"FOUNDER DETAILS RAW {founder_details_raw}")
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
                                        "is_scrapped_profile": item[
                                            "is_scrapped_profile"
                                        ],
                                    }
                                )
                                founder_names.add(name)
                                linkedin_urls.add(item["linkedin_url"])
                    if not processes_founder_details:
                        processes_founder_details.append(
                            {
                                "organization_name": i["Organization Name"],
                                "startup_uuid": i.get("startup_uuid", "kosong bro"),
                                "founder_name": "kosong bro",
                                "linkedin_url": "kosong bro",
                                "is_scrapped": 1,
                                "is_scrapped_profile": 0,
                            }
                        )
                # marks company with no founder's linkedin_url with 2
                if invalid_company:
                    processes_founder_details = [
                        {
                            "organization_name": i["Organization Name"],
                            "startup_uuid": i.get("startup_uuid", "kosong bro"),
                            "founder_name": "kosong bro",
                            "linkedin_url": "kosong bro",
                            "is_scrapped": 1,
                            "is_scrapped_profile": 0,
                        }
                    ]
                logger.info(f"process founder details {processes_founder_details}")
                founder_details += processes_founder_details
                logger.info(f"SCRAPED {idx}/{len(data_to_scrape)}")
            # insert data to after scrapping linkedin URL
            file_name = uuid.uuid4()
            with open(f"linkedin_urls_{file_name}.json", "w") as f:
                logger.info(f"SAVING DATA TO {file_name}.json ")
                json.dump(founder_details, f)
            driver.quit()
            return f"linkedin_urls_{file_name}.json"
        except Exception as e:
            file_name = uuid.uuid4()
            with open(f"linkedin_urls_{file_name}.json", "w") as f:
                logger.info(
                    f"ERROR OCCURED SAVING DATA TO {file_name}.json WITH ERROR {e}"
                )
                json.dump(founder_details, f)
            driver.quit()
            return f"linkedin_urls_{file_name}.json"

    def _scrape_linkedin_urls(self, batch_size=30, num_of_worker=10):
        self.proxies = self._get_proxy_list()
        with open("scrapped_linkedin_urls.json", "r") as f:
            scrapped_id = json.load(f)
        scrapped_startup_id = [i["startup_uuid"] for i in scrapped_id]
        filtered_data = [
            i for i in self.data if i["startup_uuid"] not in scrapped_startup_id
        ]
        batched_subList = self.batch_list_of_dict(filtered_data, batch_size)
        logger.info(f"DATA TO SCRAPE {len(filtered_data)}")
        logger.info(f"LEN BATCH {len(batched_subList)}")
        logger.info(f" LEN PROXY {len(self.proxies)}")
        if self.bypass_proxy:
            self.proxies = [i for i in range(num_of_worker)]
        # logger.info(f"BATCHED {batched_subList}")
        with multiprocessing.Pool(processes=len(self.proxies)) as pool:
            results = pool.starmap(
                self._linkedin_urls_worker, zip(self.proxies, batched_subList)
            )
        return results

    def _update_linkedin_url_db(self, file_names):
        logger.info("UPDATING DATA AT DB")
        self.conn, self.cursor = self._db_engine()
        total_data = 0
        for file in file_names:
            with open(file, "r") as f:
                data = json.load(f)
            startup_id = list(set([i["startup_uuid"] for i in data]))
            total_data += len(startup_id)
            rows = [
                (
                    d["organization_name"],
                    d["startup_uuid"],
                    d["founder_name"],
                    d["linkedin_url"],
                    d["is_scrapped"],
                    0,
                )
                for d in data
            ]
            try:
                self.cursor.executemany("INSERT INTO ids VALUES(?,?,?,?,?,?);", rows)
                self.conn.commit()
            except Exception as e:
                logger.info(e)
            os.remove(file)
        logger.info(f"TOTAL {total_data} scrapped")
        # Update latest data from DB to Json
        self._update_linkedin_url_json_scrapped_data()
        self.conn.close()

    def _scrape_profile(self, linkedin_url: str, driver=None) -> dict:
        """Fetch data for a given LinkedIn profile.

        :param linkedin_url: LinkedIn URL for a profile

        :return: Profile data
        :rtype: dict
        """
        # driver = Driver()
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
        # logger.info(results)
        # file_name = "recovery"
        # with open(f"{file_name}.json", "w") as f:
        #     json.dump(results, f)
        return results

    def _get_profile_id(self):
        self.proxies = self._get_proxy_list()
        # Get latest data from DB
        self.cursor.execute(f"SELECT * from ids where is_scrapped_profile=0")
        scrapped_id = self.cursor.fetchall()
        filtered_scrapped_id = [i for i in scrapped_id]

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

    def _process_scrapped_profile(self, file_name: str = "scrapped_profiles"):
        self.conn, self.cursor = self._db_engine()
        self.cursor.execute("SELECT * from profiles_raw")
        scrapped_profile = self.cursor.fetchall()
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
        scrapped_profile = self.data
        ordered_company_name = [i["Organization Name"] for i in scrapped_profile]
        ordered_company_name = list(set(ordered_company_name))
        df["Organization Name(Column A)"] = pd.Categorical(
            df["Organization Name(Column A)"],
            ordered=True,
            categories=ordered_company_name,
        )
        # df = df.sort_values("Organization Name(Column A)")
        df.to_csv(f"{file_name}.csv", index=False)

    def _get_proxy_list(self):
        proxy_file = "proxies.txt"
        proxies = []
        with open(proxy_file) as f:
            for line in f:
                proxies.append(line.strip())
        # working_proxy = []
        # logger.info("CHECKING WORKING PROXIES")
        # for proxy in proxies:
        #     try:
        #         # skip chinese/israel proxy
        #         if (
        #             proxy.startswith("45")
        #             # or proxy.startswith("193")
        #             # or proxy.startswith("83")
        #         ):
        #             continue
        #         response = requests.get(
        #             "http://example.com",
        #             proxies={"http": proxy, "https": proxy},
        #             timeout=0.5,
        #         )
        #         if response.status_code == 200:
        #             working_proxy.append(proxy)
        #             logger.info(len(working_proxy))
        #         if len(working_proxy) == 10:
        #             break
        #     except Exception as e:
        #         continue

        # if not working_proxy:
        #     logger.info("NO PROXY IS WORKING")
        # logger.info(f"WORKING PROXY {working_proxy}")
        return proxies

    def worker(self, proxy, username, filtered_scrapped_id):
        if self.bypass_proxy:
            proxy = []
            driver = Driver(uc=True, incognito=True)
        else:
            driver = Driver(uc=True, incognito=True, proxy=proxy)
        results = []
        # Create a new webdriver instance with the given proxy
        actions.login(
            driver, username, "delman12", timeout=10000
        )  # if email and password isnt given, it'll prompt in terminal
        # Loop your scrape function for num_loops iterations
        try:
            for idx, i in enumerate(filtered_scrapped_id):
                if not i:
                    continue
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
                logger.info(
                    f"SCRAPED {idx+1}/{len(filtered_scrapped_id)} with account {username}"
                )
            file_name = uuid.uuid4()
            with open(f"{file_name}.json", "w") as f:
                logger.info(f"SAVING DATA TO {file_name}.json ")
                json.dump(results, f)
            driver.quit()
            return file_name
        except Exception as e:
            logger.info(f"ERROR {e}")
            file_name = uuid.uuid4()
            logger.info(
                f"ERROR OCCURED SAVING DATA TO {file_name}.json at account {username}"
            )
            with open(f"{file_name}.json", "w") as f:
                json.dump(results, f)
            driver.quit()
            return file_name

    def _scrape_linkedin_profile(self, batch_size=30, num_of_worker: int = 10):
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
        batched_subList = self.batch_list_of_dict(filtered_scrapped_id, batch_size)
        logger.info(len(batched_subList))
        logger.info(len(uname))
        logger.info(len(proxies))

        if self.bypass_proxy:
            proxies = [i for i in range(num_of_worker)]
        with multiprocessing.Pool(processes=len(proxies)) as pool:
            results = pool.starmap(self.worker, zip(proxies, uname, batched_subList))
        return results

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

    def _update_profile_json_data(self, close_conn=False):
        logger.info(f"SAVING LATEST UNSCRAPPED DATA TO JSON")
        conn, cursor = ls._db_engine()
        # Update latest data from DB to Json
        cursor.execute(
            f"SELECT * from ids where is_scrapped_profile=0 and linkedin_url is not 'kosong bro'"
        )
        scrapped_id = cursor.fetchall()
        file_name = "invalid_acc.txt"
        exclude = []
        with open(file_name) as f:
            for line in f:
                exclude.append(line.strip())
        filtered_scrapped_id = [
            i for i in scrapped_id if i["linkedin_url"] not in exclude
        ]
        # filtered_scrapped_id = pd.read_csv("NotCollected_Solo.csv").to_dict("records")
        file_name = "filtered_scrapped"
        with open(f"{file_name}.json", "w") as f:
            json.dump(filtered_scrapped_id, f)
        self.total_account_to_scrape = len(filtered_scrapped_id)
        logger.info(f"TOTAL DATA TO SCRAPE {self.total_account_to_scrape}")
        # if close_conn:
        #     conn.close()

    def _update_profile_db_data(self, file_names: List):
        conn, cursor = ls._db_engine()
        logger.info(f"UPDATING LATEST DATA TO DB....")
        total_scrapped = 0
        for file in file_names:
            with open(f"{file}.json", "r") as f:
                data = json.load(f)
            total_scrapped += len(data)
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
            url = [(1, i["Linkedin Link"], i["uuid (Column B)"]) for i in data]
            cursor.executemany(
                "UPDATE ids SET is_scrapped_profile = ? WHERE linkedin_url = ? and startup_uuid = ?",
                url,
            )
            conn.commit()
            os.remove(f"{file}.json")
        # Update latest data from DB to Json
        self._update_profile_json_data()
        logger.info(
            f"TOTAL ACC SCRAPPED {total_scrapped} REMAINING TO SCRAPE {self.total_account_to_scrape-total_scrapped}"
        )

    def _get_invalid_url(self, file_name: str):
        conn, cursor = self._db_engine()
        cursor.execute(f"SELECT * from ids where is_scrapped_profile=0")
        scrapped_id = cursor.fetchall()
        df = pd.DataFrame(scrapped_id)
        df.to_csv(f"{file_name}.csv", index=False)

    def _validate_scrape_results(self):
        """Validate result by comparing the flag at table ids and data in profil_raw """
        conn, cursor = self._db_engine()
        # get data from filtered csv
        # missing_data = pd.read_csv("missing.csv").to_dict("records")
        # # # get data from ids with unscrapped flag
        # cursor.execute(f"SELECT * from ids where is_scrapped_profile=0")
        # invalid_urls = cursor.fetchall()
        # invalid_url_only = [i["linkedin_url"] for i in invalid_urls]

        # # filter out invalid urls from missing data
        # missing_data = [
        #     i for i in missing_data if i["linkedin_url"] not in invalid_url_only
        # ]

        # filter by scrapped profile
        cursor.execute(f"SELECT * from ids where is_scrapped_profile=1")
        scrapped_id_data = cursor.fetchall()
        y = set(list([i["linkedin_url"] for i in scrapped_id_data]))
        logger.info(len(y))
        # get data from profile_raw
        cursor.execute(f"SELECT * from profiles_raw")
        scrapped_profile = cursor.fetchall()
        x = set(list([i["linkedin_url"] for i in scrapped_profile]))
        logger.info(len(x))
        unscrapped_data = []
        for i in scrapped_id_data:
            if i["linkedin_url"] not in [
                x["linkedin_url"] for x in scrapped_profile
            ] and i["startup_uuid"] not in [
                x["startup_uuid"] for x in scrapped_profile
            ]:
                unscrapped_data.append(i)
        if not unscrapped_data:
            logger.info("ALL DATA IS GOOD")
            return
        logger.info(
            f"FOUND {len(unscrapped_data)} MISSING DATA {unscrapped_data}, REUPDATING FLAG AT TABLE IDS"
        )
        rows = [
            (
                d["organization_name"],
                d["startup_uuid"],
                d["founder_name"],
                d["linkedin_url"],
                1,
                0,
            )
            for d in unscrapped_data
        ]
        cursor.executemany("INSERT INTO ids VALUES(?,?,?,?,?,?);", rows)
        conn.commit()

        # update filtered_scrapped.json
        file_name = "filtered_scrapped"
        with open(f"{file_name}.json", "w") as f:
            json.dump(unscrapped_data, f)
        # close connection
        conn.close()

        # Scrape the unscrapped data
        file_names = self._scrape_linkedin_profile()
        self._update_profile_db_data(file_names)

    def _save_scrapped_linkedin_urls(
        self, file_name: str, invalid_company_file_name: str
    ):
        logger.info("SAVING LATEST SCRAPPED LINKEDIN_URLS TO CSV")
        self.conn, self.cursor = self._db_engine()
        self.cursor.execute(
            f"SELECT * from ids where is_scrapped=1 and linkedin_url is not 'kosong bro'"
        )
        scrapped_id = self.cursor.fetchall()
        # drop duplicates
        temp_set = set()
        # Create a new list of unique dictionaries using a for loop
        unique_dicts = []
        for d in scrapped_id:
            # Convert the dictionary to a string and add it to the temporary set
            d_str = str(sorted(d.items()))
            if d_str not in temp_set:
                temp_set.add(d_str)
                unique_dicts.append(d)
        self.cursor.execute(f"DELETE FROM ids")
        self.conn.commit()
        rows = [
            (
                d["organization_name"],
                d["startup_uuid"],
                d["founder_name"],
                d["linkedin_url"],
                d["is_scrapped"],
                0,
            )
            for d in unique_dicts
        ]
        self.cursor.executemany("INSERT INTO ids VALUES(?,?,?,?,?,?);", rows)
        self.conn.commit()
        res = [i for i in unique_dicts if i["linkedin_url"] != "kosong bro"]
        df = pd.DataFrame(unique_dicts)
        df.to_csv(f"{file_name}.csv", index=False)
        self.cursor.execute(f"SELECT * from ids where linkedin_url")
        scrapped_id = self.cursor.fetchall()
        scrapped_id = [i for i in scrapped_id if i["linkedin_url"] == "kosong bro"]
        df = pd.DataFrame(scrapped_id)
        df.to_csv(f"{invalid_company_file_name}.csv", index=False)

    def _get_accounts(self):
        acc_file = "accounts.txt"
        accounts = []
        with open(acc_file) as f:
            for line in f:
                accounts.append(line.strip())
        return accounts

    def _fix_solo(self):
        conn, cursor = self._db_engine()
        cursor.execute(f"SELECT * from profiles_raw")
        scrapped_profile = cursor.fetchall()
        rows = [(1, i["linkedin_url"]) for i in scrapped_profile]
        cursor.executemany(
            "UPDATE ids SET is_scrapped_profile = ? WHERE linkedin_url = ?", rows,
        )
        conn.commit()


if __name__ == "__main__":
    CREDS = {"username": "", "password": ""}
    DB_NAME = "linkedin_nonspawn.db"
    BASE_DATA = "NonSpawnedTeams_RACollection.csv"
    BYPASS_PROXY = True
    ls = LinkedinScraper()
    # GET LINKEDIN URLS FROM CRUNCHBASE
    # ls._get_linkedin_from_crunchbase()

    # GET LINKEDIN URLS FROM CRUNCHBASE WITH MP
    # ls._update_linkedin_url_json_scrapped_data(close_conn=True)
    # file_names = ls._scrape_linkedin_urls(batch_size=50, num_of_worker=10)
    # ls._update_linkedin_url_db(file_names)
    # ls._save_scrapped_linkedin_urls(
    #     file_name="scrapped_id_nonspawn",
    #     invalid_company_file_name="invalid_company_scrapped_id_nonspawn",
    # )
    # SCRAPE PROFILE DETAILS WITHOUT MULTIPROCESSING
    # profile_id = ls._get_profile_id()

    # SCRAPE PROFILE DETAILS WITH MULTIPROCESSING
    start = datetime.now()
    ls._update_profile_json_data(close_conn=True)
    file_names = ls._scrape_linkedin_profile(batch_size=40)
    ls._update_profile_db_data(file_names)
    end = datetime.now()
    logger.info(f"DUDRATIOOON {end-start}")

    # VALIDATE SCRAPE RESULTS
    # ls._validate_scrape_results()
    # # ls._fix_solo()

    # # SAVE PROCESSES DATA INTO CSV
    # ls._process_scrapped_profile(file_name="scrapped_profiles_solo")

    # # GET INVALID URLS
    # ls._get_invalid_url("invalid_urls_solo")
    # ls._scrape_profile("https://www.linkedin.com/in/vickytsai/")

