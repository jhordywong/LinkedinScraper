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

CREDS = [
    {"username": "jhoewong49@gmail.com", "password": "ikacantik2302"},
    {"username": "diviyi7246@rolenot.com", "password": "delman12"},
]


class LinkedinScraper:
    def __init__(self):
        self._MAX_SEARCH_COUNT = 49  # max seems to be 49, and min seems to be 2
        self._MAX_REPEATED_REQUESTS = (
            200  # VERY conservative max requests count to avoid rate-limit
        )
        self.list_of_li_at = self._get_li_at()
        self.li_at = self.list_of_li_at[0]
        self.data = pd.read_excel("LinkedIn_RACollection_3045_RA.xlsx").to_dict(
            "records"
        )
        self.client, self.cookies = self._build_client_and_cookies()
        # self.driver = self._driver()
        # init DB
        self.conn, self.cursor = self._db_engine()

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
        return conn, cursor

    def _logged_in_user(self, username, password):
        """Method to build client with li_at value from users"""
        AUTH_REQUEST_HEADERS = {
            "X-Li-User-Agent": "LIAuthLibrary:3.2.4 com.linkedin.LinkedIn:8.8.1 iPhone:8.3",  # noqa
            "User-Agent": "LinkedIn/8.8.1 CFNetwork/711.3.18 Darwin/14.0.0",
            "X-User-Language": "en",
            "X-User-Locale": "en_US",
            "Accept-Language": "en-us",
        }
        client = requests.Session()
        client.get(
            "https://www.linkedin.com/uas/authenticate", headers=AUTH_REQUEST_HEADERS,
        )
        payload = {
            "session_key": username,
            "session_password": password,
            "JSESSIONID": client.cookies["JSESSIONID"],
        }
        res = client.post(
            "https://www.linkedin.com/uas/authenticate",
            data=payload,
            cookies=client.cookies,
            headers=AUTH_REQUEST_HEADERS,
        )

        data = res.json()
        print(data)
        if data and data["login_result"] == "PASS":
            res = client.cookies.get_dict()["li_at"]
            return res

    def _get_li_at(self):
        """method to get list of li_at for scrape jobs if li_at is expired based on provided creds"""
        LI_AT = []
        for c in CREDS:
            LI_AT.append(
                {
                    "username": c["username"],
                    "li_at": self._logged_in_user(c["username"], c["password"]),
                }
            )
        return [i["li_at"] for i in LI_AT if i["li_at"] != None]

    def _build_client_and_cookies(self):
        """Method to build client with li_at value from users"""
        AUTH_REQUEST_HEADERS = {
            "X-Li-User-Agent": "LIAuthLibrary:3.2.4 com.linkedin.LinkedIn:8.8.1 iPhone:8.3",  # noqa
            "User-Agent": "LinkedIn/8.8.1 CFNetwork/711.3.18 Darwin/14.0.0",
            "X-User-Language": "en",
            "X-User-Locale": "en_US",
            "Accept-Language": "en-us",
        }
        client = requests.Session()
        client.get(
            "https://www.linkedin.com/uas/authenticate", headers=AUTH_REQUEST_HEADERS,
        )
        cookies = cookiejar_from_dict(
            {
                "liap": "true",
                "JSESSIONID": client.cookies["JSESSIONID"],
                "li_at": self.li_at,
            }
        )
        return client, cookies

    def _default_evade():
        """
        A catch-all method to try and evade suspension from Linkedin.
        Currenly, just delays the request by a random (bounded) time
        """
        sleep(random.randint(2, 5))

    def _fetch(
        cls, uri, evade=_default_evade, api_url=True, for_alumni=False, **kwargs
    ):
        """GET request to Linkedin API"""

        API_BASE_URL = "https://www.linkedin.com/voyager/api"
        if not api_url:
            API_BASE_URL = "https://www.linkedin.com"
        evade()

        url = f"{API_BASE_URL}{uri}"
        res = cls.client.get(url, **kwargs)
        if for_alumni:
            return res
        retries = 0
        while res.status_code in (403, 429):
            if retries == cls.len_list_of_li_at:
                raise (
                    f"failed to fetch after several times : error code {res.status_code}"
                )
            logger.info(f"failed to fetch {res.status_code} Retry fetching...")
            evade()
            # recreate client with difference li_at if we've got captcha
            cls.li_at = cls._get_li_at_by_idx(retries + 1)
            res = cls.client.get(url, **kwargs)
            retries += 1
        if res.status_code == 200:
            return res
        else:
            logger.info(f"FAILED TO FETCH {res.status_code}")

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

    def _scrape_profile_id(self, insert_raw_data=False):
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
        # logger.info(list(set(unscrapped_companies)))
        # logger.info(len(scrapped_id))

    def load(self, driver):
        driver.execute_cdp_cmd("Network.enable", {})
        with open(self.filename, mode="rb") as f:
            for cookie in pickle.load(f):
                driver.execute_cdp_cmd("Network.setCookie", cookie)
        driver.execute_cdp_cmd("Network.disable", {})

    def _scrape_profile(self, public_id: str) -> dict:
        """Fetch data for a given LinkedIn profile.

        :param public_id: LinkedIn public ID for a profile

        :return: Profile data
        :rtype: dict
        """
        res = self._fetch(
            f"/identity/profiles/{public_id}/profileView",
            cookies=self.cookies,
            headers={"csrf-token": self.cookies["JSESSIONID"].strip('"'),},
        )
        if not res:
            return
        data = res.json()
        logger.info(data)
        if data and "status" in data and data["status"] != 200:
            logger.info(
                "request failed: {}, with id {}".format(data["message"], public_id)
            )
            return {}

        # message [profile] data
        profile = data["profile"]
        if "miniProfile" in profile:
            if "picture" in profile["miniProfile"]:
                images_data = profile["miniProfile"]["picture"][
                    "com.linkedin.common.VectorImage"
                ]["artifacts"]
                for img in images_data:
                    w, h, url_segment = itemgetter(
                        "width", "height", "fileIdentifyingUrlPathSegment"
                    )(img)
                    profile[f"img_{w}_{h}"] = url_segment
                profile["profile_dp_link"] = (
                    profile["miniProfile"]["picture"][
                        "com.linkedin.common.VectorImage"
                    ]["rootUrl"]
                    + profile["img_800_800"]
                )
            del profile["miniProfile"]

        # message [experience] data
        experience = data["positionView"]["elements"]
        for item in experience:
            if "company" in item and "miniCompany" in item["company"]:
                if "logo" in item["company"]["miniCompany"]:
                    logo = item["company"]["miniCompany"]["logo"].get(
                        "com.linkedin.common.VectorImage"
                    )
                    if logo:
                        item["companyLogoUrl"] = logo["rootUrl"]
                del item["company"]["miniCompany"]
            if "$anti_abuse_metadata" in item:
                del item["$anti_abuse_metadata"]
            del item["entityUrn"]

        # message [education] data
        education = data["educationView"]["elements"]
        for item in education:
            if "school" in item:
                if "logo" in item["school"]:
                    item["school"]["logoUrl"] = item["school"]["logo"][
                        "com.linkedin.common.VectorImage"
                    ]["rootUrl"]
                    del item["school"]["logo"]

        # message [languages] data
        languages = data["languageView"]["elements"]
        for item in languages:
            del item["entityUrn"]

        # message [publications] data
        publications = data["publicationView"]["elements"]
        for item in publications:
            del item["entityUrn"]
            for author in item.get("authors", []):
                del author["entityUrn"]

        # message [certifications] data
        certifications = data["certificationView"]["elements"]
        for item in certifications:
            del item["entityUrn"]

        # message [volunteer] data
        volunteer = data["volunteerExperienceView"]["elements"]
        for item in volunteer:
            del item["entityUrn"]

        # build profile information
        result = {
            "name": profile["firstName"] + " " + profile["lastName"],
            "headline": profile.get("headline", None),
            "profile_dp_link": profile.get("profile_dp_link", None),
            "summary": profile.get("summary", None),
            "location": profile.get("locationName", None),
            "industryName": profile.get("industryName", None),
            "experiences": experience,
            "education": education,
            "languages": languages,
            "publications": publications,
            "certifications": certifications,
            "volunteer": volunteer,
            "id": public_id,
        }

        return result

    def _get_profile_id(self):
        self.cursor.execute(f"SELECT * from ids where is_scrapped_profile=1")
        scrapped_profile_f = self.cursor.fetchall()
        scrapped_profile = [i["linkedin_url"] for i in scrapped_profile_f]
        self.cursor.execute(f"SELECT * from ids where is_scrapped=1")
        scrapped_id = self.cursor.fetchall()
        results = []
        for i in scrapped_id:
            logger.info(i)
            linkedin_url = i["linkedin_url"]
            if linkedin_url in scrapped_profile:
                continue
            id = re.findall(r"in/([\w-]+)/", linkedin_url + "/")[0]
            id_details = self._scrape_profile(id)
            logger.info(id_details)
            result = {
                "Organization Name(Column A)": i["organization_name"],
                "uuid (Column B)": i["startup_uuid"],
                "Name from Column E": i["founder_name"],
                "LinkedIn Name": id_details["name"],
                "experience": id_details["experiences"],
                "education": id_details["education"],
                "Profile Image URL": id_details["profile_dp_link"],
                "Linkedin Link": i["linkedin_url"],
            }
            row = tuple(result)
            self.cursor.execute(
                "INSERT INTO profiles_raw VALUES(?,?,?,?,?,?,?,?);", row
            )
            name = i["founder_name"]
            self.cursor.execute(
                "UPDATE ids SET is_scrapped_profile = 1 WHERE founder_name = ?", name,
            )
            self.conn.commit()
            results.append(result)
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
        self.cursor.execute("SELECT * from profiles_raw")
        scrapped_profile = self.cursor.fetchall()
        f = open("ori.json")
        profile_details = json.load(f)
        results = []
        for i in profile_details:
            base_result = []
            exp_list = []
            for exp in i["experience"]:
                company_urn = exp.get("companyUrn", None)
                company_url = None
                if company_urn:
                    company_url = f"linkedin.com/company/{company_urn.split(':')[-1]}"
                period1 = None
                period2 = None
                if exp["timePeriod"]:
                    period1, period2 = self.extract_time_period(exp["timePeriod"])
                result = {
                    "Company Name": exp.get("companyName", None),
                    "Company Location": exp.get("locationName", None),
                    "Company URL": company_url,
                    "Position(Job Title)": exp.get("title", None),
                    "Period 1": period1,
                    "Period 2": period2,
                }
                exp_list.append(result)
            edu_list = []
            for edu in i["education"]:
                period1 = None
                period2 = None
                if edu["timePeriod"]:
                    period1, period2 = self.extract_time_period(edu["timePeriod"])
                result = {
                    "Education": edu.get("schoolName", None),
                    "Period 1 Edu": period1,
                    "Period 2 Edu": period2,
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
                    shorter_list = [{key: None for key in d} for d in shorter_list]
                    longer_list[d].update(shorter_list[0])

            for x in longer_list:
                base_dict = {
                    "Organization Name(Column A)": i["Organization Name(Column A)"],
                    "uuid (Column B)": i["uuid (Column B)"],
                    "Name from Column E": i["Name from Column E"],
                    "LinkedIn Name": i["LinkedIn Name"],
                }
                base_dict.update(x)
                base_result.append(base_dict)
            logger.info(base_result)
            # append linkedin_url
            for data in base_result:
                data.update({"Linkedin Link": i["Linkedin Link"]})

            results += base_result

        # write to csv
        fields = list(results[0].keys())
        self._save_to_csv(fields, results, "scrapped_profiles.csv")
        logger.info(results)


if __name__ == "__main__":
    conn = LinkedinScraper()
    # get profile id
    # conn._scrape_profile_id(insert_raw_data=True)
    # conn._scrape_profile_id()
    # scrape profile id
    # profile_id = conn._get_profile_id()
    # process or cleaning profile data
    xxx = conn._scrape_profile("rmishra")
    # result = conn._process_scrapped_profile()

