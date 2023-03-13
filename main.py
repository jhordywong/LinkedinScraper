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

CREDS = [
    {"username": "jhoewong49@gmail.com", "password": "ikacantik2302"},
    {"username": "nelawulansari4@gmail.com", "password": "Tanjakindo43_"},
]


class LinkedinScraper:
    def __init__(self):
        self._MAX_SEARCH_COUNT = 49  # max seems to be 49, and min seems to be 2
        self._MAX_REPEATED_REQUESTS = (
            200  # VERY conservative max requests count to avoid rate-limit
        )
        # self.list_of_li_at = self._get_li_at()
        # self.li_at = self.list_of_li_at[0]
        self.data = pd.read_excel("LinkedIn_RACollection_3045_RA.xlsx").to_dict(
            "records"
        )
        # self.client, self.cookies = self._build_client_and_cookies()
        self.driver = self._driver()
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

    def search(self, params, limit=None, results=[]):
        """
        Do a search.
        """
        count = (
            limit
            if limit and limit <= self._MAX_SEARCH_COUNT
            else self._MAX_SEARCH_COUNT
        )
        default_params = {
            "count": str(count),
            "filters": "List()",
            "origin": "GLOBAL_SEARCH_HEADER",
            "q": "all",
            "start": len(results),
            "queryContext": "List(spellCorrectionEnabled->true,relatedSearchesEnabled->true,kcardTypes->PROFILE|COMPANY)",
        }

        default_params.update(params)

        res = self._fetch(
            f"/search/blended?{urlencode(default_params)}",
            cookies=self.cookies,
            headers={
                "accept": "application/vnd.linkedin.normalized+json+2.1",
                "csrf-token": self.cookies["JSESSIONID"].strip('"'),
            },
        )

        data = res.json()

        new_elements = []
        for i in range(len(data["data"]["elements"])):
            new_elements.extend(data["data"]["elements"][i]["elements"])
            # not entirely sure what extendedElements generally refers to - keyword search gives back a single job?
            # new_elements.extend(data["data"]["elements"][i]["extendedElements"])

        results.extend(new_elements)
        results = results[
            :limit
        ]  # always trim results, no matter what the request returns

        # recursive base case
        if (
            limit is not None
            and (
                len(results) >= limit  # if our results exceed set limit
                or len(results) / count >= self._MAX_REPEATED_REQUESTS
            )
        ) or len(new_elements) == 0:
            return results

        logger.info(f"results grew to {len(results)}")

        return self.search(params, results=results, limit=limit)

    def search_people(
        self,
        keywords=None,
        connection_of=None,
        network_depth=None,
        current_company=None,
        past_companies=None,
        nonprofit_interests=None,
        profile_languages=None,
        regions=None,
        industries=None,
        schools=None,
        include_private_profiles=False,  # profiles without a public id, "Linkedin Member"
        limit=None,
    ):
        """
        Do a people search.
        """

        def get_id_from_urn(urn):
            """
            Return the ID of a given Linkedin URN.

            Example: urn:li:fs_miniProfile:<id>
            """
            return urn.split(":")[3]

        filters = ["resultType->PEOPLE"]
        if connection_of:
            filters.append(f"connectionOf->{connection_of}")
        if network_depth:
            filters.append(f"network->{network_depth}")
        if regions:
            filters.append(f'geoRegion->{"|".join(regions)}')
        if industries:
            filters.append(f'industry->{"|".join(industries)}')
        if current_company:
            filters.append(f'currentCompany->{"|".join(current_company)}')
        if past_companies:
            filters.append(f'pastCompany->{"|".join(past_companies)}')
        if profile_languages:
            filters.append(f'profileLanguage->{"|".join(profile_languages)}')
        if nonprofit_interests:
            filters.append(f'nonprofitInterest->{"|".join(nonprofit_interests)}')
        if schools:
            filters.append(f'schools->{"|".join(schools)}')

        params = {"filters": "List({})".format(",".join(filters))}

        if keywords:
            params["keywords"] = keywords

        data = self.search(params, limit=limit)

        results = []
        for item in data:
            if "publicIdentifier" not in item:
                continue
            results.append(
                {
                    "urn_id": get_id_from_urn(item.get("targetUrn")),
                    "distance": item.get("memberDistance", {}).get("value"),
                    "public_id": item.get("publicIdentifier"),
                }
            )

        return results

    def _driver(self):
        chrome_options = uc.ChromeOptions()
        # options.headless = True
        # options.add_argument("--headless")
        # options.add_argument("--user-data-dir=F:\KERJA\BOT\Profile 8")
        # chrome_options.add_argument("--headless")
        # chrome_options.add_argument("--start-maximized")
        # chrome_options.add_argument("--start-fullscreen")
        # chrome_options.add_argument("--no-proxy-server")
        # chrome_options.add_argument("--proxy-server='direct://'")
        # chrome_options.add_argument("--proxy-bypass-list=*")
        # chrome_options.add_argument("--no-sandbox")
        return uc.Chrome(options=chrome_options)

    def _get_profile_id(self, insert_raw_data=False):
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
        # get scrapped id
        self.cursor.execute(f"SELECT startup_uuid from ids where is_scrapped=1")
        scrapped_id = self.cursor.fetchall()
        scrapped_startup_uuid = [i["startup_uuid"] for i in scrapped_id]
        logger.info(scrapped_id)
        # logger.info(len(scrapped_id))
        # sleep(30)
        cookies = cookiejar_from_dict(
            {
                "GOOGLE_ABUSE_EXEMPTION": "ID=31a6bc3604c6efad:TM=1678689048:C=r:IP=139.193.66.215-:S=rFV5WUECyjY39XNn8OZzOVg",
            }
        )
        driver = self.driver
        webcache_url = "https://webcache.googleusercontent.com/search?q=cache:"
        founder_details = []
        for i in self.data:
            startup_id = i["startup_uuid"]
            # skip scrape if its already scrapped before
            if startup_id in scrapped_startup_uuid:
                continue
            founder_details_raw = []
            founders_list = i["Founders"].split(", ")
            crunchbase_url = webcache_url + i["Organization Name URL"]
            ua = UserAgent()

            driver.execute_cdp_cmd(
                "Network.setExtraHTTPHeaders",
                {"headers": {"User-Agent": str(ua.random)}},
            )
            cookies = pickle.load(open("cookies2.pkl", "rb"))
            driver.get(crunchbase_url)
            sleep(7)
            html = driver.page_source
            soup = BeautifulSoup(html, "html.parser")
            for li in soup.select('li:-soup-contains("Founders")'):
                for a in li.select("a.link-accent[href]"):
                    founder_url = (
                        webcache_url + "https://www.crunchbase.com" + a["href"]
                    )
                    logger.info(founder_url)
                    driver.get(founder_url)
                    # pickle.dump(driver.get_cookies(), open("cookies3.pkl", "wb"))
                    sleep(10)
                    founder_html = driver.page_source
                    soup = BeautifulSoup(founder_html, "html.parser")
                    founder_linkedin_url_list = []
                    for li in soup.select('li:-soup-contains("View on LinkedIn")'):
                        for a in li.select("a.link-accent[href]"):
                            founder_linkedin_url = a["href"]
                            founder_details_raw.append(
                                {
                                    "organization_name": i["Organization Name"],
                                    "startup_uuid": i["startup_uuid"],
                                    "founder_name": founders_list,
                                    "linkedin_url": founder_linkedin_url,
                                    "is_scrapped": 1,
                                    "is_scrapper_profile": 0,
                                }
                            )
            # drop duplicates name in founder_details
            processes_founder_details = []
            for dictionary in founder_details_raw:
                for founder_name in dictionary["founder_name"]:
                    new_dict = {
                        "organization_name": dictionary["organization_name"],
                        "startup_uuid": dictionary["startup_uuid"],
                        "founder_name": founder_name,
                        "linkedin_url": dictionary["linkedin_url"],
                        "is_scrapped": dictionary["is_scrapped"],
                        "is_scrapper_profile": dictionary["is_scrapper_profile"],
                    }
                    if new_dict["founder_name"] not in [
                        x["founder_name"] for x in processes_founder_details
                    ]:
                        processes_founder_details.append(new_dict)
            # update data at DB after scrapping linkedin URL
            rows = [
                (d["linkedin_url"], d["is_scrapped"], d["founder_name"])
                for d in processes_founder_details
            ]
            self.cursor.executemany(
                "UPDATE ids SET linkedin_url = ?, is_scrapped = ? WHERE founder_name = ?",
                rows,
            )
            self.conn.commit()
            founder_details += processes_founder_details
            logger.info(processes_founder_details)
            # driver.close()
            # driver.switch_to.window(driver.window_handles[0])
            driver.quit()
            sleep(7)
        self.cursor.execute(f"SELECT * from ids where is_scrapped=0")
        unscrapped_id = self.cursor.fetchall()
        logger.info(len(unscrapped_id))

        # new_scrapped_founder = []
        # for i in unscrapped_id[:2]:
        #     founder = i["founder_name"]
        #     logger.info(founder)
        #     founder_id = self.search_people(founder)[0]["public_id"]
        #     logger.info(founder_id)
        #     new_scrapped_founder.append((founder, founder_id, 1))

        # logger.info(new_scrapped_founder)
        # self.cursor.executemany(
        #     "UPDATE ids SET id = ?, is_scrapped = ? WHERE founder_name = ?",
        #     new_scrapped_founder,
        # )
        # self.cursor.execute(f"SELECT * from ids where is_scrapped=1")
        # scrapped_id = self.cursor.fetchall()
        # logger.info(scrapped_id)


if __name__ == "__main__":
    conn = LinkedinScraper()
    conn._get_profile_id()
    # conn._get_profile_id(insert_raw_data=True)
    # get profile id
    # scrape profile id
    # process or cleaning profile data

