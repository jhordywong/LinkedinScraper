Custom LinkedinScraper for Upwork Task by Jhordy Wong
Instructions:
1. Install python 3.7+ in your system
2. Install google chrome browser
3. run python -m pip install -r requirements.txt --user from your terminal to intall necessary package
4. go to where is linkedin_scraper library installed and replace person.py with person.py at this folders
There are some variable you need to fill first at main.py line 1167 which is BASE_DATA
its must be filled with csv raw files which founders to scrape from crunchbase
you can also use proxy by filling the proxies at proxies.txt and set BYPASS_PROXY = False at line 1168 on main.py
After that you can run this command:
# To scrape founder linkedin_url
python main.py -c update_crunchbase_json
python main.py -c scrape_crunchbase 

# To scrape linkedin profile details
# please fills accounts.txt with account you wish to use to scrape, you can use multiple account but with all same password
# you must fill variable ACC_PASS at main.py as password to your account
python main.py -c scrape_linkedin_profiles

# To validate the scrape result
python main.py -c validate_scrape

# To save scrape result
python main.py -c save_scrape_result

If you want to stop the scrape in the middle of the process you can just close the deployed google chrome
and dont be worry because all of your data is already saved if you wish to continue to scrape again
Please be mind, this is semi automatic scrapping which you should solve the captcha if its appear when scraping
