import re
import requests

"""

Search for "anchor" in the network tab while the site is loading to obtain the url

anchor url should look like:
https://www.google.com/recaptcha/api2/anchor?ar=1&k=...

"""

ANCHOR_URL = "https://www.google.com/recaptcha/api2/anchor?ar=1&k=6LfwuyUTAAAAAOAmoS0fdqijC2PbbdH4kjq62Y1b&co=aHR0cHM6Ly93d3cuZ29vZ2xlLmNvbTo0NDM.&hl=en&v=MuIyr8Ej74CrXhJDQy37RPBe&size=normal&s=UCPOzhrrddFh_oHnlA6ytDDBADKqV1HPD0jIR9TF2LFm7rK3V_cZD3-FY8dXGzU0bT7HKF6Sfs3pRyb7jba4jkbUPhQidEM49SXhPUvgR_o3XDtgOOGBFup7vi6llVb6sUtQ3RdgpoE75tinNkxj0WIkeMUGTZMAMT_nbTGqTfydKee30PI-gXr_hyAfXJB7nK-uiQGf7H0QgkXAE73iTPB5YRNl7pd3XLdg1EUlqw51wOmXy1GPVfVaN260IxPhbT4K59JzyWGGUh2aqX_OobWhb_v7kVg&cb=a9v9r52se5w7"

# -------------------------------------------


def RecaptchaV3(ANCHOR_URL):
    url_base = "https://www.google.com/recaptcha/"
    post_data = "v={}&reason=q&c={}&k={}&co={}"

    client = requests.Session()

    client.headers.update({"content-type": "application/x-www-form-urlencoded"})

    matches = re.findall("([api2|enterprise]+)\/anchor\?(.*)", ANCHOR_URL)[0]
    url_base += matches[0] + "/"
    params = matches[1]

    res = client.get(url_base + "anchor", params=params)
    token = re.findall(r'"recaptcha-token" value="(.*?)"', res.text)[0]

    params = dict(pair.split("=") for pair in params.split("&"))
    post_data = post_data.format(params["v"], token, params["k"], params["co"])

    res = client.post(url_base + "reload", params=f'k={params["k"]}', data=post_data)

    answer = re.findall(r'"rresp","(.*?)"', res.text)[0]

    return answer


# -------------------------------------------

ans = RecaptchaV3(ANCHOR_URL)

print(ans)

# If you are not sure how/where to use this token then this script is not for you :)
