from os import access
import time
import urllib
import requests
import json
import dateutil.parser
import datetime
from configparser import ConfigParser
from splinter import Browser
from chromedriver_py import binary_path
from datetime import datetime


class TokenInitiator:
    def __init__(self):

        config = ConfigParser()
        config.read('configs.ini')
        self.CLIENT_ID = config.get('main', 'CLIENT_ID')
        self.REDIRECT_URI = config.get('main', 'REDIRECT_URI')
        self.ACCOUNT_ID = config.get('main', 'ACCOUNT_ID')
        self.USERNAME = config.get('main', 'USERNAME')
        self.PASSWORD = config.get('main', 'PASSWORD')
        self.ANSWER_1 = config.get('main', 'ANSWER_1')
        self.ANSWER_2 = config.get('main', 'ANSWER_2')
        self.ANSWER_3 = config.get('main', 'ANSWER_3')
        self.ANSWER_4 = config.get('main', 'ANSWER_4')
        self.selected_answer = None

    def unix_time_millis(dt):
        epoch = datetime.datetime.utcfromtimestamp(0)
        return (dt - epoch).total_seconds() * 1000.0

    def get_access_token(self):

        executable_path = {'executable_path': '/bin/chromedriver'}

        browser = Browser('chrome', **executable_path, headless=True)

        method = 'GET'
        url = 'https://auth.tdameritrade.com/auth?'
        client_code = self.CLIENT_ID + '@AMER.OAUTHAP'
        payload = {'response_type': 'code',
                   'redirect_uri': self.REDIRECT_URI, 'client_id': client_code}

        p = requests.Request(method=method, url=url, params=payload).prepare()
        url_update = p.url

        browser.visit(url_update)

        browser.find_by_id("username0").first.fill(self.USERNAME)
        browser.find_by_id("password1").first.fill(self.PASSWORD)
        time.sleep(.3)

        browser.find_by_id("accept").first.click()
        time.sleep(.3)

        browser.find_by_tag("summary").first.click()
        time.sleep(.5)

        browser.find_by_name("init_secretquestion").first.click()
        time.sleep(.5)

        security_question = browser.find_by_tag("p")[2].text

        if security_question == "Question: What was your high school mascot?":
            self.selected_answer = self.ANSWER_1
        elif security_question == "Question: What is your father's middle name?":
            self.selected_answer = self.ANSWER_2
        elif security_question == "Question: In what city were you married? (Enter full name of city only.)":
            self.selected_answer = self.ANSWER_3
        else:
            self.selected_answer = self.ANSWER_4

        browser.find_by_id("secretquestion0").first.fill(self.selected_answer)
        time.sleep(.5)

        browser.find_by_id("accept").first.click()
        time.sleep(.5)

        browser.find_by_css("div.option")[1].click()
        time.sleep(.5)

        browser.find_by_id("accept").first.click()
        time.sleep(.5)

        browser.find_by_id("accept").first.click()
        time.sleep(1)

        new_url = browser.url
        parse_url = urllib.parse.unquote(new_url.split('code=')[1])

        browser.quit()

        auth_url = r"https://api.tdameritrade.com/v1/oauth2/token"

        headers = {"Content-Type": "application/x-www-form-urlencoded"}

        payload = {
            'grant_type': 'authorization_code',
            'access_type': 'offline',
            'code': parse_url,
            'client_id': self.CLIENT_ID,
            'redirect_uri': self.REDIRECT_URI
        }

        auth_response = requests.post(
            url=auth_url, headers=headers, data=payload)

        token = auth_response.json()

        with open("token.json", "w+") as f:
            json.dump(token, f)

        access_token = token['access_token']

        headers = {'Authorization': "Bearer {}".format(access_token)}

        endpoint = "https://api.tdameritrade.com/v1/userprincipals"

        params = {'fields': 'streamerSubscriptionKeys,streamerConnectionInfo'}

        userprinciples_request = requests.get(
            url=endpoint, params=params, headers=headers)
        userprinciples_response = userprinciples_request.json()

        return userprinciples_response
