from os import access
import time
import urllib
import requests
import json
import dateutil.parser
import datetime
from configparser import ConfigParser
from selenium import webdriver
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

        opts = webdriver.ChromeOptions()
        opts.add_argument("--disable-gpu")
        opts.add_argument("--disable-dev-shm-usage")
        opts.add_argument("--no-sandbox")
        opts.add_argument("--headless")
        executable_path = '/usr/bin/chromedriver'

        browser = webdriver.Chrome(
            executable_path=executable_path, options=opts)

        method = 'GET'
        url = 'https://auth.tdameritrade.com/auth?'
        client_code = self.CLIENT_ID + '@AMER.OAUTHAP'
        payload = {'response_type': 'code',
                   'redirect_uri': self.REDIRECT_URI, 'client_id': client_code}

        p = requests.Request(method=method, url=url, params=payload).prepare()
        url_update = p.url

        browser.get(url_update)

        username = browser.find_element_by_id("username0")
        username.send_keys(self.USERNAME)
        password = browser.find_element_by_id("password1")
        password.send_keys(self.PASSWORD)
        time.sleep(.3)

        accept_login = browser.find_element_by_id("accept")
        accept_login.click()
        time.sleep(.3)

        summary = browser.find_element_by_tag_name("summary")
        summary.click()
        time.sleep(.5)

        secret_question = browser.find_element_by_name("init_secretquestion")
        secret_question.click()
        time.sleep(.5)

        security_question_text = browser.find_elements_by_tag_name("p")[2]
        security_question = security_question_text.text

        if security_question == "Question: What was your high school mascot?":
            self.selected_answer = self.ANSWER_1
        elif security_question == "Question: What is your father's middle name?":
            self.selected_answer = self.ANSWER_2
        elif security_question == "Question: In what city were you married? (Enter full name of city only.)":
            self.selected_answer = self.ANSWER_3
        else:
            self.selected_answer = self.ANSWER_4

        secret_question_answer = browser.find_element_by_id("secretquestion0")
        secret_question_answer.send_keys(self.selected_answer)
        time.sleep(.5)

        accept_secret_question = browser.find_element_by_id("accept")
        accept_secret_question.click()
        time.sleep(.5)

        div_option = browser.find_elements_by_css_selector("div.option")[1]
        div_option.click()
        time.sleep(.5)

        accept_1 = browser.find_element_by_id("accept")
        accept_1.click()
        time.sleep(.5)

        accept_2 = browser.find_element_by_id("accept")
        accept_2.click()
        time.sleep(1)

        new_url = browser.current_url
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
