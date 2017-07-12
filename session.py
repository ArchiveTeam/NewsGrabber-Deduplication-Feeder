import time

import requests

requests.packages.urllib3.disable_warnings()


class Session(object):
    session = None

    def __init__(self):
        Session.session = requests.Session()

    @classmethod
    def login(cls, account_file):
        with open(account_file, 'r') as file:
            email, password = file.read().strip().split(',', 1)
        cls.session.get('https://archive.org/account/login.php')
        cls.session.post('https://archive.org/account/login.php',
            data = {
                'username': email,
                'password': password,
                'action': 'login',
                'submit': 'Log+in'
            })

    @classmethod
    def get(cls, url, status_codes=[200], content_length=0, max_tries=1,
          headers=None, cookies=None, preserve_url=False, stream=False,
          sleep_time=0, verify=False):
        tries = 0

        while tries < max_tries:
            try:
                response = cls.session.get(url, headers=headers,
                    cookies=cookies, stream=stream, verify=verify)
                assert len(response.content) > content_length
                if type(status_codes) is list:
                    assert response.status_code in status_codes
                if preserve_url:
                    assert response.url == url
                return response
            except:
                tries += 1
                time.sleep(sleep_time)

        return False