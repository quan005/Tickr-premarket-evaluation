from configparser import ConfigParser


class Write_Config:
    def __init__(self):
        config = ConfigParser()

        config.add_section('main')
        config.set('main', 'CREDENTIALS_PATH', 'token.json')

        with open('configs.ini', 'w+') as f:
            config.write(f)
