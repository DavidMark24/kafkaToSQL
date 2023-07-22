import configparser


def config_parse(file):
    config = configparser.ConfigParser()
    config.read(file)
    return config
