import yaml
import datetime


def load_keys(connector):

    with open('keys.yaml', 'r') as file:
        key_file = yaml.safe_load(file)
    pub_key = key_file[connector]['public']
    priv_key = key_file[connector]['private']

    return pub_key, priv_key


def current_timestamp():
    current_time = datetime.datetime.now()
    return int(current_time.timestamp())