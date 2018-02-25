import os
import logging.config

import yaml


def setup_logging():
    abs_dir = os.path.dirname(__file__)
    config_path = os.path.join(abs_dir, '../conf/logging.yaml')
    if os.path.exists(config_path):
        with open(config_path, 'rt') as f:
            config = yaml.safe_load(f.read())
        logging.config.dictConfig(config)
    else:
        print("could not find config file")
        logging.basicConfig(level=logging.INFO)
