import os
import logging.config

import yaml


def setup_logging(
    config_path='./logging.yaml',
):
    if os.path.exists(config_path):
        with open(config_path, 'rt') as f:
            config = yaml.safe_load(f.read())
        logging.config.dictConfig(config)
    else:
        logging.basicConfig(level=logging.INFO)
