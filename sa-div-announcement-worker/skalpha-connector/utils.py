import os

URL_ENV_VAR_NAME = 'SA_BASE_URL'

def get_endpoint_url():
    url = os.environ.get(URL_ENV_VAR_NAME)
    if url is None:
        raise Exception(
            "Env variable= {} is not set".format(URL_ENV_VAR_NAME))
    else:
        return url
