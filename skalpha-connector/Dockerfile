FROM python:3.6

# set environment variables
ENV TZ=America/New_York \
	MONGODB_HOST=192.168.99.100 \
	MONGODB_PORT=27017

# Copy python code
WORKDIR /usr/src/skalpha-connector
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY ./skalpha-connector ./skalpha-connector
COPY ./conf/logging.yaml ./conf/logging.yaml

ENTRYPOINT ["python","./skalpha-connector/scrape_divs.py"]