FROM python:3.6

# Install cron, syslog packages
# dos2unix package to solve windows end of line issues
RUN apt-get update && apt-get install -y apt-utils \
	&& apt-get install -y cron dos2unix rsyslog \
	&& apt-get clean \
	&& rm -rf /var/lib/apt/lists/*

# set environment variables
# ENV LANG=C.UTF-8
ENV TZ=America/New_York

# Copy python code
WORKDIR /usr/src/skalpha-connector
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY ./skalpha-connector ./skalpha-connector
COPY ./conf/logging.yaml ./conf/logging.yaml

# set cron table
COPY ./conf/skalpha-cron /etc/cron.d/
RUN dos2unix /etc/cron.d/skalpha-cron \
	&& crontab /etc/cron.d/skalpha-cron

# Copy script setting environment variables
COPY ./scripts/.profile ./scripts/skalpha_env.sh /root/
RUN dos2unix -o /root/skalpha_env.sh /root/.profile

# Entrypoint starts cron and syslog daemons
COPY ./scripts/docker-entrypoint.sh /usr/local/bin/
RUN dos2unix /usr/local/bin/docker-entrypoint.sh \
	&& chmod +x /usr/local/bin/docker-entrypoint.sh

CMD ["/usr/local/bin/docker-entrypoint.sh"]