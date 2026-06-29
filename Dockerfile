FROM apache/airflow:3.2.2

ENV PLAYWRIGHT_BROWSERS_PATH=/opt/playwright-browsers

# Create the shared browser directory before switching users
USER root
RUN mkdir -p /opt/playwright-browsers

# pip must run as airflow — the Airflow image blocks pip when run as root
USER airflow
COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt

# playwright install-deps needs root to run apt-get, but playwright itself lives
# in the airflow user's local site-packages, so we expose it via PYTHONPATH
USER root
RUN PYVER=$(python3 -c "import sys; print(f'python{sys.version_info.major}.{sys.version_info.minor}')") \
    && PYTHONPATH=/home/airflow/.local/lib/$PYVER/site-packages \
       python3 -m playwright install-deps chromium \
    && chown -R airflow:root /opt/playwright-browsers \
    && rm -rf /var/lib/apt/lists/*

# Install the Chromium browser binary as airflow so it lands in PLAYWRIGHT_BROWSERS_PATH
USER airflow
RUN playwright install chromium
