FROM python:3.12

RUN pip install --upgrade pip && \
    pip install pandas && \
    pip install pgcli && \
    pip install "psycopg[binary,pool]" && \
    pip install pyarrow && \
    pip install sqlalchemy && \
    pip install tqdm && \
    pip install os && \
    pip install gc && \
    pip install argparse && \
    pip install datetime

WORKDIR /app
COPY job_pg_table.py job_pg_table.py
ENTRYPOINT ["python3", "job_pg_table.py"]