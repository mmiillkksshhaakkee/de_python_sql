FROM python:3.12

RUN pip install pandas && \
    pip install --upgrade pip && \
    pip install pgcli && \
    pip install "psycopg[binary,pool]" && \
    pip install pyarrow && \
    pip install sqlalchemy

WORKDIR /app
COPY pipeline.py pipeline.py
ENTRYPOINT ["python", "pipeline.py"]