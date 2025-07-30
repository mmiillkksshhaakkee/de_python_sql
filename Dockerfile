FROM python:3.12

RUN pip install pandas && \
    pip install --upgrade pip

WORKDIR /app
COPY pipeline.py pipeline.py
ENTRYPOINT ["python", "pipeline.py"]