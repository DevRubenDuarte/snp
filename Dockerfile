# Build
FROM python:3.14-alpine
ADD main.py .

# Import dependencies from requirements.txt
COPY requirements.txt requirements.txt
RUN pip install -r requirements.txt

CMD ["python", "./main.py"]