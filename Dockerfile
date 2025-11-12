# Build
FROM python:3.14-alpine

# Set the working directory
WORKDIR /app

# Import dependencies from requirements.txt
COPY requirements.txt requirements.txt
RUN pip install -r requirements.txt

# Install system dependencies required for psycopg and other libraries
RUN apk add --no-cache \
    gcc \
    musl-dev \
    postgresql-dev \
    libpq \
    python3-dev \
    build-base

# Copy the rest of the files
COPY main.py db_connection.py plink_integration.py zip_file_handler.py /app/
COPY ibd/ /app/ibd/
COPY roh/ /app/roh/
COPY uploads/ /app/uploads/

CMD ["python", "./main.py"]