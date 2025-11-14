# Build
FROM python:3.14-slim

# Set the working directory
WORKDIR /app

# Install system dependencies required for psycopg and other libraries


# Import dependencies from requirements.txt
COPY requirements.txt requirements.txt
RUN pip install -r requirements.txt

# Copy the rest of the files
COPY main.py db_connection.py plink_integration.py zip_file_handler.py /app/
COPY plink/ /app/plink/

RUN mkdir -p /app/uploads

EXPOSE 8000

# Run the application
CMD ["python", "-m", "uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]