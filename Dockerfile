# Using a Python image
FROM python:3.9-slim

# Working directory
WORKDIR /app

# Copy requirements file into container
COPY requirements.txt .

# Installing dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy the rest of the app into the container
COPY . /app

# Run the app
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]