# Data Migration Project

## Overview

This project facilitates the migration of historical data from CSV files into a MySQL database using a FastAPI application. The application dynamically creates tables based on the CSV file names and loads the data into the appropriate tables. This is especially useful for managing job, department, and hired employee data without requiring headers in the CSV files.

## Features

- Automatic table creation based on file names.
- Data loading into MySQL tables.
- Batch transactions for efficient data insertion.
- Handling of multiple CSV files in a single request.
- Support for job, department, and hired employee data.

## Table Structures

The tables in the MySQL database are structured as follows:

- **jobs**
  - `id` (int)
  - `job` (string)
  
- **departments**
  - `id` (int)
  - `department` (string)
  
- **hired_employees**
  - `id` (int)
  - `name` (string)
  - `datetime` (string)
  - `department_id` (int)
  - `job_id` (int)

## Prerequisites

- Docker
- Docker Compose
- MySQL

## Getting Started

1. Clone the repository:

```bash
git clone <https://github.com/ronniefuertes/big-data-migration_glbnt>
cd <repository-directory>
```

2. Build and start the Docker containers:

```bash
docker-compose up --build
```

3. The FastAPI application will be available at http://localhost:8000.

4. To visually see the database go to http://localhost:8080

## Usage
### Upload CSV Files

1. Check ipAdress on bash terminal with: 
```bash
docker container ls # to verify its running and its name (in case it was changed on docker-compose.yml)
docker inspect docker_mysql # Find the ip at the end
```
2. Sign in into phpMyAdmin with: ip, user: root, pass: password

3. To upload multiple CSV files, use the following command:

```bash
curl -X POST "http://localhost:8000/upload-csv/" -F "files=@data/jobs.csv" -F "files=@data/departments.csv" -F "files=@data/hired_employees.csv"
```
### CSV File Naming Convention
The application determines the table name based on the CSV file names. The CSV files should follow these naming conventions:

- Files containing the word "job" will be loaded into the jobs table.
- Files containing the word "department" will be loaded into the departments table.
- Files containing the word "hired_employee" will be loaded into the hired_employees table.

### No Headers in CSV Files
Ensure that the CSV files do not have headers. The application assigns columns to the tables based on the file type:

- `jobs`: 2 columns (id, job)
- `departments`: 2 columns (id, department)
- `hired_employees`: 5 column (id, name, datetime, department_id, job_id)

## Project Structure
- `app/main.py`: The main FastAPI application file.
- `Dockerfile`: Dockerfile for building the FastAPI application.
- `docker-compose.yml`: Docker Compose file for setting up the FastAPI application and MySQL database.
- `data/`: Directory to store the uploaded CSV files.

## License
This project is licensed under the MIT License.

