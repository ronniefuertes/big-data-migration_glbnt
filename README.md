# Data Migration Project

## Overview

This project automates the migration of CSV data into MySQL using AWS serverless components and a REST API for centralized validation/transformation. The solution leverages AWS Free Tier services (S3, Lambda, EC2, RDS) to ensure cost-efficiency while maintaining scalability and reliability.

## Architecture Diagram

![Architecture](https://github.com/ronniefuertes/big-data-migration_glbnt/blob/main/images/Database%20migration.png)

## Key Components

### 1. S3 Buckets
- **Raw**: `s3://your-bucket/raw/` (Initial CSV uploads)
- **Processed**: `s3://your-bucket/processed/` (Successfully migrated data)
- **Errors**: `s3://your-bucket/errors/` (Validation error logs)
- **Backups**: `s3://your-bucket/backups/` (AVRO backups)

### 2. AWS Lambda
- **Trigger**: S3 upload event to `raw/`
- **Actions**:
  - Batch CSV rows (1000 rows/request)
  - Invoke API validation endpoint
  - Insert valid data into RDS MySQL
  - Move files to `processed/` or `errors/`

### 3. REST API (FastAPI on EC2)
- **Verification**:
  - `/`: Validate connection with database and required tables
- **Endpoints**:
  - `POST /upload_csv`: Validate/transform CSV batches
  - `POST /backup/{table}`: Backup MySQL table to AVRO
  - `POST /restore/{table}`: Restore table from AVRO
  - `POST /employees_hired_per_quarter}`: Retrieve the number of employees hired in 2021
  - `POST /departments_above_mean_hires}`: Retrieve departments that hired more employees in 2021
- **Validation**: Uses Pydantic models for data rules

### 4. RDS MySQL
- Stores validated data
- Free Tier instance (db.t2.micro)

#### Database Schema

#### departments <!-- omit from toc -->
  
     | Column  | Type  | Description            |
     |---------|-------|------------------------|
     | id      | INT   | Primary Key            |
     | name    | TEXT  | Name of the department |

#### jobs <!-- omit from toc -->
  
     | Column  | Type  | Description              |
     |---------|-------|--------------------------|
     | id      | INT   | Primary Key              |
     | name    | TEXT  | Name of the job position |

#### hired_employees <!-- omit from toc -->
  
     | Column          | Type      | Description                     |
     |-------------- --|-----------|---------------------------------|
     | id              | INT       | Primary Key                     |
     | name            | TEXT      | Employee's full name            |
     | datetime        | TIMESTAMP | Date and time of hiring         |
     | department_id   | INT       | Foreign Key → `departments(id)` |
     | job_id          | INT       | Foreign Key → `jobs(id)`        |


## 📈 **Conclusion**  
**The PoC validated**:  
1. AWS serverless tools can automate CSV-to-MySQL migration cost-effectively.  
2. Centralized API validation ensures data integrity.  
3. Backup/restore workflows are reliable for disaster recovery. 

### 1. **AWS Free Tier Feasibility**  
- **All components operated within Free Tier limits**:  
  | Service          | Usage                          | Free Tier Limit              |  
  |------------------|--------------------------------|------------------------------|  
  | **Lambda**       | <1M invocations/month          | 1M requests/month            |  
  | **S3**           | <5GB storage                   | 5GB standard storage         |  
  | **EC2**          | <750 hours/month               | 750 hours/month (t2.micro)   |  
  | **RDS MySQL**    | Single db.t2.micro instance    | 750 hours/month              |  

### 2. **End-to-End Connectivity**  
- **S3 → Lambda → API → RDS workflow succeeded**:  
  - CSV files uploaded to `raw/` triggered Lambda within seconds.  
  - API validated/transformed data in batches up to 1000 without problems.  
  - Valid data inserted into RDS MySQL.  
  - Invalid rows logged to `errors/` with descriptive error messages.  

### 3. **BI Tool Integration**  
- **RDS MySQL connected to BI tools (e.g., Tableau, Power BI)**:  
  - Queries on RDS executed without latency for small datasets (<10k rows).  

### 4. **Backup/Restore Reliability**  
- **AVRO backups**:  
  - `POST /backup/` created compressed AVRO files in `s3://your-bucket/backups/`.  
  - `POST /restore/` fully restored test tables from backups.  