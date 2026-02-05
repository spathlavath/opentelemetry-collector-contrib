-- MySQL Sample Employees Database
-- Simplified version with only employees table
-- Based on https://github.com/datacharmer/test_db

DROP DATABASE IF EXISTS employees;
CREATE DATABASE IF NOT EXISTS employees;
USE employees;

SELECT 'CREATING EMPLOYEES TABLE' as 'INFO';

DROP TABLE IF EXISTS employees;

SET default_storage_engine = InnoDB;

CREATE TABLE employees (
    emp_no      INT             NOT NULL,
    birth_date  DATE            NOT NULL,
    first_name  VARCHAR(14)     NOT NULL,
    last_name   VARCHAR(16)     NOT NULL,
    gender      ENUM ('M','F')  NOT NULL,    
    hire_date   DATE            NOT NULL,
    PRIMARY KEY (emp_no),
    INDEX idx_first_name (first_name),
    INDEX idx_last_name (last_name),
    INDEX idx_hire_date (hire_date)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

SELECT 'LOADING EMPLOYEES DATA' as 'INFO';
source /docker-entrypoint-initdb.d/load_employees.dump;

SELECT 'EMPLOYEES DATABASE SETUP COMPLETE' as 'INFO';
SELECT COUNT(*) as 'Total Employees' FROM employees;
