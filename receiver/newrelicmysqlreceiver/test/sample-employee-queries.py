#!/usr/bin/env python3
"""
Sample queries for the MySQL Employees Database
Demonstrates realistic workload for performance monitoring
"""

import mysql.connector
import json
from datetime import datetime

# MySQL Connection for Employee Database
MYSQL_CONFIG = {
    'user': 'root',
    'password': 'rootpassword',
    'host': 'localhost',
    'port': 3306,
    'database': 'employees',
}

def run_sample_queries():
    """Run various sample queries against the employee database"""
    
    connection = mysql.connector.connect(**MYSQL_CONFIG)
    cursor = connection.cursor(dictionary=True)
    
    print("=" * 80)
    print("MySQL Employees Database - Sample Queries")
    print("=" * 80)
    print()
    
    # Query 1: Simple SELECT
    print("1. Top 10 Employees by Employee Number:")
    cursor.execute("""
        SELECT emp_no, first_name, last_name, hire_date 
        FROM employees 
        ORDER BY emp_no 
        LIMIT 10
    """)
    for row in cursor.fetchall():
        print(f"   {row['emp_no']}: {row['first_name']} {row['last_name']} (hired: {row['hire_date']})")
    print()
    
    # Query 2: JOIN with aggregation (will show up in slow queries)
    print("2. Department Salary Statistics (Complex JOIN + Aggregation):")
    cursor.execute("""
        SELECT 
            d.dept_name,
            COUNT(DISTINCT de.emp_no) as employee_count,
            ROUND(AVG(s.salary), 2) as avg_salary,
            MIN(s.salary) as min_salary,
            MAX(s.salary) as max_salary
        FROM departments d
        JOIN dept_emp de ON d.dept_no = de.dept_no
        JOIN salaries s ON de.emp_no = s.emp_no
        WHERE de.to_date = '9999-01-01'
          AND s.to_date = '9999-01-01'
        GROUP BY d.dept_no, d.dept_name
        ORDER BY avg_salary DESC
    """)
    for row in cursor.fetchall():
        print(f"   {row['dept_name']:20} - Employees: {row['employee_count']:6}, "
              f"Avg Salary: ${row['avg_salary']:10.2f}, "
              f"Range: ${row['min_salary']:6} - ${row['max_salary']:6}")
    print()
    
    # Query 3: Multi-table JOIN (realistic slow query)
    print("3. Top 10 Current Highest Paid Employees (Multi-table JOIN):")
    cursor.execute("""
        SELECT 
            e.emp_no,
            e.first_name,
            e.last_name,
            s.salary,
            t.title,
            d.dept_name
        FROM employees e
        JOIN salaries s ON e.emp_no = s.emp_no
        JOIN titles t ON e.emp_no = t.emp_no
        JOIN dept_emp de ON e.emp_no = de.emp_no
        JOIN departments d ON de.dept_no = d.dept_no
        WHERE s.to_date = '9999-01-01'
          AND t.to_date = '9999-01-01'
          AND de.to_date = '9999-01-01'
        ORDER BY s.salary DESC
        LIMIT 10
    """)
    for row in cursor.fetchall():
        print(f"   {row['emp_no']}: {row['first_name']} {row['last_name']:15} - "
              f"${row['salary']:6} - {row['title']:25} ({row['dept_name']})")
    print()
    
    # Query 4: Subquery (can be slow)
    print("4. Employees Earning Above Department Average (Subquery):")
    cursor.execute("""
        SELECT 
            e.emp_no,
            e.first_name,
            e.last_name,
            s.salary,
            d.dept_name
        FROM employees e
        JOIN salaries s ON e.emp_no = s.emp_no
        JOIN dept_emp de ON e.emp_no = de.emp_no
        JOIN departments d ON de.dept_no = d.dept_no
        WHERE s.to_date = '9999-01-01'
          AND de.to_date = '9999-01-01'
          AND s.salary > (
              SELECT AVG(s2.salary)
              FROM salaries s2
              JOIN dept_emp de2 ON s2.emp_no = de2.emp_no
              WHERE s2.to_date = '9999-01-01'
                AND de2.to_date = '9999-01-01'
                AND de2.dept_no = de.dept_no
          )
        ORDER BY s.salary DESC
        LIMIT 10
    """)
    count = 0
    for row in cursor.fetchall():
        count += 1
        print(f"   {row['emp_no']}: {row['first_name']} {row['last_name']:15} - "
              f"${row['salary']:6} ({row['dept_name']})")
    print(f"   Total above average: {count}")
    print()
    
    # Query 5: Date range query (time-series analysis)
    print("5. Salary Changes in Last 5 Years (Date Range Query):")
    cursor.execute("""
        SELECT 
            YEAR(from_date) as year,
            COUNT(DISTINCT emp_no) as employees_with_raise,
            AVG(salary) as avg_new_salary
        FROM salaries
        WHERE from_date >= DATE_SUB(CURDATE(), INTERVAL 5 YEAR)
        GROUP BY YEAR(from_date)
        ORDER BY year DESC
    """)
    for row in cursor.fetchall():
        print(f"   Year {row['year']}: {row['employees_with_raise']} raises, "
              f"Avg New Salary: ${row['avg_new_salary']:.2f}")
    print()
    
    # Query 6: Career progression (complex query)
    print("6. Employees with Most Title Changes (Career Progression):")
    cursor.execute("""
        SELECT 
            e.emp_no,
            e.first_name,
            e.last_name,
            COUNT(*) as title_count,
            GROUP_CONCAT(DISTINCT t.title ORDER BY t.from_date SEPARATOR ' → ') as career_path
        FROM employees e
        JOIN titles t ON e.emp_no = t.emp_no
        GROUP BY e.emp_no, e.first_name, e.last_name
        HAVING COUNT(*) > 1
        ORDER BY title_count DESC
        LIMIT 5
    """)
    for row in cursor.fetchall():
        print(f"   {row['emp_no']}: {row['first_name']} {row['last_name']}")
        print(f"      {row['title_count']} titles: {row['career_path']}")
    print()
    
    print("=" * 80)
    print("Queries completed! Check performance_schema for metrics.")
    print("Run ./run-monitoring-queries-v2.py to see these queries in slow query metrics")
    print("=" * 80)
    
    cursor.close()
    connection.close()

if __name__ == '__main__':
    try:
        run_sample_queries()
    except mysql.connector.Error as err:
        print(f"MySQL Error: {err}")
    except Exception as e:
        print(f"Error: {e}")
