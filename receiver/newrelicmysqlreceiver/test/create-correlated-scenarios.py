#!/usr/bin/env python3

"""
Script to create realistic e-commerce scenarios showing slow queries, wait events, and blocking sessions.
Uses persistent MySQL connections to ensure transactions stay active.
"""

import mysql.connector
import threading
import time
import signal
import sys

# MySQL connection settings
MYSQL_CONFIG = {
    'host': 'localhost',
    'port': 3306,
    'user': 'monitor',
    'password': 'monitorpass',
    'database': 'testdb',
    'autocommit': False  # Important: disable autocommit for transactions
}

# Global flag to stop all threads
stop_threads = threading.Event()


def signal_handler(sig, frame):
    """Handle Ctrl+C gracefully"""
    print('\n\033[1;33mStopping all scenarios...\033[0m')
    stop_threads.set()
    sys.exit(0)


signal.signal(signal.SIGINT, signal_handler)


def user_a_inventory_update():
    """
    User A: Updates product inventory and holds lock WITHOUT committing.
    This keeps the lock active indefinitely until Ctrl+C.
    """
    print('\033[0;32mStarting USER-A-INVENTORY-UPDATE...\033[0m')
    
    try:
        # Create separate connection for User A
        conn_a = mysql.connector.connect(**MYSQL_CONFIG)
        cursor_a = conn_a.cursor()
        
        # Start transaction ONCE and hold it
        cursor_a.execute("START TRANSACTION")
        
        # Update product stock (this acquires a lock on row id=1)
        cursor_a.execute("""
            UPDATE products 
            SET stock_quantity = stock_quantity - 5
            WHERE id = 1
        """)
        
        print('\033[0;36m[USER-A] Lock acquired on product ID 1, holding INDEFINITELY (no commit)...\033[0m')
        print('\033[0;36m[USER-A] Connection will hold lock until Ctrl+C is pressed\033[0m')
        
        # Keep connection alive without committing
        # This holds the lock indefinitely
        while not stop_threads.is_set():
            time.sleep(1)  # Just keep thread alive
                
    except mysql.connector.Error as e:
        print(f'\033[0;31m[USER-A] Error: {e}\033[0m')
    except Exception as e:
        print(f'\033[0;31m[USER-A] Connection error: {e}\033[0m')
    finally:
        print('\033[0;36m[USER-A] Cleaning up and releasing lock...\033[0m')
        if 'conn_a' in locals() and conn_a.is_connected():
            cursor_a.close()
            conn_a.close()


def user_b_inventory_update():
    """
    User B: Tries to update the same product as User A.
    Will be BLOCKED indefinitely while User A holds the lock.
    Uses a separate connection from User A.
    """
    print('\033[0;32mStarting USER-B-INVENTORY-UPDATE...\033[0m')
    
    try:
        # Create separate connection for User B
        conn_b = mysql.connector.connect(**MYSQL_CONFIG)
        cursor_b = conn_b.cursor()
        
        # Set lock wait timeout to a large value so it stays blocked
        cursor_b.execute("SET SESSION innodb_lock_wait_timeout = 3600")  # 1 hour
        
        # Start transaction
        cursor_b.execute("START TRANSACTION")
        
        print('\033[0;33m[USER-B] Attempting to update product ID 1 (will be BLOCKED by USER-A)...\033[0m')
        
        # This will BLOCK indefinitely because User A holds the lock and never commits
        cursor_b.execute("""
            UPDATE products 
            SET stock_quantity = stock_quantity - 3
            WHERE id = 1
        """)
        
        # This line will never be reached until User A releases the lock
        print('\033[0;33m[USER-B] Update successful (USER-A released lock)\033[0m')
        conn_b.commit()
                
    except mysql.connector.Error as e:
        if 'Lock wait timeout' in str(e):
            print('\033[0;31m[USER-B] Lock wait timeout exceeded\033[0m')
        else:
            print(f'\033[0;31m[USER-B] Error: {e}\033[0m')
    except Exception as e:
        print(f'\033[0;31m[USER-B] Connection error: {e}\033[0m')
    finally:
        print('\033[0;33m[USER-B] Cleaning up...\033[0m')
        if 'conn_b' in locals() and conn_b.is_connected():
            cursor_b.close()
            conn_b.close()


def inefficient_order_query():
    """
    Runs an inefficient query that builds order payload using subqueries.
    This creates slow query with poor execution plan.
    """
    print('\033[0;32mStarting INEFFICIENT-ORDER-QUERY...\033[0m')
    
    try:
        conn = mysql.connector.connect(**MYSQL_CONFIG)
        cursor = conn.cursor()
        
        while not stop_threads.is_set():
            try:
                # Inefficient query with subqueries in SELECT clause
                cursor.execute("""
                    SELECT 
                        o.id,
                        o.order_date,
                        o.total_amount,
                        o.status,
                        (SELECT username FROM users WHERE id = o.user_id) AS customer_name,
                        (SELECT email FROM users WHERE id = o.user_id) AS customer_email,
                        (SELECT COUNT(*) FROM order_items WHERE order_id = o.id) AS item_count,
                        (SELECT SUM(quantity) FROM order_items WHERE order_id = o.id) AS total_quantity,
                        (SELECT GROUP_CONCAT(name) 
                         FROM products 
                         WHERE id IN (SELECT product_id FROM order_items WHERE order_id = o.id)) AS product_names,
                        (SELECT SUM(stock_quantity) 
                         FROM products 
                         WHERE id IN (SELECT product_id FROM order_items WHERE order_id = o.id)) AS total_stock
                    FROM orders o
                    WHERE o.order_date >= DATE_SUB(NOW(), INTERVAL 30 DAY)
                    ORDER BY o.order_date DESC
                    LIMIT 50
                """)
                
                # Fetch results to actually execute the query
                results = cursor.fetchall()
                print(f'\033[0;35m[INEFFICIENT-QUERY] Retrieved {len(results)} orders\033[0m')
                
                # Small pause before next iteration
                time.sleep(0.5)
                
            except mysql.connector.Error as e:
                print(f'\033[0;31m[INEFFICIENT-QUERY] Error: {e}\033[0m')
                time.sleep(1)
                
    except Exception as e:
        print(f'\033[0;31m[INEFFICIENT-QUERY] Connection error: {e}\033[0m')
    finally:
        if 'conn' in locals() and conn.is_connected():
            cursor.close()
            conn.close()


def main():
    """Main function to start all scenarios in parallel threads"""
    
    print('\033[0;34m' + '=' * 60 + '\033[0m')
    print('\033[0;34mMySQL Query Performance Monitoring - Scenario Generator\033[0m')
    print('\033[0;34m' + '=' * 60 + '\033[0m')
    print('\033[1;33mThis creates 3 real-world scenarios:\033[0m')
    print('  1. User A updates product stock (holds lock INDEFINITELY - no commit)')
    print('  2. User B tries to update same product (BLOCKED indefinitely)')
    print('  3. Inefficient order query (slow query with poor execution plan)')
    print('')
    print('\033[1;31mIMPORTANT: User A and User B use separate connections\033[0m')
    print('\033[1;31mUser A will hold the lock until you press Ctrl+C\033[0m')
    print('')
    print('\033[1;33mPress Ctrl+C to stop all scenarios and release locks\033[0m')
    print('\033[0;34m' + '=' * 60 + '\033[0m')
    print('')
    
    # Create threads for each scenario
    threads = [
        threading.Thread(target=user_a_inventory_update, name='USER-A', daemon=True),
        threading.Thread(target=user_b_inventory_update, name='USER-B', daemon=True),
        threading.Thread(target=inefficient_order_query, name='INEFFICIENT-QUERY', daemon=True)
    ]
    
    # Start all threads
    for thread in threads:
        thread.start()
        time.sleep(0.5)  # Stagger startup slightly
    
    print('')
    print('\033[0;32mAll scenarios running in parallel!\033[0m')
    print('\033[0;34mWhat you\'ll see:\033[0m')
    print('  - USER-A: Starts transaction, updates product ID 1, NEVER commits (holds lock forever)')
    print('  - USER-B: Tries to update same product, gets BLOCKED waiting for USER-A')
    print('  - INEFFICIENT-QUERY: Continuously runs slow query with subqueries')
    print('')
    print('\033[1;33mMonitor blocking with: ./run-monitoring-queries.py\033[0m')
    print('\033[1;31mUser B will remain blocked until you stop this script with Ctrl+C\033[0m')
    print('')
    print('\033[0;32mWaiting... (Press Ctrl+C to stop and release locks)\033[0m')
    print('')
    
    # Keep main thread alive
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        signal_handler(None, None)


if __name__ == '__main__':
    main()
