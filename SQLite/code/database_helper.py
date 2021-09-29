# Author: Yangyang Cai
# Date: 18/09/2021
# This module is built to setup employer SQLlite database

import sqlite3
from sqlite3 import Error
import os

def create_connection(db_file):
    """ create a database connection to the SQLite database
        specified by db_file
    :param db_file: database file
    :return: Connection object or None
    """
    conn = None
    try:
        conn = sqlite3.connect(db_file)
        return conn
    except Error as e:
        print(e)

    return conn

def create_table(conn, create_table_sql):
    """ create a table from the create_table_sql statement
    :param conn: Connection object
    :param create_table_sql: a CREATE TABLE statement
    :return:
    """
    try:
        c = conn.cursor()
        c.execute(create_table_sql)
    except Error as e:
        print(e)

def insert_employer(conn, employer_rows):
    """
    Create a new employer into the employer table
    :param conn:
    :param employer_rows:
    :return:
    """
    sql = ''' INSERT INTO employer(Employer_No,Effective_From,Effective_To,Status,Tier)
              VALUES(?,?,?,?,?) '''
    cur = conn.cursor()
    cur.executemany(sql, employer_rows)
    conn.commit()

def insert_payment(conn, payment_rows):
    """
    Create a new payment transaction
    :param conn:
    :param payment_rows:
    :return:
    """

    sql = ''' INSERT INTO payment(Employer_No,Cash_Received_Date,Total_Amt)
              VALUES(?,?,?) '''
    cur = conn.cursor()
    cur.executemany(sql, payment_rows)
    conn.commit()

def delete_employer(conn, id):
    """
    Delete a employer by employer id
    :param conn: Connection to the SQLite database
    :param id: id of the employer
    :return:
    """
    sql = 'DELETE FROM employer WHERE Employer_No=?'
    cur = conn.cursor()
    cur.execute(sql, (id,))
    conn.commit()

def delete_employer(conn, id):
    """
    Delete a payment by employer id
    :param conn: Connection to the SQLite database
    :param id: id of the employer
    :return:
    """
    sql = 'DELETE FROM payment WHERE Employer_No=?'
    cur = conn.cursor()
    cur.execute(sql, (id,))
    conn.commit()

def delete_all_employers(conn):
    """
    Delete all rows in the employer table
    :param conn: Connection to the SQLite database
    :return:
    """
    sql = 'DELETE FROM employer'
    cur = conn.cursor()
    cur.execute(sql)
    conn.commit()

def delete_all_payments(conn):
    """
    Delete all rows in the payment table
    :param conn: Connection to the SQLite database
    :return:
    """
    sql = 'DELETE FROM payment'
    cur = conn.cursor()
    cur.execute(sql)
    conn.commit()


def monthly_employer_report_generator(conn, sql):
    """
    Run the statement to get results
    :param conn: Connection to the SQLite database
    :param sql: sql statement for report generation
    :return: csv file
    """
    # Export data into CSV file

    try:
        print("Exporting data into CSV.")
        cursor = conn.cursor()
        cur.execute(sql)
        with open("monthly_employer_report.csv", "w") as csv_file:
            csv_writer = csv.writer(csv_file, delimiter="\t")
            csv_writer.writerow([i[0] for i in cursor.description])
            csv_writer.writerows(cursor)

        dirpath = os.getcwd() + "/monthly_employer_report.csv"
        print("Data exported Successfully into {}".format(dirpath))
    except Error as e:
        print(e)

    # Close database connection
    finally:
        conn.close()