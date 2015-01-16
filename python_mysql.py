###########################################################
#                                                           
#  Some helpful wrapper functions to mysql.connector       
#                                                         
###########################################################

import mysql.connector as mysql
from mysql.connector import errorcode



def mysql_connect(conf):
    try:
        con = mysql.connect(**conf)
        print ('DB connection successful.')
    except mysql.Error as err:
        if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
            print ('Access denied.')
        elif err.errno == errorcode.ER_BAD_DB_ERROR:
            print ('Bad DB.')
        else:
            print (err)
    return con


def mysql_disconnect(con):
    con.close()
    if not con:
        print ('DB disconnected.') 


def mysql_create_db(con, name):
    cursor = con.cursor()
    query = 'CREATE DATABASE IF NOT EXISTS ' + name + ';'
    try:
        cursor.execute(query)
    except mysql.Error as err:
        print ("Failed to create database due to error: {}".format(err))
        exit(1)
    
    cursor.close()


def mysql_use_db(con, db):
    try:
        con.database = db
    except mysql.Error as err:
        if err.errno == errorcode.ER_BAD_DB_ERROR:
            print ('Bad DB.')
        else:
            print (err)
            exit(1)


def mysql_create_table(con, query):
    cursor = con.cursor()
    try:
        print("Creating table.")
        cursor.execute(query)
    except mysql.Error as err:
        if err.errno == errorcode.ER_TABLE_EXISTS_ERROR:
            print ("Table exists.")
        else:
            print (err.msg)

    cursor.close()


# drop the table if it exists
def mysql_drop_table(con, table):
    cursor = con.cursor()
    query = 'DROP TABLE IF EXISTS ' + table + ';'
    cursor.execute(query)
    cursor.close()


# helper functions for constructing queries
def mysql_construct_insert(table, columns, values):
    query = 'INSERT INTO ' + table + '(' + \
            ','.join(str(c) for c in columns) + \
            ') VALUES('
    
    for v in values:
        if type(v) == str:
            v = "'" + v + "'"
        else:
            v = str(v)
        query = query + v + ','

    # remove last comma
    query = query.rstrip(',');

    # finish off the query.
    query = query + ');'
    return query


# for now, input will be seperate lists for columns and values
# disable auto so it doesn't take forever to insert into the database.
# everytime it commits, it makes a disk write and this will take a long time
# if, say, you're populating a database with 10 000 entries
def mysql_insert(con, table, columns, values, auto=True):
    cursor = con.cursor()
    query = mysql_construct_insert(table, columns, values)
    cursor.execute(query)
    if auto:
        con.commit()
    cursor.close()


# columns: list of columns
# conditionals: a string containing the rest of the query
def mysql_select(con, table, columns, conditionals):
    query = 'SELECT ' + ','.join(str(c) for c in columns) + ' FROM ' \
            + table + ' ' + conditionals + ';'
    cursor = con.cursor()
    cursor.execute(query)
    rows = cursor.fetchall()
    cursor.close()
    return rows


# returns number of rows in table
def mysql_count_rows(con, table):
    cursor = con.cursor()
    query = 'SELECT COUNT(*) FROM ' + table + ';'
    cursor.execute(query)
    num_rows = cursor.fetchall()
    cursor.close()
    return num_rows[0][0]
    

# update field 
def mysql_update(con, table, column, value, condition, auto=True):
    cursor = con.cursor()
    query = 'UPDATE ' + table + ' SET '
    for c, v in zip(column, value):
        if type(v) == str:
            v = "'" + v + "'"
        else:
            v = str(v)
        query = query + c + ' = ' + v + ', '

    query = query.rstrip(', ')
    query = query + ' ' + condition + ';' 
    cursor.execute(query)
    if auto:
        con.commit()
    cursor.close()
