#
#  Unit testing of mysql.connector wrapper
#  functions in python_mysql
#

import python_mysql as pysql
from datetime import date, timedelta

DB = 'pythondb'

SERVER_CONF = {
        'user': 'jason',
        'password': 'jason',
        'host': 'localhost',
        'database': DB
        }

# I chose not to use CREATE TABLE IF NOT EXISTS since I want
# my subroutine to catch the error where the table already exists.
COUNT_TABLE_NAME = 'counts_test'
COUNT_TABLE = 'CREATE TABLE ' + COUNT_TABLE_NAME + '( \
        id INT(11) NOT NULL AUTO_INCREMENT PRIMARY KEY, \
        domain VARCHAR(255) NOT NULL, \
        date DATE NOT NULL, \
        count INT(11) UNSIGNED NOT NULL);'

print ('Connecting to DB...')
con = pysql.mysql_connect(SERVER_CONF)
print ('Dropping table if it exists...')
pysql.mysql_drop_table(con, COUNT_TABLE_NAME)
print ('Creating table...')
pysql.mysql_create_table(con, COUNT_TABLE)

# test insertion.
old_row_count = pysql.mysql_count_rows(con, COUNT_TABLE_NAME)
print ('Checking table is empty...')
assert old_row_count == 0
print ('OK')

pysql.mysql_insert(con, COUNT_TABLE_NAME,
        ['domain', 'date', 'count'],
        ['hello.com', str(date.today()), 4])
pysql.mysql_insert(con, COUNT_TABLE_NAME,
        ['domain', 'date', 'count'],
        ['me.com', str(date.today()), 6])

# get the count
print ('Checking row counts...')
row_count = pysql.mysql_count_rows(con, COUNT_TABLE_NAME)
assert row_count == 2 
print ('OK')

# test select
# no conditional for now
rows = pysql.mysql_select(con, COUNT_TABLE_NAME,
        ['id', 'domain', 'date', 'count'],
        '')
select_count = rows[0][3]
print ('Checking values are inserted correctly...')
assert select_count == 4
print ('OK')

# test select with conditional
rows = pysql.mysql_select(con, COUNT_TABLE_NAME,
        ['id', 'domain', 'date', 'count'],
        'LIMIT 1')
# rows list should only be of size 1.
print ('Checking SELECT with LIMIT conditional...')
assert len(rows) == 1
print ('OK')

# test update
pysql.mysql_update(con, COUNT_TABLE_NAME, 
        ['date', 'count'],
        [str(date.today()-timedelta(days=30)), 9, 4],
        'WHERE id = 2')

rows = pysql.mysql_select(con, COUNT_TABLE_NAME,
        ['id', 'date', 'count'],
        'WHERE id = 2')

print ('Checking update was successful...')
ret_id = rows[0][0]
date = rows[0][1]
count = rows[0][2]
assert ret_id == 2
assert date == date.today()-timedelta(days=30)
assert count == 9
print ('OK')

print ('Disconnecting...')
pysql.mysql_disconnect(con)

