# Unit testing specifically for sub-routines
# in process.py

import process
import python_mysql as pysql
import conf
import setup
import os
import os.path
from datetime import date, timedelta

# test email validation
valid_email = 'left-1-23@valid.com'
invalid_email = ['left@@valid.com'
        'left@valid..com']

print ('Testing email validation:')

# this should pass
assert process.check_valid_email(valid_email) == True

# these should fail
for addr in invalid_email:
    assert process.check_valid_email(addr) == False 

# sucecss!  no assertions
print ('OK')

# test get_domain:
print ('Testing get_domain():') 
domain = process.get_domain(valid_email)
assert domain == 'valid.com'

# sucess no assertions!
print ('OK')


# test check_table_updated
# this may fail if this table doesn't exist...  so be sure
# to run setup
con = pysql.mysql_connect(conf.SERVER_CONF)
print ('Checking greater than case...')
update_table = process.check_table_updated(con, 'mailing', 'greater.txt')
assert update_table == 1 
print ('OK')
print ('Checking case if file does not exist...')
update_table = process.check_table_updated(con, 'mailing', 'not_exist')
assert update_table == -1
# check that the file exists now.
assert os.path.exists('not_exist') == True
print ('OK')
# clean up
os.remove('not_exist')

# test process_all_address
COUNT_TABLE_NAME = 'process_counts_test'
COUNT_TABLE = 'CREATE TABLE ' + COUNT_TABLE_NAME + '( \
        id INT(11) NOT NULL AUTO_INCREMENT PRIMARY KEY, \
        domain VARCHAR(255) NOT NULL, \
        date DATE NOT NULL, \
        count INT(11) UNSIGNED NOT NULL);'

# make a test table
pysql.mysql_drop_table(con, COUNT_TABLE_NAME)
pysql.mysql_create_table(con, COUNT_TABLE)

# insert some entry into the count table
pysql.mysql_insert(con, COUNT_TABLE_NAME, ['domain', 'date', 'count'],
        ['hello.com', str(date.today()), 10])

# test write to table.
process.write_count_to_table(con, COUNT_TABLE_NAME, 'hello.com', str(date.today()), 14)
# should still be 1 entry since it already exists.
row_count = pysql.mysql_count_rows(con, COUNT_TABLE_NAME)
print ('Testing write_count_to_table()...')
assert row_count == 1
print ('Test 1: OK')
process.write_count_to_table(con, COUNT_TABLE_NAME, 'hello.com', str(date.today()+timedelta(days=10)), 16)
row_count = pysql.mysql_count_rows(con, COUNT_TABLE_NAME)
assert row_count == 2
print ('Test 2: OK')

# test get top 50
# first fill our test table with data using a subroutine in setup.
print ('Testing top 50 report...')
setup.populate_count_table(con, COUNT_TABLE_NAME)
process.print_top50(con, COUNT_TABLE_NAME)
assert len(process.DOMAIN_TOP50_GROWTH) == 2
print ('OK')

pysql.mysql_disconnect(con)

