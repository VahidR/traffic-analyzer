#!/usr/bin/env python

''' 
Copyright (C) 2013  Vahid Rafiei (@vahid_r)

This program is free software: you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation, either version 3 of the License, or
(at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program.  If not, see <http://www.gnu.org/licenses/>.
'''

import csv
import sqlite3

def main():
	
	# First, let's import required CSV files, with proper delimiter
	file1 = csv.reader(open('DATA_TRAFFIC.csv', 'rb'), delimiter=';') #traffic
	file2 = csv.reader(open('IMSI_TO_CUSTOMER.csv', 'rb'), delimiter=';') # customer
	file3 = csv.reader(open('MCCMNC_TO_GROUP.csv', 'rb'), delimiter=';') # group
	file4 = csv.reader(open('PRICE_PER_GROUP.csv', 'rb'), delimiter=';') # price

	# Since, we should map the relations among diffrent CSV files for calculating relevant prices,
	# let's convert these files to dictionaries. Dictionary lookup is really fast..
	# We can acheive this by a functional operator called "filter". 
	prices = dict(filter(None, file4))
	groups = dict(filter(None, file3))
	customers = dict(filter(None, file2))

	# Traffic file are normally really massive!! I'd like to make a middle CSV file, derived from these 4 CSV files.
	# This part is a bit tricky !!
	# It contains 3 fields: 
	#   1) Date (year-month)
	#   2) Customer: based on the proper mapping
	#   3) Cost: [calculated base price for the gruop] * [upstram + downstram traffic] / [1 Mb]
	# The lookup is based upon 'roaming' and relevant mapping is also considered.
	middle = csv.writer(open('middle.csv', 'wb') , delimiter = ';')
	for record in file1:
		middle.writerow([record[0][0:6], customers[record[1]], float(prices[groups[record[2]]]) * (int(record[4])+int(record[5])) /1000000])

	# Since the middle file has a DB style nature, I found it a bit handy to put the contant on a RAM-Based SQLite3 and process the results.
	# It will ease the complexity a lot.
	conn = sqlite3.connect(":memory:") # Don't save on disk.. Fetch them from RAM
	curs = conn.cursor()
	curs.execute("CREATE TABLE tel(dat, customer , cost);")
	reader = csv.reader(open('middle.csv', 'rb'), delimiter=';')
	for row in reader:
		to_db = [(row[0]), (row[1]), (row[2])]
		curs.execute("INSERT INTO tel(dat, customer, cost) VALUES(?,?,?)", to_db)

	conn.commit()

	# Find just unique dates.. we have millions of repetitions !!
	dates = []
	for row in conn.execute("select distinct dat from tel;"):
		dates.append(row)

	#find proper info for Customer_1
	customer1_table = []
	for d in dates:
		for record in conn.execute("select dat, customer, sum(cost) from tel where dat = ? and customer='Customer_1';", d):
			customer1_table.append(record)

	# Do the same logic for Customer_2
	customer2_table = []
	for d in dates:
		for record in conn.execute("select dat, customer, sum(cost) from tel where dat = ? and customer='Customer_2';", d):
			if record[0] == 'None':  # apparently, we have some None fields for some month in this customer. Let's eliminate them..
				record[0] = 0
				customer2_table.append(record)
			customer2_table.append(record)	

	# Let's print the Customer_1 info
	print
	print 'Billing for Customer_1:'
	print "{:<20}{:<20}{:<20}".format("Date", "Customer", "Cost")
	print "{:<20}{:<20}{:<20}".format('-'*5, '-'*8, '-'*8)
	for elm in customer1_table:
		print "{:<20} {:<20}{:<20}".format(elm[0],elm[1],elm[2])



	# Let's print the Customer_2 info
	print
	print 'Billing for Customer_2:'
	print "{:<20}{:<20}{:<20}".format("Date", "Customer", "Cost")
	print "{:<20}{:<20}{:<20}".format('-'*5, '-'*8, '-'*8)
	for elm in customer2_table:
		if type(elm[0]) == type(None):
			continue
		print "{:<20} {:<20}{:<20}".format(elm[0],elm[1],elm[2])


if __name__ == '__main__':
	main()
