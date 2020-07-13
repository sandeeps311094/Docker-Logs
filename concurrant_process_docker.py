#--- This program includes the use of a Docker container and a Database in SQL. It also showcases the multithread and multiprocess features of Python. 

#-- This program collects the container IDs and other parameters using multiprocessing and writes them into a database.


import sys
import docker
import concurrent.futures
import time
import pdb
import psutil

import multiprocessing
from concurrent.futures import ProcessPoolExecutor
from multiprocessing import *

import pickle
import _pickle as cPickle

from concurrent.futures import ProcessPoolExecutor, wait
from multiprocessing import cpu_count

import datetime
import mysql.connector

#--

def get_container_id():

	client = docker.from_env()

	i = 0
	ls = []

	while (i < len(sys.argv)):
		ls.append(sys.argv[i])
		i += 1

	ls.remove(ls[0])
	print (ls)

	#with concurrent.futures.ThreadPoolExecutor (max_workers=3) as executor: 
	with concurrent.futures.ProcessPoolExecutor (max_workers = len(ls)) as executor:
		for iterator in ls:
			cont = client.containers.get(iterator)

			ID = cont.id
			ID = ID [:10]

			#for i in cont.logs(stream=True):
			#	print (i)

			output = executor.submit (structured_print, ID)
			output.add_done_callback (callback)

	print ("Added Callback")

#--

def check_if_db_exists():
	mydb = mysql.connector.connect (
		host = "localhost",
		user = "docker",
		passwd = "docker123"
	)

	ls_db = []

	mycursor = mydb.cursor()
	mycursor.execute ("show databases")

	for i in mycursor:
		ls_db.append(i)

	#print ("LIST_OF_DATABASES ----> {}".format(ls_db))

	if (('logger',) not in ls_db):
		create_dbase()

	else:
		get_container_id()

#--

def structured_print(ID):
	client = docker.from_env()
	cont = client.containers.get(ID)

	print (type(ID))

	p = psutil.Process()
	cpu_num = str(p.cpu_num())
	proc_id = str(p.pid)

	for line in cont.logs(stream=True):
		new_log = line.strip()
		str_log = new_log.decode("utf-8")

		c_time = str((datetime.datetime.now()))
		# pdb.set_trace()
		print ("{} : {} : {} : {} : {}".format(ID, cpu_num, proc_id, str_log, c_time))

		tup_items = (ID, cpu_num, proc_id, str_log, c_time)

		load_to_db(tup_items)


#--

def callback (exec):
	print ("---- END ----")

#--

def create_dbase():
	mydb = mysql.connector.connect (
		host = "localhost",
		user = "docker",
		passwd = "docker123",
		database = "logger"
	)

	mycursor = mydb.cursor()
	mycursor.execute ("create table log_table (id varchar (300), cpu_num varchar (300), proc_id varchar (300), str_log varchar(300), c_time varchar (300) primary key)")


#--

def load_to_db (tup_items):
	tup = tup_items

	mydb = mysql.connector.connect(
		host="localhost",
		user="docker",
		passwd="docker123",
		database="logger"
	)

	mycursor = mydb.cursor()

	sql = "insert into log_table (ID, cpu_num, proc_id, str_log, c_time) values (%s, %s, %s, %s, %s)"
	val = (tup[0], tup[1], tup[2], tup[3], tup[4])

	mycursor.execute (sql, val)

	mydb.commit()

	#print (mycursor.rowcount, "\nTable updated successfully !\n\n")

#--

def delete_complete_table():
	mydb = mysql.connector.connect(
		host="localhost",
		user="docker",
		passwd="docker123",
		database="logger"
	)

	mycursor = mydb.cursor()
	mycursor.execute ("truncate table log_table")

#--

def structured_print_prev(ID):
	client = docker.from_env()
	cont = client.containers.get(ID)

	#print ("{} --- {} --- {}".format(ID, type(ID), cont))

	p = psutil.Process()
	cpu_num = str(p.cpu_num())
	proc_id = str(p.pid)


	str_fin = ""
	for line in cont.logs(stream = True):
		new_log = line.strip()
		str_log = new_log.decode ("utf-8")
		if str_log  != "\n":
			str_fin = str_fin + str_log

		print (str_fin)
		if (str_fin[0]=="R") and len(str_fin)>25:
			print (str_fin)
			c_time = str((datetime.datetime.now()))

			print ("{} : {} : {} : {} : {}".format(ID, cpu_num, proc_id, str_fin, c_time))
			tup_items = (ID, cpu_num, proc_id, str_fin, c_time)

			load_to_db(tup_items)
			str_fin = ""
		

#--

if __name__ == '__main__':
	#get_container_id()
	check_if_db_exists()

	#delete_complete_table()

