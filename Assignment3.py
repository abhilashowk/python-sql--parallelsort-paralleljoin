#!/usr/bin/python2.7
#
# Assignment3 Interface
#Submitted By: Abhilash Owk

import psycopg2
import os
import sys
import threading

##################### This needs to changed based on what kind of table we want to sort. ##################
##################### To know how to change this, see Assignment 3 Instructions carefully #################
FIRST_TABLE_NAME = 'MovieRating'
SECOND_TABLE_NAME = 'MovieBoxOfficeCollection'
SORT_COLUMN_NAME_FIRST_TABLE = 'Rating'
SORT_COLUMN_NAME_SECOND_TABLE = 'Collection'
JOIN_COLUMN_NAME_FIRST_TABLE = 'MovieID'
JOIN_COLUMN_NAME_SECOND_TABLE = 'MovieID'
##########################################################################################################


# Donot close the connection inside this file i.e. do not perform openconnection.close()
def ParallelSort (InputTable, SortingColumnName, OutputTable, openconnection):
	cur=openconnection.cursor()
	print "hi"
	cur.execute("DROP TABLE IF EXISTS "+OutputTable)
	cur.execute("CREATE TABLE "+OutputTable+" AS SELECT * FROM "+"\""+InputTable+"\""+" WHERE 1=0")
	cur.execute("select max("+"\""+SortingColumnName+"\""+") from "+"\""+InputTable+"\"")
	max=cur.fetchone()
	cur.execute("select min("+"\""+SortingColumnName+"\""+") from "+"\""+InputTable+"\"")
	min=cur.fetchone()
	range1=(max[0]-min[0])/5
	#print range1
	min1=min[0]
	#print min1
	print "Starting Threads for Parallel Sort..."
	for i in range(1,6):
		#print "worked"
		startValue=min1
		endValue=min1+range1
		#print endValue
		#print "hi"
		thread=threading.Thread(target=RangePartition, args=(i,InputTable, SortingColumnName, OutputTable, openconnection,startValue, endValue))
		#print "Thread "+str(i)+" Starting"
		thread.start()
		thread.join()
		min1=endValue
	print "End of Execution for Parallel Sort"
    #Implement ParallelSort Here.
    #pass #Remove this once you are done with implementation

def RangePartition(i,InputTable, SortingColumnName, OutputTable, openconnection,startValue, endValue):
	cur=openconnection.cursor()
	if(i==1):
		cur.execute("select * from "+"\""+InputTable+"\""+" where "+"\""+SortingColumnName+"\""+">="+str(startValue)+ "and" +"\""+SortingColumnName+"\""+"<="+str(endValue)+ "order by "+"\""+SortingColumnName+"\""+" asc")
		temp=cur.fetchall()
		#print temp
	else:
		cur.execute("select * from "+"\""+InputTable+"\""+" where "+"\""+SortingColumnName+"\""+">"+str(startValue)+ "and" +"\""+SortingColumnName+"\""+"<="+str(endValue)+ "order by "+"\""+SortingColumnName+"\""+" asc")
		temp=cur.fetchall()
		#print temp
	rows = []
	for row in temp:
		rows = []
		for j in range(0,len(row)):
			rows.append(str(row[j]))
        #print(rows)
        #print(row)
		temp = ",".join(rows)
		#print "check this"
		#print(temp)
		q = "INSERT INTO "+OutputTable+" VALUES("+temp+")"
		#print(q)
		cur.execute(q)
        
    
    
	openconnection.commit()
		
def ParallelJoin (InputTable1, InputTable2, Table1JoinColumn, Table2JoinColumn, OutputTable, openconnection):
    #Implement ParallelJoin Here.
    #pass # Remove this once you are done with implementation
	#print("hello")
	cur = openconnection.cursor()
	cur.execute("create table "+OutputTable+" as SELECT * FROM "+"\""+InputTable1+"\""+" INNER JOIN "+"\""+InputTable2+"\""+" USING ("+"\""+Table1JoinColumn+"\""+")")
	cur.execute("delete from "+OutputTable)
	openconnection.commit()
	cur = openconnection.cursor()
	cur.execute("select min("+"\""+Table1JoinColumn+"\""+") from "+"\""+InputTable1+"\"")
	min1val = cur.fetchone()
	#print(min1val)
	cur.execute("select min("+"\""+Table2JoinColumn+"\""+") from "+"\""+InputTable2+"\"")
	min2val = cur.fetchone()
	#print(min2val)
	cur.execute("select max("+"\""+Table1JoinColumn+"\""+") from "+"\""+InputTable1+"\"")
	max1val = cur.fetchone()
	#print(max1val)
	cur.execute("select min("+"\""+Table2JoinColumn+"\""+") from "+"\""+InputTable2+"\"")
	max2val = cur.fetchone()
	#print(max2val)
	mi1 = float(min1val[0])
	ma1 = float(max1val[0])
	ma2 = float(max2val[0])
	mi2 = float(min2val[0])
	if mi1 > mi2:
		minval = mi2
	else:
		minval = mi1
	if ma1 > ma2:
		maxval = ma1
	else:
		maxval = ma2
	#print("minvalue is:"+str(minval)+" maxvalue is:"+str(maxval))    
	rangeval = (maxval - minval)/5.0
	#print("range is"+str(rangeval))
	print "Starting threads for Parallel Join"
	try:
		for i in range (1,6):
			#print(i)
			startval =minval
			endval = minval+rangeval
			t=threading.Thread(target=RangePartition1 ,args=(i,InputTable1, InputTable2, Table1JoinColumn, Table2JoinColumn, OutputTable, openconnection, startval, endval))
			t.start()
			t.join()
			minval = endval
        
	except:
		print("Unable to start thread")
	print "Ended execution for Parallel Join"
	openconnection.commit()
	cur.close()
    #openconnection.close()

def RangePartition1(i,InputTable1, InputTable2, Table1JoinColumn, Table2JoinColumn, OutputTable, openconnection, startval, endval):
	#print("helloagain")
	cur = openconnection.cursor()
    #cur2 = openconnection.cursor()
	if i==1:
		cur.execute("SELECT * FROM "+"\""+InputTable1+"\""+" WHERE "+"\""+Table1JoinColumn+"\""+" >= "+str(startval)+" AND "+"\""+Table1JoinColumn+"\""+" <= "+str(endval))
		values1 = cur.fetchall()
		cur.execute("SELECT * FROM "+"\""+InputTable2+"\""+" WHERE "+"\""+Table2JoinColumn+"\""+" >= "+str(startval)+" AND "+"\""+Table2JoinColumn+"\""+" <= "+str(endval))
		values2 = cur.fetchall()
	else:
		cur.execute("SELECT * FROM "+"\""+InputTable1+"\""+" WHERE "+"\""+Table1JoinColumn+"\""+ ">= "+str(startval)+" AND "+"\""+Table1JoinColumn+"\""+" <= "+str(endval))
		values1 = cur.fetchall()
		cur.execute("SELECT * FROM "+"\""+InputTable2+"\""+" WHERE "+"\""+Table2JoinColumn+"\""+" > "+str(startval)+" AND "+"\""+Table2JoinColumn+"\""+" <= "+str(endval))
		values2 = cur.fetchall()
    
	#print("done with thread"+str(i))
	cur.execute("DROP TABLE IF EXISTS PART1"+str(i))
	cur.execute("DROP TABLE IF EXISTS PART2"+str(i))
	cur.execute("CREATE TABLE PART1"+str(i)+" AS SELECT * FROM "+"\""+InputTable1+"\""+" where 1=0")
	#print("created part1next")
	cur.execute("CREATE TABLE PART2"+str(i)+" AS SELECT * FROM "+"\""+InputTable2+"\""+" where 1=0")
	#print("created part2next")
	for row in values1:
		rows = []
		for j in range(0,len(row)):
			rows.append(str(row[j]))
		#print "the rows are"
		#print(rows)
        #print(row)
		temp = ",".join(rows)
		#print("PART1"+str(i))
		#print(temp)
		cur.execute("INSERT INTO PART1"+str(i)+" VALUES ("+temp+")")
	for row in values2:
		rows = []
		for j in range(0,len(row)):
			rows.append(str(row[j]))
		temp = ",".join(rows)
		cur.execute("INSERT INTO PART2"+str(i)+" VALUES ("+temp+")")
	#cur.execute("SELECT * FROM PART1"+str(i)+" INNER JOIN PART2"+str(i)+" ON PART1"+str(i)+"."+"\""+Table1JoinColumn+"\""+"=PART2"+str(i)+"."+"\""+Table2JoinColumn+"\"")
	cur.execute("SELECT * FROM PART1"+str(i)+" INNER JOIN PART2"+str(i)+" USING ("+"\""+Table1JoinColumn+"\""+")")
	outputtab = cur.fetchall()
	for row in outputtab:
		rows = []
		for j in range(0,len(row)):
			rows.append(str(row[j]))
		temp = ",".join(rows)
		cur.execute("INSERT INTO "+OutputTable+" VALUES ("+temp+")")
	cur.execute("drop table PART1"+str(i)+",PART2"+str(i))
	openconnection.commit()	
################### DO NOT CHANGE ANYTHING BELOW THIS #############################


# Donot change this function
def getOpenConnection(user='postgres', password='1234', dbname='ddsassignment3'):
    return psycopg2.connect("dbname='" + dbname + "' user='" + user + "' host='localhost' password='" + password + "'")

# Donot change this function
def createDB(dbname='ddsassignment3'):
    """
    We create a DB by connecting to the default user and database of Postgres
    The function first checks if an existing database exists for a given name, else creates it.
    :return:None
    """
    # Connect to the default database
    con = getOpenConnection(dbname='postgres')
    con.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
    cur = con.cursor()

    # Check if an existing database with the same name exists
    cur.execute('SELECT COUNT(*) FROM pg_catalog.pg_database WHERE datname=\'%s\'' % (dbname,))
    count = cur.fetchone()[0]
    if count == 0:
        cur.execute('CREATE DATABASE %s' % (dbname,))  # Create the database
    else:
        print 'A database named {0} already exists'.format(dbname)

    # Clean up
    cur.close()
    con.commit()
    con.close()

# Donot change this function
def deleteTables(ratingstablename, openconnection):
    try:
        cursor = openconnection.cursor()
        if ratingstablename.upper() == 'ALL':
            cursor.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'")
            tables = cursor.fetchall()
            for table_name in tables:
                cursor.execute('DROP TABLE %s CASCADE' % (table_name[0]))
        else:
            cursor.execute('DROP TABLE %s CASCADE' % (ratingstablename))
        openconnection.commit()
    except psycopg2.DatabaseError, e:
        if openconnection:
            openconnection.rollback()
        print 'Error %s' % e
        sys.exit(1)
    except IOError, e:
        if openconnection:
            conn.rollback()
        print 'Error %s' % e
        sys.exit(1)
    finally:
        if cursor:
            cursor.close()

# Donot change this function
def saveTable(ratingstablename, fileName, openconnection):
    try:
        cursor = openconnection.cursor()
        cursor.execute("Select * from %s" %(ratingstablename))
        data = cursor.fetchall()
        openFile = open(fileName, "w")
        for row in data:
            for d in row:
                openFile.write(`d`+",")
            openFile.write('\n')
        openFile.close()
    except psycopg2.DatabaseError, e:
        if openconnection:
            openconnection.rollback()
        print 'Error %s' % e
        sys.exit(1)
    except IOError, e:
        if openconnection:
            conn.rollback()
        print 'Error %s' % e
        sys.exit(1)
    finally:
        if cursor:
            cursor.close()

if __name__ == '__main__':
    try:
	# Creating Database ddsassignment2
	print "Creating Database named as ddsassignment2"
	createDB();
	
	# Getting connection to the database
	print "Getting connection from the ddsassignment2 database"
	con = getOpenConnection();

	# Calling ParallelSort
	print "Performing Parallel Sort"
	ParallelSort(FIRST_TABLE_NAME, SORT_COLUMN_NAME_FIRST_TABLE, 'parallelSortOutputTable', con);

	# Calling ParallelJoin
	print "Performing Parallel Join"
	ParallelJoin(FIRST_TABLE_NAME, SECOND_TABLE_NAME, JOIN_COLUMN_NAME_FIRST_TABLE, JOIN_COLUMN_NAME_SECOND_TABLE, 'parallelJoinOutputTable', con);
	
	# Saving parallelSortOutputTable and parallelJoinOutputTable on two files
	saveTable('parallelSortOutputTable', 'parallelSortOutputTable.txt', con);
	saveTable('parallelJoinOutputTable', 'parallelJoinOutputTable.txt', con);

	# Deleting parallelSortOutputTable and parallelJoinOutputTable
	deleteTables('parallelSortOutputTable', con);
       	deleteTables('parallelJoinOutputTable', con);

        if con:
            con.close()

    except Exception as detail:
        print "Something bad has happened!!! This is the error ==> ", detail
