#PGSQL 데이터 -> DW 적재

#import sqlite3
import pyodbc
#import MySQLdb
#import mysqlclient
import datetime
# import threading
import sys
import time

def PGSQLtoMSSQL():

    try:

        #print(pyodbc.drivers())

        '''
        # SOURCE SERVER INFO
        cursor = cnxn.cursor()
        '''
        # SOURCE SERVER INFO
        cnxn = pyodbc.connect("Driver={PostgreSQL UNICODE};Server=123.123.123.123;Port=5432;Database=postgres;Uid=postgres;Pwd=password;")
        cursor = cnxn.cursor()
        
        # TARGET SERVER INFO
        cnxn2 = pyodbc.connect('DRIVER={ODBC Driver 13 for SQL Server};SERVER=123.123.123.123;DATABASE=database;UID=userid;PWD=password')
        cursor2 = cnxn2.cursor()

        # SQL
        # table  = "QLT_JAJOO"
        # ilja   = "2017-05-15"
        # addsql = " WHERE ILJA < '" + ilja + "'"
        # sql    = "SELECT * FROM " + table + addsql

        '''
        cursor.execute("""\
        SELECT PJI.*, PJO.*, 
                CST.ABCGS 
        FROM  dbo.Traverse AS TRE 
                      LEFT OUTER JOIN dbo.TraversePreEntry AS TPE 
                            ON TRE.JobNum = dbo.GetJobNumberFromGroupId(TPE.GroupId)
                      LEFT OUTER JOIN AutoCADProjectInformation AS PJI
                            ON TRE.JobNum = PJI.JobNumber
                      LEFT OUTER JOIN CalculationStorageReplacement AS CST
                            ON CST.ProjectNumber = dbo.GetJobNumberFromGroupId(TPE.GroupId)
                      LEFT OUTER JOIN dbo.TraverseElevations AS TEV
                          ON TRE.TraverseId = TEV.TraverseId
                      LEFT OUTER JOIN VGSDB.dbo.ProjectOffice PJO
                            ON PJI.PjbId = PJO.PjbId
        where jobnum = 1205992""")
        '''

        '''
        changelog
        errorldc
        joborder

        logininfo
        measurement
        pcinfo
        prd010
        producti
        producti_sum
        producti_sum_n
        qualityparm
        statisticdata
        terminalqdc
        '''

        #select * from pplsbomd_v where last_yn = 'Y'
        tableGroup = ['pplsbomd_v']
        #tableGroup = ['pcinfo']

        for tablename in tableGroup:

            #tablename = 'joborder'

            #select mgmt_seq, subj_title, s_fob_pric from
            sql = ""
            sql = sql + """\
            select * from             
            """
            sql = sql + tablename
            sql = sql + " where last_yn = 'Y'"
            #sql = sql + " and mgmt_seq = '2019-0002' and d_part_no_upper = 'EEA00377AB'"


            print(sql)
            print(tablename)

            msg = "\nselect start: " + str(datetime.datetime.now())
            print(msg)

            r = cursor.execute(sql)
                   
            c = [column[0] for column in r.description]

            results = []
            
            k = 0

            for row in cursor.fetchall():

                k += 1
                results.append(dict(zip(c, row)))

                #print(row)

            msg = "%s row(s)" % (str(k))
            print(msg)

            msg = "select end  : " + str(datetime.datetime.now())
            print(msg)

            msg = "\ninsert start: " + str(datetime.datetime.now())
            print(msg)

            k = 0
            for myDict in results:
                
                k += 1
                columns = ','.join(myDict.keys())

                # %s 인 경우에는 에러 발생하여 ? 로 placeholders 변경
                placeholders = ','.join(['?'] * len(myDict))
                #print(placeholders)

                #sql = "insert into " + tablename + " (%s) values (%s)" % (columns, placeholders)
                sql = "insert into " + 'new_table' + " (%s) values (%s)" % (columns, placeholders)

                #print(sql)
                #print(list(myDict))
                #print(list(myDict.values()))

                #리스트내의 decimal 을 문자열로 변환
                #new_list = [str(i) for i in list(myDict.values())]

                new_list = []
                for i in list(myDict.values()):

                    if i is None:
                        new_list.append(None)
                    #elif str(i) == "0E-20":
                    elif str(i)[:2] == "0E":

                        dec = str(i)[-2:]
                        zero = "0."
                        for j in range(int(dec)):
                            zero = zero + '0'

                        #print(i)
                        #print("here")
                        #new_list.append("0.00000000000000000000")
                        new_list.append(zero)

                    else:
                        new_list.append(str(i))

                    '''
                    if str(i).isdecimal:
                        new_list.append(str(i))
                    elif i is None:
                        new_list.append("NULL")
                    else:
                        new_list.append(i)
                    '''


                #print(new_list)
                #return

                #cursor2.execute(sql, list(myDict.values()))
                cursor2.execute(sql, new_list)
                
                #sys.stdout.write("#")
                #sys.stdout.flush()

                msg = "%s row(s)" % (str(k))
                print(msg, end='')
                print("\r", end='')

                
            msg = "%s row(s)" % (str(k))
            print(msg)

            msg = "insert end  : " + str(datetime.datetime.now())
            print(msg)
            
            cnxn2.commit()

            time.sleep(5)
        
    except Exception as e:
        print('error:', e)

PGSQLtoMSSQL()
