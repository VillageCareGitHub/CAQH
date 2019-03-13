import pandas as pd
import psycopg2
import configparser
import os
import pysftp
import datetime
import glob

# SCHEMA SELECTION
DBSCHEMA=''

# GET CURRENT DATE
NOW=datetime.datetime.now()

def importproviders(filename,actualfile):
    # AWS Credentials Information for Databases
    initConfig = configparser.ConfigParser()
    initConfig.read("AWS_List.config") 

    # Database connection to redshift
    conn=psycopg2.connect(dbname= 'dev', host=initConfig.get('profile prod', 'host'), 
    port= initConfig.get('profile prod', 'port'), user= initConfig.get('profile prod', 'dbuser'), password= initConfig.get('profile prod', 'dbpwd')) 
    #print(filename)   
    caqh=pd.read_json(filename,orient='columns')

    # getting columns
    value=caqh["providers"]
    insertfields=""
    insertvalues=""
    for key in range(len(value)):
        newkey=value[key]
        for dkey,dvalue in newkey.items():
            #print(type(dvalue))
            if type(dvalue)==str or type(dvalue)==int:            
                if insertfields=="":
                    insertfields="fileuploaded,"+"uploaddate"+","+str(dkey)
                else:
                    insertfields=insertfields+","+str(dkey)

                if insertvalues=="":
                    insertvalues="'"+actualfile+"'"+","+"'"+str(NOW)+"'"+","+"'"+str(dvalue).replace("'","''")+"'"
                else:
                    insertvalues=insertvalues+","+"'"+str(dvalue).replace("'","''")+"'"
        #print(insertfields)
        #print(insertvalues)
        newinsert="INSERT INTO caqh_providers ({}) values ({})".format(insertfields,insertvalues)
        cur=conn.cursor()
        cur.execute(newinsert)
        conn.commit()
        insertfields=""
        insertvalues=""
        
    conn.close()

def importotherinfo(filename,actualfile):
    # AWS Credentials Information for Databases
    initConfig = configparser.ConfigParser()
    initConfig.read("AWS_List.config") 

    # Database connection to redshift
    conn=psycopg2.connect(dbname= 'dev', host='vcs-dw-01.cjpkvjykaaiu.us-east-1.redshift.amazonaws.com', 
    port= '5439', user= initConfig.get('profile prod', 'dbuser'), password= initConfig.get('profile prod', 'dbpwd'))    
    #print(filename)
    caqh=pd.read_json(filename,orient='columns')
    # getting columns
    value=caqh["providers"]
    insertfields=""
    insertvalues=""
    for key in range(len(value)):
        newkey=value[key]
        for dkey,dvalue in newkey.items():            
            #getting primary key value
            if type(dvalue)==int:                
                if dkey=='caqh_id':
                    pkid=str(dvalue)
                    
            if type(dvalue)==list:
                if len(dvalue)>0:
                    listkey=dvalue[0]                    
                    if type(listkey)==dict:
                        for lkey,lvalue in listkey.items():
                            if str(lkey)=='6_month_gaps':
                                newkey='six_month_gaps'
                            elif str(lkey)=='re-verification_date':
                                newkey='reverification_date'
                            else:
                                newkey=str(lkey)

                            if insertfields=="":
                                insertfields="caqh_id,"+"fileuploaded,"+"uploaddate"+","+str(newkey)
                            else:
                                insertfields=insertfields+","+str(newkey)
                                
                            if insertvalues=="":
                                insertvalues="'"+pkid+"'"+","+"'"+actualfile+"'"+","+"'"+str(NOW)+"'"+","+"'"+str(lvalue).replace("'","''")+"'"
                            else:
                                insertvalues=insertvalues+","+"'"+str(lvalue).replace("'","''")+"'"
                    if insertfields!="" and insertvalues!="":
                        newinsert="INSERT INTO {} ({}) values ({})".format("caqh_"+str(dkey),insertfields,insertvalues)
                        #print(newinsert)
                        cur=conn.cursor()
                        cur.execute(newinsert)
                        conn.commit()
    
            #print(insertfields)
            #print(insertvalues)
            # insertfields=""
            # insertvalues=""

            if type(dvalue)==dict:
                if len(dvalue)>0:
                    #print(dvalue)
                    if str(dkey).lower()=='category_counts':
                        insertfields="caqh_id,A,B,C,D,Q,fileuploaded,uploaddate"
                        insertvalues="'{}','{}','{}','{}','{}','{}','{}','{}'".format(pkid,dvalue['A'],dvalue['B'],dvalue['C'],dvalue['D'],dvalue['Q'],actualfile,NOW)
                        if insertfields!="" and insertvalues!="":
                            newinsert="INSERT INTO {} ({}) values ({})".format("caqh_"+str(dkey),insertfields,insertvalues)
                            #print(newinsert)
                            cur=conn.cursor()
                            cur.execute(newinsert)
                            conn.commit()
                    
            insertfields=""
            insertvalues=""


    
    conn.close()



               


# going through file directory
Path = "C:\\Users\Public\Documents\Python Scripts\RE__Need_Access_to_JSON_and_XML_files\\"

filelist = os.listdir(Path)
for i in filelist:
    if i.startswith("795_PSV"):  # You could also add "and i.startswith('f')
        with open(Path + i, 'r') as filename:
            #print(i)
            importproviders(filename,i)
        
        with open(Path + i, 'r') as filename:
            importotherinfo(filename,i)
            #print(i)

# list_of_files = glob.glob(Path+'795_PSV*.*') # * means all if need specific format then *.csv
# latest_file = max(list_of_files, key=os.path.getctime)
# print (latest_file)
