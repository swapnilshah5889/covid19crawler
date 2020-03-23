# -*- coding: utf-8 -*-
"""
Created on Fri Mar 20 16:28:48 2020

@author: swapn
"""

import requests
import lxml.html as lh
import pandas as pd
import numpy as np

def convertNan(data):
    
    if data == np.nan:
        return ""
    else:
        return data
    
    


url='https://en.wikipedia.org/wiki/2020_coronavirus_pandemic_in_India'


print("Web scraping started...")
page = requests.get(url)

doc = lh.fromstring(page.content)


import requests
from bs4 import BeautifulSoup

soup = BeautifulSoup(page.content, 'html.parser')
#print(soup.prettify())

tb = soup.find('table', class_='wikitable mw-collapsible mw-made-collapsible')


rows = soup.find_all('tr')

today = 0
result = []
states = []

col = []
col.append("Date")

col_flag = 0

months = ['Jan','Feb','Mar']

for row in rows:
    row_td = row.find_all('td')
    #print(row_td)
    str_cells = str(row_td)
    cleantext = BeautifulSoup(str_cells, "lxml").get_text()
    cleantext = cleantext[1:-1]
    result = [x.strip() for x in cleantext.split(', ')]
    #print(result)
    
    if 'Andhra Pradesh, ' in cleantext:
        #print(cleantext)
        states = [x.strip() for x in cleantext.split(', ')]
        while "" in states:    
            states.remove("")
            
        for i in states:
            i1 = i.replace(" ","_")
            col.append(i1)
            
        col.append("New_Cases")
        col.append("Total_Cases")
        col.append("Difference")
        col.append("New_Deaths")
        col.append("Total_Deaths")
        
        df = pd.DataFrame(columns = col)
        col_flag = 1
        
        #print(col)
    
    split_result = result[0].split("-")
    if col_flag == 1 and len(result) == 1+(len(col)) and (split_result[0] in months or split_result[1] in months) :
        #print(result)
        
        temp_dict = {}
        for p in range(len(col)):
            temp_dict[col[p]] = result[p]
            
        df = df.append(temp_dict,ignore_index=True)
        """
        df = df.append({col[0]:result[0],col[1]:result[1],col[2]:result[2],
                        col[3]:result[3],col[4]:result[4],col[5]:result[5],
                        col[6]:result[6],col[7]:result[7],col[8]:result[8],
                        col[9]:result[9],col[10]:result[10],col[11]:result[11],
                        col[12]:result[12],col[13]:result[13],col[14]:result[14],
                        col[15]:result[15],col[16]:result[16],col[17]:result[17],
                        col[18]:result[18],col[19]:result[19],col[20]:result[20],
                        col[21]:result[21],col[22]:result[22],col[23]:result[23],
                        col[24]:result[24],col[25]:result[25],col[26]:result[26],
                        col[27]:result[27]},ignore_index=True)
        """
        #print(result)
        
        
#for i in df.index:
    #print(df['Date'][i])
    

print("Web scraping successful !!!")
    
import mysql.connector

"""
mydb = mysql.connector.connect(
  host="dev.mobihealth.in",
  user="root",
  passwd="siMplEpssxWd#",
  database = "covid19"
)
"""

mydb = mysql.connector.connect(
  host="localhost",
  user="root",
  passwd="",
  database = "covid19"
)

mycursor = mydb.cursor()

#Drop and Create table 
def DropAndCreateTable():
    sql = "DROP TABLE IF EXISTS covid19_india_wikipidea"
    mycursor.execute(sql)
    print("Table Dropped")
    
    sql = "CREATE TABLE covid19_india_wikipidea (id INT(6) UNSIGNED AUTO_INCREMENT PRIMARY KEY, "
    
    for i in range(len(col)):
        if i < len(col)-1:
            sql+=col[i]+" VARCHAR(11), "
        else:
            sql+=col[i]+" VARCHAR(11) )"
    
    mycursor.execute(sql)
    print("New table created !!!")


#Save to csv file        
#df.to_csv('covid19_india_wikipidea.csv',encoding='utf-8',index=False)



#Insert data into database
def InsertData():
    insert_q = "INSERT INTO covid19_india_wikipidea ("
    insert_q2 = "VALUES ("
    for i in range(len(col)):
        if i < len(col)-1:
            insert_q += col[i]+", "
            insert_q2 += "%s, "
        else:
            insert_q += col[i]+") "
            insert_q2 += "%s) "
    
    insert_q += insert_q2
    print("Data insertion started...")
    for pos in df.index:    
        values = []
        for i in range(len(col)):
            values.append(df[col[i]][pos])
            #print(df[col[i]][pos])
        final_values = tuple(values)
        print(final_values)
        mycursor.execute(insert_q, final_values)
        mydb.commit()
        
    print("\n"+str(i)+" Records inserted !")
    
    
def UpdateData():
    mycursor.execute("SELECT * FROM covid19_india_wikipidea ORDER BY id DESC LIMIT 1")
    myresult = mycursor.fetchall()
    
    database_data = myresult[0][1:]
    
    latest_data = df.iloc[-1]
    
    inequal_pos = []
    if len(latest_data) == len(database_data):
        if latest_data[0] == database_data[0]:
            for i in range(len(latest_data)):
                if str(latest_data[i]) != str(database_data[i]):
                    #print(latest_data[i])
                    #print(database_data[i])
                    inequal_pos.append(i)
            
        else:
            print("Date Different ! Today's data has not been inserted into the database !")
            insert_q = "INSERT INTO covid19_india_wikipidea ("
            insert_q2 = "VALUES ("
            for i in range(len(col)):
                if i < len(col)-1:
                    insert_q += col[i]+", "
                    insert_q2 += "%s, "
                else:
                    insert_q += col[i]+") "
                    insert_q2 += "%s) "
                    
            insert_q += insert_q2
            print("Data insertion started...")
            values = []
            for i in latest_data:
                values.append(i)
                #print(df[col[i]][pos])
            final_values = tuple(values)
            #print(final_values)
            mycursor.execute(insert_q, final_values)
            mydb.commit()
            print("Today's data inserted !!!")
            return
    
    else:
        print("Column inconsistent ! New column has been inserted !")
        DropAndCreateTable()
        InsertData()
        print("\nDatabase has been updated !!!")
        return
        
        
        
                
    if len(inequal_pos) != 0:
        print(len(inequal_pos))
        print("New data found ! Update in database")
        
        indexes = latest_data.index
        
        update_q = "UPDATE covid19_india_wikipidea SET "
        for pos in range(len(inequal_pos)):
            if pos < len(inequal_pos)-1:
                update_q +=indexes[inequal_pos[pos]]+" = '"+str(latest_data[indexes[inequal_pos[pos]]])+"', "
            else:
                update_q +=indexes[inequal_pos[pos]]+" = '"+str(latest_data[indexes[inequal_pos[pos]]])+"' "
        
        update_q += "WHERE id = '"+str(myresult[0][0])+"' "
        
        mycursor.execute(update_q)
        mydb.commit()
        print("Database updated !!!")
        
        
    else:
        print("Data up to date !")
        
        

#Uncomment these two methods when running the file for the first time then comment it.
#These 2 methods creates the table in the database and inserts all the data at once that is scrapped from the website
#DropAndCreateTable()
#InsertData()
 
#Uncomment the below method after the first run of this file
#This method updates today's data as well as inserts data if date is changed or new columns are added. 
#Run this method at particular time intervals to keep the database updated. 
UpdateData()
       
    
    
        
