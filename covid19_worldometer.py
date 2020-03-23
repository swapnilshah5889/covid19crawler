import requests
import lxml.html as lh
import pandas as pd


def ConvertToInt(num):
    
    num = num.rstrip()
    if "," in num:
        
        res = num.split(",")
        s = ""
        for i in res:
            s+=i
            
        return int(s)
    elif num != "" :
        return int(num)
    else:
        return 0
    
def CleanString(data):
    data = data.rstrip()

    if data.startswith("Tot") and "1M" in data and "pop" in data.lower():
        return "Total_Cases_1M_Pop"
    
    if data == "":
        return ""
    
    if "," in data:
        res = data.split(",")
        s = ""
        for i in res:
            s+=i
        data = s
    
    data = data.replace("/","_")
    data = data.replace(' ','_')
    data = data.replace(" ","_")
    
    
    
    return data
    

url='https://www.worldometers.info/coronavirus/'


print("Web-Scraping Started...")
page = requests.get(url)

doc = lh.fromstring(page.content)

tr_elements = doc.xpath('//tr')

[len(T) for T in tr_elements[:]]


tr_elements = doc.xpath('//tr')
#Create empty list
col=[]
i=0
#For each row, store each first element (header) and an empty list
for t in tr_elements[0]:
    i+=1
    name=t.text_content()
    name = CleanString(name)
    #print(str(i)+':'+str(name))
    col.append((name,[]))
    
    
for j in range(1,len(tr_elements)):
    #T is our j'th row
    T=tr_elements[j]
    
    #If row is not of size 10, the //tr data is not from our table 
    if len(T)!=10:
        break
    
    #i is the index of our column
    i=0
    
    #Iterate through each element of the row
    for t in T.iterchildren():
        data=t.text_content() 
        #Check if row is empty
        if i>0:
        #Convert any numerical value to integers
            try:
                data=int(data)
            except:
                pass
        #Append the data to the empty list of the i'th column
        col[i][1].append(data)
        #Increment i for the next column
        i+=1
        
[len(C) for (title,C) in col]


Dict={title:column for (title,column) in col}
df=pd.DataFrame(Dict)



import requests
from bs4 import BeautifulSoup

soup = BeautifulSoup(page.content, 'html.parser')
#print(soup.prettify())

tb = soup.find('table', class_='table table-bordered table-hover main_table_countries dataTable no-footer')


rows = soup.find_all('tr')

today = 0
result = []
countries = []
for row in rows:
    row_td = row.find_all('td')
    #print(row_td)
    str_cells = str(row_td)
    cleantext = BeautifulSoup(str_cells, "lxml").get_text()
    cleantext = cleantext[1:-1]
    result = [x.strip() for x in cleantext.split(', ')]
    
    if len(result) == 9:
        df = df.append({col[0][0]:result[0],col[1][0]:result[1],col[2][0]:result[2],
                        col[3][0]:result[3],col[4][0]:result[4],col[5][0]:result[5],
                        col[6][0]:result[6],col[7][0]:result[7],col[8][0]:result[8]},ignore_index=True)
        countries.append(result[0])
        #print(result)
        
    if result[0] == "Total:":
        break
    

print("Web-Scraping completed !")

df["TotalRecovered"] = df["TotalRecovered"].apply(ConvertToInt)
df["TotalCases"] = df["TotalCases"].apply(ConvertToInt)

#df["Percentage-recovered"] = df["TotalRecovered"]/df["TotalCases"]*100



import mysql.connector

mydb = mysql.connector.connect(
  host="localhost",
  user="root",
  passwd="",
  database = "covid19"
)

print("Database Connection successful !")

mycursor = mydb.cursor()

table = "covid19_worldometer"



#Drop and Create table 
def DropAndCreateTable():
    sql = "DROP TABLE IF EXISTS "+table
    mycursor.execute(sql)
    print("Table Dropped")
    
    sql = "CREATE TABLE "+table+" (id INT(6) UNSIGNED AUTO_INCREMENT PRIMARY KEY, "
    
    for i in range(len(col)):
        if i < len(col)-1:
            sql+=col[i][0]+" VARCHAR(50), "
        else:
            sql+=col[i][0]+" VARCHAR(50) )"
    
    mycursor.execute(sql)
    print("New table created !!!")
    
    
def InsertData():
    insert_q = "INSERT INTO "+table+" ("
    insert_q2 = "VALUES ("
    for i in range(len(col)):
        if i < len(col)-1:
            insert_q += col[i][0]+", "
            insert_q2 += "%s, "
        else:
            insert_q += col[i][0]+") "
            insert_q2 += "%s) "
    
    insert_q += insert_q2
    print("Data insertion started...")
    for pos in df.index:    
        values = []
        for i in range(len(col)):
            data = str(df[col[i][0]][pos])
            values.append(data)
            #print(df[col[i]][pos])
        final_values = tuple(values)
        print(final_values)
        mycursor.execute(insert_q, final_values)
        mydb.commit()
        
    print("\n"+str(pos+1)+" Records inserted !")


def UpdateData():
    mycursor.execute("SELECT * FROM "+table)
    myresult = mycursor.fetchall()
    print("Data fetched from database !")
    
    if len(myresult[0]) - 1 == df.shape[1]: 
        
        if len(df.index) == len(myresult):
            
            total_updates = 0
            for i in range(len(myresult)):
                update_pos = []
                update_col = []
                latest_data = (df[df['CountryOther'] == myresult[i][1]].values).flatten()
                for j in range(df.shape[1]):
                    j2 = j+1
                    if str(myresult[i][j2]) != str(latest_data[j]):
                        #print(myresult[i])
                        #print(latest_data)
                        update_pos.append(j)
                        update_col.append(col[j][0])
                        #print( str(myresult[i][j2]) + " | " + str(latest_data[j]) )
                
                if len(update_pos) > 0 :
                    total_updates +=1
                    print("\n"+myresult[i][1]+"->"+str(update_col)+"\n")
                    
                    update_q = "UPDATE "+table+" SET "
                    
                    for p in range(len(update_pos)):
                        if p < len(update_pos)-1:
                            update_q += update_col[p]+"='"+str(latest_data[update_pos[p]]) +"', "
                        else:
                            update_q += update_col[p]+"='"+str(latest_data[update_pos[p]])+"' "
                    
                    update_q += " WHERE id = '"+str(myresult[i][0])+"'"
                    mycursor.execute(update_q)
                    mydb.commit()
                    
            if total_updates > 0 :
                print("Total rows updated : "+str(total_updates))
            else:
                print("Data up to date !")
            
        else:
            print("New country added !")
            DropAndCreateTable()
            InsertData()
    
    else:
        print("New Column added ! Drop and create Table and insert all data at once !")
        DropAndCreateTable()
        InsertData()



#DropAndCreateTable()
#InsertData()
        
UpdateData()


