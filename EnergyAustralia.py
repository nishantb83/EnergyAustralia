import  csv
import  json
import  pandas  as pd #import the pandas library
from    pandasql import sqldf
import  sqlite3
pysqldf = lambda q: sqldf(q, globals())


def csv_to_json(csvFilePath, jsonFilePath,counter):
    jsonArray = []

    # read csv file
    with open(csvFilePath, encoding='utf-8') as csvf:
        # load csv file data using csv library's dictionary reader
        csvReader = csv.DictReader(csvf)

        # convert each csv row into python dict
        for row in csvReader:
            # add this python dict to json array
            jsonArray.append(row)

    # convert python jsonArray to JSON String and write to file
    with open(jsonFilePath, 'w', encoding='utf-8') as jsonf:
        jsonString = json.dumps(jsonArray, indent=4)
        jsonf.write(jsonString)


# Loading the CSV file to a dataframe
df = pd.read_csv('Transaction.csv')
df.columns = ['Account_ID','CODE','Implemented_Date','Active_Indicator','Account_Type','Service', 'BU','Request_Date','Account_status','Status Code','Amount','Version','AgentID','FIBRE','last_Updated_Date',
              'Property_Type','Post_Code','col_18','col_19']


# List post codes based on fastest response
q1= """ select  Post_Code,  (Implemented_Date - Request_Date) AS 'Response time' from df group by 1 order by 2 asc ;"""
print(pysqldf(q1))


# Top Agents based on Post code and Amount
q2= """ select  AgentID,Post_Code, sum(Amount) as "Amount Collected"  from df group by 1,2 order by 3 desc;"""
print(pysqldf(q2))


#Variables assignments
csvFilePath = r'C:\Users\nisha\PycharmProjects\pythonProject\Transaction.csv'
jsonFilePath = r'C:\Users\nisha\PycharmProjects\pythonProject\Transaction.json'
counter =1

#Calling the Main Functiom
csv_to_json(csvFilePath, jsonFilePath,counter)

