import mysql.connector
my_db = mysql.connector.connect(
    host='localhost',
    user = 'root',
    password = 'root',
    port = '3306',
    database = 'tokyoolympic2020')

mycursor=my_db.cursor()

mycursor.execute('select * from athlets')
result = mycursor.fetchall()
for i in result:
    print(i)
