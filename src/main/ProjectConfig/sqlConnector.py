import mysql.connector
my_db = mysql.connector.connect(
    host='localhost',
    user = 'root',
    password = 'root',
    port = '3306',
    database = 'tokyoolympic2020')

mycursor=my_db.cursor()

# 1. Checking number of athletes from each country

mycursor.execute('''
select country, count('name') as 'TotalCountOfAthletes' from athlets 
group by country
order by TotalCountOfAthletes desc;
''')
TotalCOE_result = mycursor.fetchall()
for i in TotalCOE_result:
    print(i)


# 2. Number of medals won by each countries.

mycursor.execute('''
select Country , sum(medal_code) as 'totalmedalBYCOE' from
medals 
group by Country
order by totalmedalBYCOE DESC;
''')
eachcoeMedals = mycursor.fetchall()
print(eachcoeMedals)



# 3

