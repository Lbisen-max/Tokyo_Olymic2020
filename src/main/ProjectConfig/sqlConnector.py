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


# 3. No of Medals won by India

mycursor.execute('''
select * 
from medals
where country in ('India');
''')
totalMedalsofIndia = mycursor.fetchall()
print(totalMedalsofIndia)


# 4. Top 5 countries with highest number of coaches

mycursor.execute('''
SELECT Country_code , count(name) as 'TotalCoaches'
from coachs
group by Country_code
order by TotalCoaches desc
Limit 5;
''')
TophighestnumberofCoaches = mycursor.fetchall()
print(TophighestnumberofCoaches)


# 6. Top 10  country with gold medals

mycursor.execute('''
select Country , Gold_Medal
from totalmedals
order by Gold_Medal desc
limit 10;
''')
top10GoldwinnerCOE = mycursor.fetchall()
print(top10GoldwinnerCOE)

