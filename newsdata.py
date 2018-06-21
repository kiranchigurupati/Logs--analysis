#!/usr/bin/env python3
import psycopg2
conn = psycopg2.connect(database="news", user="vagrant", password="vagrant")
cur = conn.cursor()
output1=(" create  view  view_article as select title,count(*) as likes from articles,log where  log.path like concat('%',articles.slug) group by articles.title,articles.author order by likes desc;")
try:
    print("What are the most popular three articles of all time?")
    qry ="select * from article_view limit 3;"
    cur.execute(qry)
    a = cur.fetchall()
    conn.commit()
    print("The top three popular articles are:")
    for result in a:
        print '\t', '', result[0], '', '-', result[1], " views"
except Exception as e:
    print(e)
output2 = (" create  view  authors_view as select name,count(*) as views from articles,authors,log where authors.id=articles.author and  log.path like concat('%',articles.slug) group by name order by views desc;")

try:
    print("Who are the most popular article authors of all time?")
    qry1 = "select * from authors_view;"
    cur.execute(qry1)
    b = cur.fetchall()
    print("The top three popular authors are:")
    for len in b:
        print '\t', '', len[0], '', ' - ', len[1], " views"
except Exception as e:
    print(e)

view1 = (" create view view_error as select date(time),count(*) as errors  from log where log.status like concat('404 NOT FOUND') group by date(time) order by errors desc;")
view2 = (" create view view_total as select date(time),count(status) as total_errors from log group by date(time)  order by total_errors desc;")
view3 = (" create view view_log as select view_total.date as date,((100.00*errors)/(total_errors)) as percentage_errors from view_error natural join view_total where view_error.date=view_total.date group by view_total.date,percentage_errors order by percentage_errors desc;")
output3 = ("select * from view_log where percentage_errors > 1;")
try:
    print("On which days did more than 1% of requests lead to errors?")
    cur.execute(output3)
    c = cur.fetchall()
    for exm in c:      
        print(str(exm[0].strftime('%B %d, %Y')) + " has " +
              str(round(exm[1],1)) + "% " + "errors")
except Exception as e:
    print(e)
cur.close()
conn.close()
