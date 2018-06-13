#!/usr/bin/env python3
import psycopg2

DBNAME = "news"

run_query1 = "What are the most popular three articles of all time?\n"
run_query2 = "Who are the most popular article authors of all time?\n"
run_query3 = "On which days did more than 1% of requests lead to errors?\n"

run_query1_ans = ("SELECT title, count(*) FROM articles JOIN log ",
                  "ON log.path LIKE concat('%',articles.slug,'%')",
                  "GROUP BY title, path ORDER BY count(*) DESC limit 3;")
run_query2_ans = ("SELECT name, count(*) FROM authors JOIN articles ON ",
                  "articles.author = authors.id JOIN log ON log.path ",
                  "LIKE concat('%', articles.slug, '%') GROUP BY name,",
                  "log.path ORDER BY count(*) DESC limit 3;")
run_query3_ans = ("SELECT date, perc FROM err_perc GROUP BY date,",
                  "perc HAVING perc >= 1 ORDER BY perc;")


def tag_query(query):
    try:
        db = psycopg2.connect(database=DBNAME)
        c = db.cursor()
        c.execute(query)
        q = c.fetchall()
        db.close()
        for row in q:
            print row
        print "\n"
    except:
        print "connect to database."

if __name__ == '__main__':
    print run_query1
    tag_query(run_query1_ans)

    print run_query2
    tag_query(run_query2_ans)

    print run_query3
    tag_query(run_query3_ans)
