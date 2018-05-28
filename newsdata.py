#! /usr/bin/env python

import psycopg2

DBNAME = "news"


def kiran(query):
    """Connects to the database, runs the query passed to it,
    and returns the results"""
    db = psycopg2.connect('dbname=' + DBNAME)
    a = db.cursor()
    a.execute(query)
    rows = a.fetchall()
    db.close()
    return rows


def articles_list():
    """Returns top 3 read articles"""

    # Build Query String
    k = """
        SELECT articles.title, COUNT(*) AS num
        FROM articles
        JOIN log
        ON log.path LIKE concat('/article/%', articles.slug)
        GROUP BY articles.title
        ORDER BY num DESC
        LIMIT 3;
    """

    # Run Query
    results = kiran(k)

    # Print Results
    print('\n The Popular Articles Of All Time:')
    count = 1
    for result in results:
        v = '(' + str(count) + ') "'
        i = result[0]
        d = '" with ' + str(result[1]) + " views"
        print(v + i + d)
        count += 1


def authors_list():
    """returns top 3 authors"""

    # Build Query String
    k = """
        SELECT authors.name, COUNT(*) AS num
        FROM authors
        JOIN articles
        ON authors.id = articles.author
        JOIN log
        ON log.path like concat('/article/%', articles.slug)
        GROUP BY authors.name
        ORDER BY num DESC
        LIMIT 3;
    """

    # Run Query
    results = kiran(k)

    # Print Results
    print('\n The Popular Authors Of All Time:')
    count = 1
    for result in results:
            print('(' + str(count) + ') ' + result[0] +
                  ' with ' + str(result[1]) + " views")
            count += 1


def log_list():
    """returns days with more than 1% errors"""

    # Build Query String
    k = """
        SELECT total.day,
          ROUND(((errors.error_requests*1.0) / total.requests), 3) AS percent
        FROM (
          SELECT date_trunc('day', time) "day", count(*) AS error_requests
          FROM log
          WHERE status LIKE '404%'
          GROUP BY day
        ) AS errors
        JOIN (
          SELECT date_trunc('day', time) "day", count(*) AS requests
          FROM log
          GROUP BY day
          ) AS total
        ON total.day = errors.day
        WHERE (ROUND(((errors.error_requests*1.0) / total.requests), 3) > 0.01)
        ORDER BY percent DESC;
    """

    # Run Query
    results = kiran(k)

    # Print Results
    print('\n LOG VALUES WITH MORE THAN 1% ERRORS:')
    for result in results:
        date = result[0].strftime('%B %d, %Y')
        errors = str(round(result[1]*100, 1)) + "%" + " errors"
        print(date + " -- " + errors)

print('Listing Out The Results...\n')
articles_list()
authors_list()
log_list()
