#!/usr/bin/env python3

import psycopg2

import pycodestyle

# 1. What are the most popular three articles of all time?

 query ="""select title, count(*) as views from articles, log
    where log.path like concat('%', articles.slug)
    and log.status like '%200%'
    group by articles.title
    order by views desc limit 3;
    """
    results = get_query(query)

    print("\nThe top 3 articles viewed are:")
    for i, r in enumerate(results):
        title = r[0]
        views = str(r[1]) + " views"
        print(str(i+1) + ": " + title + " -- " + views)

# Who are the most popular authors of all time?

authorsQuery = ''' select authors.name as Author, count(*) as Views
             from log,articles,authors
             where log.path=CONCAT('/article/',articles.slug)
             AND articles.author = authors.id
             Group By authors.name
             order by views DESC '''
# Place the articles query within a function whichc will later be called


def print_query2(query):
    results = get_query(query)
    print('\nQ2.Most Popular Authors:\n')
    for j in results:
        print ('\t' + str(j[0]) + ' - ' + str(j[1]) + ' views')
        
# 3. On which days did more than 1% of requests lead to errors?

errorQuery = ''' select date, (fail * 1.0/ total) * 100 FailurePercentage
                FROM (SELECT cast(time as date) date,
                count(*) as total, SUM(CASE status when '404 NOT FOUND'
                then 1 else 0 END) as fail
                FROM log group by date) as notfail
                where ((fail * 1.0/ total) * 1.0) * 100 > 1 '''
# Place the articles query within a function whichc will later be called


def print_query3(query):
    results = get_query(query)
    print('\nQ3.error days:\n')
    for k in results:
        print ('\t' + str(k[0]) + ' - ' + str(k[1]) + ' %')
# establish connection to makeshift psql instance


def get_query(query):
    '''Connect to news db'''
    db = psycopg2.connect("dbname=news")
    c = db.cursor()
    c.execute(query)
    return c.fetchall()
# print out results

print_query1(articlesQuery)
print_query2(authorsQuery)
print_query3(errorQuery)
