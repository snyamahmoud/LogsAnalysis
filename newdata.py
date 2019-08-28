import psycopg2

db = psycopg2.connect("dbname=news")
c = db.cursor()

c.execute("select articles.title, count(log.path) as views from articles left "
          "join log on log.path like '%' || articles.slug where log.status"
          " like '%OK' and path like '/article%' group by articles.title,"
          " log.path order by count(log.path) desc limit 3;")
          
articles = c.fetchall()
print("\nMost popular three articles of all time:")
for article in articles:
    print(article[0], " - ", article[1])

c.execute("select author_name.name, count(log.path) as views from author_name "
          "left join log on log.path like '%' || author_name.slug where"
          " log.status like '%OK' and log.path like '/article%' group by "
          "author_name.name order by count(log.path) desc;")
          
authors = c.fetchall()
print("\nMost popular article authors of all time:")
for author in authors:
    print(author[0], " - ", author[1], " views")

c.execute("select to_char(date,'Month DD, YYYY'), percentage from"
          " (select error_count.date, round(((float8(error_count.count)/float8"
          "(req_count.count))*100)::numeric,2) as percentage from error_count "
          "join req_count on error_count.date=req_count.date) as highest"
          " where percentage>1;")
          
errors = c.fetchall()
print("\nDays when more than 1%  of the requests lead to errors:")
for error in errors:
    print(error[0], " - ", error[1], "%")
