import psycopg2

def get_query_results(query):
    """Execute given query and return results"""

    db, cursor = connect_db()
    cursor.execute(query)
    results = cursor.fetchall()
    db.close()
    return results


def connect_db(name="news"):
    """Connect to the database and returns its connection"""
    try:
        db = psycopg2.connect("dbname={}".format(name))
        cursor = db.cursor()
        return db, cursor
    except:
        print "Error while connecting to the database"
        sys.exit(1)


def most_popular_articles():
    """Print the most popular three articles of all time"""

    query = "SELECT * FROM popular_articles LIMIT 3"
    results = get_query_results(query)

    print "\nWhat are the most popular three articles of all time?"
    for result in results:
        print "\"" + result[0] + "\" -- " + str(result[1]) + " views"


def most_popular_authors():
    """Print the most popular authors of all time"""

    query = "SELECT * FROM popular_authors"
    results = get_query_results(query)

    print "\nWho are the most popular article authors of all time?"
    for result in results:
        print "\"" + result[0] + "\" -- " + str(result[1]) + " views"


def error_requests():
    """Print on which days did more than 1% of requests lead to errors"""

    query = "SELECT * FROM requests_error_log WHERE error_percentage > 1"
    results = get_query_results(query)

    print "\nOn which days did more than 1% of requests lead to errors?"
    for result in results:
        print "\"" + str(result[0]) + "\" -- " + str(result[1]) + "% errors"

if __name__ == '__main__':
    most_popular_articles()
    most_popular_authors()
    error_requests()
