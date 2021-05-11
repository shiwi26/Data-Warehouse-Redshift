import configparser
import psycopg2
from sql_queries import testing_queries


def Num_of_rows(cur, conn):
    """
    getting the number of records for each table to test the ETL pipelines
    
    parameters: 
    cur: gets cursor to it
    conn: establish the connection with the cluster
    
    return: 
    Number of rows
    """
    for query in testing_queries:
        cur.execute(query)
        results = cur.fetchone()

        for row in results:
            print("Number of rows:", row)


def main():
    """
    - Establishes connection and gets cursor to it.   
    
    - gets the number of records for each table 
    
    - Finally, closes the connection. 
    """
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    Num_of_rows(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()