import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    """
    loading data to the staging tables from S3 buckets
    
    parameters: 
    cur: gets cursor to it
    conn: establish the connection with the cluster
    
    return: 
    none
    """
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    """
    inserting data to the dimension and fact tables from the staging tables
    
    parameters: 
    cur: gets cursor to it
    conn: establish the connection with the cluster
    
    return: 
    none
    """
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """
    - Establishes connection and gets cursor to it.  
    
    - loads the data.  
    
    - inserts the data. 
    
    - Finally, closes the connection. 
    """
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()