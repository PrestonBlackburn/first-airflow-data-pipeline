import pymysql
import pandas as pd
import configparser

if __name__ == "__main__":

    df_key = pd.read_csv('data headers key.csv')

    headers = df_key['Code'].values
    header_data_type = df_key['DataType'].values
    print(headers)

    print(headers[0] + " " + header_data_type[0])

    table_columns = []
    for i in range(0, len(headers)):
        table_column = headers[i] + " " + header_data_type[i]
        table_columns.append(table_column)

    table_columns_list = ', '.join(table_columns)


    # create table
    parser = configparser.ConfigParser()
    parser.read("pipeline.conf")

    hostname = parser.get("mysql_config", "hostname")
    port = parser.get("mysql_config", "port")
    username = parser.get("mysql_config", "username")
    dbname = parser.get("mysql_config", "database")
    password = parser.get("mysql_config", "password")

    conn = pymysql.connect(host=hostname,
                user=username,
                password=password,
                db=dbname,
                port=int(port),
                autocommit=True)


    if conn is None:
        print("Error connecting to aurora db")
    else:
        print("aurora db connection established")
        create_table_query = f"CREATE TABLE US_DEMOGRAPHICS ({table_columns_list});"
        #drop_table = "DROP TABLE US_DEMOGRAPHICS"

    m_cursor = conn.cursor()
    #m_cursor.execute(drop_table)
    m_cursor.execute(create_table_query)
    conn.commit()



