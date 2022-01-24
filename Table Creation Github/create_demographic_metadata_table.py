import pymysql
import configparser


if __name__ == "__main__":

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
        create_table_query = "CREATE TABLE US_DEMOGRAPHICS_COLUMNS ( \
            Code varchar(250), \
            Description varchar(250), \
            DataType varchar(250) \
        );"

        #drop_table = "DROP TABLE US_DEMOGRAPHICS_COLUMNNS"

    m_cursor = conn.cursor()
    #m_cursor.execute(drop_table)
    m_cursor.execute(create_table_query)
    conn.commit()

