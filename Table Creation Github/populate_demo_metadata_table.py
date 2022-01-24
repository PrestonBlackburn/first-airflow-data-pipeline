import pymysql
import pandas as pd
import chardet
import configparser

if __name__ == "__main__":

    # check data encoding
    df = pd.read_csv('data headers key.csv')

    headers = ['Code', 'Description', 'DataType']
    table_columns_list = ', '.join(headers)

    # Add Rows
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
        m_cursor = conn.cursor()
        
        #probably a better way to do this
        tuple_list = []
        for i in range(0, len(df)):
            df_row_vals = df.iloc[i].values
            df_row_vals = df_row_vals.tolist()
            # separate by dash so we can split into columns later
            df_row_vals = [x.replace(',','-') if type(x) == str else x for x in df_row_vals]
            single_row_values = tuple(map(str, df_row_vals))
            tuple_list.append(single_row_values)

        s_pct_list = ', '.join(['%s'] * len(headers))

        create_row = f"""INSERT INTO US_DEMOGRAPHICS_COLUMNS \
                            VALUES ({s_pct_list});"""

        print(create_row)
        m_cursor.executemany(create_row, tuple_list)
        conn.commit()