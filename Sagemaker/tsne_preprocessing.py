
import subprocess
import sys

def install(package):
    subprocess.check_call([sys.executable, "-m", "pip", "install", package])
    
print("install snowflake connector")
install("snowflake-connector-python[pandas]")
print("installed snowflake connector")

import argparse
import os
import warnings

import pandas as pd
import numpy as np
from sklearn.preprocessing import StandardScaler
from sklearn.manifold import TSNE

import snowflake.connector
from snowflake.connector.pandas_tools import pd_writer

def prep_data(df):
    numeric_cols = df.columns[(df.dtypes=='float64') | (df.dtypes=='int64')].values.tolist()
    df_numeric = df[numeric_cols]
    df_numeric = df_numeric[['TOTALPOPULATION', 'TOTALCOMUTERS',
                        'TOTALPOPOVER25', "BACHELORS_DEGREE",
                        'MEDIANHOUSEHOLDINCOME2019INFLATIONADJUSTED',
                        'TOTALPOPINLABORFORCEUNEMPLOYED', 'TOTALHOUSINGUNITS',
                        'MEDIANHOUSEVALUE']]
    X = df_numeric
    print("created data for TSNE")
    return X

    
def run_tsne(X):
    scaler = StandardScaler()
    X_scaled = scaler.fit_transform(X)
    X_tsne = TSNE(n_components=2, init='random').fit_transform(X)
    print("Ran TSNE")
    return X_tsne


def prep_dataframe(df, X_tsne):
    df_export = df[['CITY_COUNTY_STATE']]
    df_export['tsne_d1'] = X_tsne[:,0]
    df_export['tsne_d2'] = X_tsne[:,1]
    df_export['CITY'] = df_export['CITY_COUNTY_STATE'].str.split('-').apply(lambda x: x[0])
    df_export['COUNTY'] = df_export['CITY_COUNTY_STATE'].str.split('-').apply(lambda x: x[1])
    df_export['STATE'] = df_export['CITY_COUNTY_STATE'].str.split('-').apply(lambda x: x[-1])
    df_export = df_export.drop('CITY_COUNTY_STATE', axis=1)
    print("Prepped data for export")
    return df_export
    
    
if __name__ == "__main__":
    
    # For reading from Snowflake
    conn = snowflake.connector.connect(user='',
                                       password="",
                                       account="",
                                       warehouse='',
                                       database= "",
                                       schema="")
    
    # cursor
    cs = conn.cursor()
    sql = "select * from US_DEMOGRAPHICS;"
    cs.execute(sql)
    census_df = cs.fetch_pandas_all()
    
    # Close connections
    cs.close()
    conn.close()
    
    # Process data
    X = prep_data(census_df)
    X_tsne = run_tsne(X)
    df_export = prep_dataframe(census_df, X_tsne)

    # Create output paths
    census_tsne_output_path = os.path.join("/opt/ml/processing/censustsne", "censustsne.csv")
    
    
    print("Saving Train Data {}".format(census_tsne_output_path))
    df_export.to_csv(census_tsne_output_path, header=True, index=False)
    
