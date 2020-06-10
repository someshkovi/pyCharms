import pandas as pd
import os
import glob
import numpy as np
import datetime
from datetime import date
import psycopg2
from psycopg2 import Error



def main():
    dataU = pd.read_excel ('overview2020.xlsx', sheet_name='Sheet1')
    dataU.loc[:,'dateC'] = dataU.dateCalculated.apply(lambda c:c.date())

    try:
        connection = psycopg2.connect(user = "postgres",
                                      password = "iambatman",
                                      host = "hcsc-dps02.tspd.telangana.gov.in",
                                      port = "5432",
                                      database = "postgres")

        cur = connection.cursor()
        for index, row in dataU.iterrows():
            sqlStatement = """INSERT INTO public.overview_sla_overview_consolidated("dateCalculated", device_count, total_hours, maintenance_hours, unavailable_hours, exclusion_hours, value_without_exclusion, value_with_exclusion, sla_point_id) VALUES (%(dateC)s, %(Device Count)s, %(Total Hours)s, %(Maintenance Hours)s, %(Unavailable Hours)s, %(Exclusion Hours)s, %(Value Without Exclusions)s, %(Value With Exclusions)s, %(Point_id)s)"""
            cur.execute(sqlStatement, row.to_dict())
            connection.commit()
        print("commited successfully in PostgreSQL ")

    except (Exception, psycopg2.DatabaseError) as error :
        print ("Error while uploading to PostgreSQL table", error)
    finally:
        #closing database connection.
            if(connection):
                cur.close()
                connection.close()
                print("PostgreSQL connection is closed")

if __name__ == "__main__":
    main()
