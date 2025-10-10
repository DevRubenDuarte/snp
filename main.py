import os
from build_tables import create_tbl_loci_sql, create_tbl_alleles_sql
from db_connection import execute_sql_file, get_connection
import pandas as pd

def main():
    # Load the TPED file
    tped_file = pd.read_csv("31220612410335.tped", sep=" ", header=None)

    # Call the function to create the SQL files
    #create_tbl_loci_sql(tped_file)
    #print("SQL file "tbl_loci.sql" created successfully.")
    #create_tbl_alleles_sql(tped_file, dog=1, source=1)
    #print("SQL file "tbl_alleles.sql" created successfully.")

    #execute_sql_file("tbl_loci.sql")
    #! locusIdentifier is not taking str
    sql = 'INSERT INTO "public"."tbl_loci" ("lngLocusID", "intChromosome", "strLocusIdentifier", "lngDistance", "blnEmbark8", "blnVHL", "blnEmbark9", "blnMyDogDNA") VALUES (8, 1, 1, 68723, True, NULL, NULL, NULL);' 

    with get_connection() as conn:
        with conn.cursor() as cur:
            # psycopg can execute multiple statements in one execute when autocommit=True
            cur.execute(sql)

if __name__ == "__main__":
    main()