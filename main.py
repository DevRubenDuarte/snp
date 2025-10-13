from build_tables import add_to_tbl_loci
from db_connection import execute_sql_file, get_connection

import pandas as pd

def main():
    # Load the TPED file
    tped_file = pd.read_csv("database/barbel_ex/test.tped", sep=" ", header=None)

    # # Call the function to create the SQL files
    # write_tbl_loci_sql(tped_file)
    # create_tbl_alleles_sql(tped_file, dog=1, source=1)
    # print("SQL file \"tbl_alleles.sql\" created successfully.")

    #execute_sql_file("tbl_loci.sql")
    add_to_tbl_loci(tped_file)

if __name__ == "__main__":
    main()