from build_tables import create_tbl_loci_sql, create_tbl_alleles_sql
import pandas as pd

def main():
    # Load the TPED file
    tped_file = pd.read_csv("~/dogs_global/database/barbel_ex/barbel.tped", sep=" ", header=None)

    # Call the function to create the SQL files
    create_tbl_loci_sql(tped_file)
    print("SQL file 'tbl_loci.sql' created successfully.")
    create_tbl_alleles_sql(tped_file, dog=1, source=1)
    print("SQL file 'tbl_alleles.sql' created successfully.")

if __name__ == "__main__":
    main()