from db_connection import add_to_tbl_loci, add_to_tbl_alleles

import pandas as pd

def main():
    # Load the TPED file
    tped_file = pd.read_csv("database/barbel_ex/test.tped", sep=" ", header=None)
    # Create tables
    add_to_tbl_loci(tped_file)
    add_to_tbl_alleles(tped_file, dog=int(1), source=1)

if __name__ == "__main__":
    main()