from db_connection import add_to_tbl_loci, add_to_tbl_alleles
from zip_file_handler import unzip_file

import pandas as pd
import os

def main() -> None:
    process_zip("uploads/289_223766.zip")

def process_zip(file_path: str) -> None:
    # Unzip the file
    path, contents = unzip_file(file_path)

    # Load the TPED file
    file_name = next((name for name in contents.keys() if name.endswith(".tped")), None)
    if file_name is None:
        raise FileNotFoundError("No .tped file found in the zip archive.")
    tped_file_path = os.path.join(path, file_name)
    tped_file = pd.read_csv(tped_file_path, sep=" ", header=None)

    # Create tables
    add_to_tbl_loci(tped_file)
    # add_to_tbl_alleles(tped_file, dog=int(1), source=1)

if __name__ == "__main__":
    main()