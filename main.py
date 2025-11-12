from plink_integration import plink_roh, plink_parentage
from zip_file_handler import unzip_file

import os

def main() -> None:
    # Create tables
    # add_to_tbl_loci(process_zip("uploads/289_223766.zip"))
    # add_to_tbl_alleles(tped_file, dog=int(1), source=1)

    # Run Runs of Homozygosity (ROH) analysis
    # plink_roh(input_file="uploads/289_223766_unzipped/31220610301956", output_folder="roh/31220610301956", plink_path="/home/artos/dogs_global/database/plink/plink")

    # Run Parentage analysis
    offspring_folder, offspring_contents = unzip_file("uploads/289_105581.zip")
    offspring_path = offspring_folder + "/" + list(offspring_contents.keys())[0].rsplit(".", 1)[0]
    parent1_folder, parent1_contents = unzip_file("uploads/289_208989.zip")
    parent1_path = parent1_folder + "/" + list(parent1_contents.keys())[0].rsplit(".", 1)[0]
    parent2_folder, parent2_contents = unzip_file("uploads/289_235631.zip")
    parent2_path = parent2_folder + "/" + list(parent2_contents.keys())[0].rsplit(".", 1)[0]

    plink_parentage(
        offspring_file=offspring_path,
        parent1_file=parent1_path,
        parent2_file=parent2_path,
        genome_file="ibd/" + os.path.basename(offspring_path)
    )


if __name__ == "__main__":
    main()