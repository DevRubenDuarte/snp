from multiprocessing import parent_process
from db_connection import execute_rows
import pandas as pd

tped_file = pd.read_csv("test.tped", sep=" ", header=None)


def create_tbl_loci_sql(tped) -> None:
    loci = pd.DataFrame(
        columns=["indexID","chromossome","locusID","distance","embark8","VHL","embark9","myDogDNA"]
    )
    
    tped[len(tped.columns)] = (tped.index + 1) * 8

    loci = loci.assign(
                indexID=tped[6],
                chromossome=tped[0],
                locusID=tped[1],
                distance=tped[3]
            )
