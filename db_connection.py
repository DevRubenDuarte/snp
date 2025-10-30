import os, logging
from typing import Optional

import psycopg
import dotenv
import pandas as pd

dotenv.load_dotenv()

def _get_logger() -> logging.Logger:
    logging.basicConfig(level=logging.INFO)
    return logging.getLogger(__name__)

def _get_env(key: str, default: Optional[str] = None) -> str:
    value = os.getenv(key, default)
    if value is None:
        raise RuntimeError(f"Missing required environment variable: {key}")
    return value

def _map_bases(df: pd.DataFrame) -> None:
    mapping = {
        "A": "1",
        "C": "2",
        "G": "3",
        "T": "4",
        "0": "0"
    }
    df.replace({"firstAllele": mapping, "secondAllele": mapping}, inplace=True)

def get_connection() -> psycopg.Connection:
    """
    Return new psycopg connection

    Required env vars:
        - PGHOST, PGPORT, PGDATABASE, PGUSER, PGPASSWORD
    """
    host = _get_env("PGHOST")
    port = int(_get_env("PGPORT"))
    dbname = _get_env("PGDATABASE")
    user = _get_env("PGUSER")
    password = _get_env("PGPASSWORD")

    conn = psycopg.connect(
        host=host,
        port=port,
        dbname=dbname,
        user=user,
        password=password,
        autocommit=True,
    )
    return conn

def add_to_tbl_loci(tped: pd.DataFrame) -> None:
    # Create loci DataFrame
    loci = pd.DataFrame(
        columns=["indexID","chromossome","locusID","distance","embark8","VHL","embark9","myDogDNA"]
    )
    # Create indexID as multiples of 8
    tped[6] = (tped.index + 1) * 8

    # Assign values to loci DataFrame from tped file
    loci = loci.assign(
                indexID = tped[6],
                chromossome = tped[0],
                locusID = tped[1],
                distance = tped[3],
                embark8 = True
            )

    # add tbl_loci
    logger = _get_logger()
    batch_size = 10000
    total_rows = len(loci)
    logger.info(f"Starting to insert {total_rows} rows into tbl_loci in batches of {batch_size}...")

    try:
        with get_connection() as conn:
            with conn.cursor() as cur:
                for start in range(0, total_rows, batch_size):
                    end = min(start + batch_size, total_rows)
                    batch = loci.iloc[start:end]
                    values = [
                        (row['indexID'], row['chromossome'], row['locusID'], row['distance'], row['embark8'], None, None, None)
                        for _, row in batch.iterrows()
                    ]
                    cur.executemany(
                        '''
                        INSERT INTO "public"."tbl_loci" (
                        "lngLocusID", "intChromosome", "strLocusIdentifier", "lngDistance", "blnEmbark8", "blnVHL", "blnEmbark9", "blnMyDogDNA")
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                        ''',
                        values
                    )
                    logger.info(f"Inserted rows {start + 1} to {end} into tbl_loci.")
    except Exception as e:
        logger.error(f"Error inserting into tbl_loci: {e}")
        raise
    logger.info("Loci added successfully.")

def add_to_tbl_alleles(tped: pd.DataFrame, dog: int, source: int) -> None:
    # Create alleles DataFrame
    alleles = pd.DataFrame(
        columns=["locusID","firstAllele","secondAllele","isHomozygous","sourceID", "dogID"]
    )
    # Assign values to alleles DataFrame from tped file
    alleles = alleles.assign(
                locusID = tped[1],
                firstAllele = tped[4],
                secondAllele = tped[5],
                isHomozygous = tped[4] == tped[5],
                sourceID = source,
                dogID = dog
            )
    # Map bases (a=1, c=2, g=3, t=4, 0=0)
    _map_bases(alleles)

    # add tbl_alleles
    logger = _get_logger()
    batch_size = 10000
    total_rows = len(alleles)
    logger.info(f"Starting to insert {total_rows} rows into tbl_alleles in batches of {batch_size}...")

    try:
        with get_connection() as conn:
            with conn.cursor() as cur:
                for start in range(0, total_rows, batch_size):
                    end = min(start + batch_size, total_rows)
                    batch = alleles.iloc[start:end]
                    values = [
                        (row['dogID'], row['locusID'], row['firstAllele'], row['secondAllele'], row['isHomozygous'], row['sourceID'])
                        for _, row in batch.iterrows()
                    ]
                    cur.executemany(
                        '''
                        INSERT INTO "public"."tbl_alleles" (
                        "lngDogID", "strLocusID", "bytFirstAllele", "bytSecondAllele", "blnIsHomozygous", "lngSourceID")
                        VALUES (%s, %s, %s, %s, %s, %s)
                        ''',
                        values
                    )
                    logger.info(f"Inserted rows {start + 1} to {end} into tbl_alleles.")
    except Exception as e:
        logger.error(f"Error inserting into tbl_alleles: {e}")
        raise
    logger.info("Alleles added successfully.")