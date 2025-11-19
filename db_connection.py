import os
import logging
from typing import Optional
from zip_file_handler import unzip_file

import psycopg
import dotenv
import polars as pl
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

def _map_bases(df: pl.DataFrame) -> pl.DataFrame:
    """
    Map DNA bases to numeric values: A=1, C=2, G=3, T=4, 0=0
    """

    mapping = {
        "A": "1",
        "C": "2",
        "G": "3",
        "T": "4",
        "0": "0"
    }
    # Replace values in firstAllele and secondAllele columns and return new DataFrame
    return df.with_columns([
        pl.col("firstAllele").replace(mapping).cast(pl.Int8),
        pl.col("secondAllele").replace(mapping).cast(pl.Int8)
    ])

def _check_file_is_valid(file_path: Path) -> bool:
    tped = pd.read_csv(file_path, sep="\\s+", header=None)
    if tped.shape[1] < 3:
        raise ValueError("TPED file must have at least 3 columns.")
    elif tped.shape[0] < 3:
        raise ValueError("TPED file must have at least 3 rows.")
    return True

def process_zip(file_path: str) -> pl.DataFrame:
    # Unzip the file
    path, contents = unzip_file(file_path)

    # Load the TPED file
    file_name = next((name for name in contents.keys() if name.endswith(".tped")), None)
    if file_name is None:
        raise FileNotFoundError("No .tped file found in the zip archive.")
    tped_file_path = os.path.join(path, file_name)
    return pl.read_csv(tped_file_path, separator="\t", has_header=False)

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

def add_to_tbl_loci(tped: pl.DataFrame) -> None:
    """
    Takes a tped dataframe and adds its loci to tbl_loci.

    Args:
        tped (pl.DataFrame): DataFrame representing the tped file.
    """

    # Create loci DataFrame
    loci = pl.DataFrame(
        schema=["indexID","chromossome","locusID","distance","embark8","VHL","embark9","myDogDNA"]
    )

    # Create indexID as multiples of 8
    # tped[6] = (tped.index + 1) * 8
    tped = tped.with_columns(
        (pl.arange(1, tped.height + 1) * 8).alias("indexID")
    )

    # Assign values to loci DataFrame from tped file
    loci = tped.select([
        pl.col("indexID"),
        pl.col("chromossome"),
        pl.col("locusID"),
        pl.col("distance"),
        pl.lit(True).alias("embark8"),
        pl.lit(None).alias("VHL"),
        pl.lit(None).alias("embark9"),
        pl.lit(None).alias("myDogDNA")
    ])

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
                    batch = loci.slice(start, end - start)
                    values = batch.rows()

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

def add_to_tbl_alleles(tped: pl.DataFrame, dog: int, source: int) -> None:
    """
    Takes a tped dataframe and adds its alleles to tbl_alleles.

    Args:
        tped (pl.DataFrame): DataFrame representing the tped file.
        dog (int): Dog ID.
        source (int): Source ID.
    """

    # Create alleles DataFrame with Polars
    alleles = tped.select([
        pl.col("locusID"),
        pl.col("firstAllele"),
        pl.col("secondAllele"),
        (pl.col("firstAllele") == pl.col("secondAllele")).alias("isHomozygous"),
        pl.lit(source).alias("sourceID"),
        pl.lit(dog).alias("dogID")
    ])

    # Map bases (A=1, C=2, G=3, T=4, 0=0)
    alleles = _map_bases(alleles)

    # Add tbl_alleles
    logger = _get_logger()
    batch_size = 10000
    total_rows = alleles.height
    logger.info(f"Starting to insert {total_rows} rows into tbl_alleles in batches of {batch_size}...")

    try:
        with get_connection() as conn:
            with conn.cursor() as cur:
                for start in range(0, total_rows, batch_size):
                    end = min(start + batch_size, total_rows)
                    # Use slice instead of iloc
                    batch = alleles.slice(start, end - start)
                    # Use rows() instead of iterrows()
                    values = batch.rows()

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

def send_tped_to_bucket(file_path: Path) -> None:
    """
    Sends a tped file to the S3 bucket.

    Args:
        file_path (Path): Path to the tped file.
    """
    if _check_file_is_valid(file_path):
        # TODO insert file into S3 bucket
        pass
