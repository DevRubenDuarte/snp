import os
import logging
from typing import Optional
from pathlib import Path
from snp_utils import unzip_file


import psycopg
import dotenv
import polars as pl

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
        "A": 1,
        "C": 2,
        "G": 3,
        "T": 4,
    }

    # Replace values in firstAllele and secondAllele columns and return new DataFrame
    return df.with_columns([
        pl.col("firstAllele").replace_strict(mapping, default=0).cast(pl.Int8),
        pl.col("secondAllele").replace_strict(mapping, default=0).cast(pl.Int8)
    ])

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

def add_to_tbl_snp_loci(tped: pl.DataFrame) -> None:
    """
    Add loci from a TPED dataframe to the tbl_snp_loci database table.

    This function processes a TPED (transposed PED) format dataframe, extracts locus
    information, and inserts it into the tbl_snp_loci table in batches. Each locus is assigned
    a unique indexID as multiples of 8, and additional boolean flags are initialized with
    default values (embark8=True, VHL=None, embark9=None, myDogDNA=None).

        tped (pl.DataFrame): A Polars DataFrame representing the TPED file with columns:
            - indexID: Unique identifier for the locus (generated as multiples of 8)
            - chromosome: Chromosome identifier
            - locusID: Locus identifier
            - distance: Genetic distance
            - Additional columns for SNP panel membership flags

    Returns:
        None

    Raises:
        Exception: If any error occurs during database insertion, the exception is logged
            and re-raised. Partial inserts may have occurred before the exception.

    Note:
        - Insertions are performed in batches of 10,000 rows to optimize performance
        - All processing and errors are logged using the configured logger
        - Requires an active database connection via get_connection()
    """

    # Create indexID as multiples of 8
    tped = tped.with_columns(
        (pl.arange(1, tped.height + 1) * 8).alias("indexID")
    )

    # Assign values to loci DataFrame from tped file
    loci = tped.select([
        pl.col("indexID"),
        pl.col("chromosome"),
        pl.col("locusID"),
        pl.col("distance"),
        pl.lit(True).alias("embark8"),
        pl.lit(None).alias("VHL"),
        pl.lit(None).alias("embark9"),
        pl.lit(None).alias("myDogDNA")
    ])

    # add tbl_snp_loci
    logger = _get_logger()
    batch_size = 10000
    total_rows = loci.height
    logger.info(f"Starting to insert {total_rows} rows into tbl_snp_loci in batches of {batch_size}...")

    try:
        with get_connection() as conn:
            with conn.cursor() as cur:
                # Check if table exists, if not create it
                cur.execute(
                    """
                    SELECT EXISTS (
                        SELECT FROM information_schema.tables
                        WHERE table_name = 'tbl_snp_loci'
                    );
                    """
                )
                row = cur.fetchone()
                tbl_exists = bool(row[0]) if row else False
                if not tbl_exists:
                    cur.execute(
                        '''
                        CREATE TABLE "public"."tbl_snp_loci" (
                            "lngIndexID" int8 NOT NULL PRIMARY KEY,
                            "intChromosome" int2 NOT NULL,
                            "strLocusID" varchar(255) NOT NULL,
                            "lngDistance" int8 NOT NULL,
                            "blnEmbark8" bool,
                            "blnVHL" bool,
                            "blnEmbark9" bool,
                            "blnMyDogDNA" bool
                        );
                        '''
                    )
                    logger.info("Created table tbl_snp_loci.")

                for start in range(0, total_rows, batch_size):
                    end = min(start + batch_size, total_rows)
                    batch = loci.slice(start, end - start)
                    values = batch.rows()

                    cur.executemany(
                        '''
                        INSERT INTO "public"."tbl_snp_loci" (
                            "lngIndexID", "intChromosome", "strLocusID", "lngDistance",
                            "blnEmbark8", "blnVHL", "blnEmbark9", "blnMyDogDNA")
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT ("lngIndexID") DO UPDATE SET
                            "intChromosome" = EXCLUDED."intChromosome",
                            "strLocusID" = EXCLUDED."strLocusID",
                            "lngDistance" = EXCLUDED."lngDistance",
                            "blnEmbark8" = EXCLUDED."blnEmbark8",
                            "blnVHL" = EXCLUDED."blnVHL",
                            "blnEmbark9" = EXCLUDED."blnEmbark9",
                            "blnMyDogDNA" = EXCLUDED."blnMyDogDNA"
                        ''',
                        values
                    )
                    logger.info(f"Inserted rows {start + 1} to {end} into tbl_snp_loci.")
    except Exception as e:
        logger.error(f"Error inserting into tbl_snp_loci: {e}")
        raise

    logger.info("Loci added successfully.")

def add_to_tbl_snp_alleles(tped: pl.DataFrame, dog: int, source: str) -> None:
    """
    Takes a tped dataframe and adds its alleles to tbl_snp_alleles.

    Args:
        tped (pl.DataFrame): DataFrame representing the tped file.
        dog (int): Dog ID.
        source (str): Source ID.
    """

    # Create alleles DataFrame with Polars
    alleles = tped.select([
        pl.lit(dog).alias("dogID"),
        pl.col("locusID"),
        pl.col("firstAllele"),
        pl.col("secondAllele"),
        (pl.col("firstAllele") == pl.col("secondAllele")).alias("isHomozygous"),
        pl.lit(source).alias("source")
    ])

    # Map bases (A=1, C=2, G=3, T=4, 0=0)
    alleles = _map_bases(alleles)

    # Add tbl_snp_alleles
    logger = _get_logger()
    batch_size = 10000
    total_rows = alleles.height
    logger.info(f"Starting to insert {total_rows} rows into tbl_snp_alleles in batches of {batch_size}...")

    try:
        with get_connection() as conn:
            with conn.cursor() as cur:
                # Check if table exists, if not create it
                cur.execute(
                    """
                    SELECT EXISTS (
                        SELECT FROM information_schema.tables
                        WHERE table_name = 'tbl_snp_alleles'
                    );
                    """
                )
                row = cur.fetchone()
                tbl_exists = bool(row[0]) if row else False
                if not tbl_exists:
                    cur.execute(
                        '''
                        CREATE TABLE "public"."tbl_snp_alleles" (
                            "lngDogID" int8 NOT NULL,
                            "strLocusID" varchar(255) NOT NULL,
                            "bytFirstAllele" int8 NOT NULL,
                            "bytSecondAllele" int8 NOT NULL,
                            "blnIsHomozygous" bool NOT NULL,
                            "lngSourceID" varchar(255) NOT NULL,
                            PRIMARY KEY ("lngDogID", "strLocusID")
                        );
                        '''
                    )
                    logger.info("Created table tbl_snp_alleles.")

                for start in range(0, total_rows, batch_size):
                    end = min(start + batch_size, total_rows)
                    # Use slice instead of iloc
                    batch = alleles.slice(start, end - start)
                    # Use rows() instead of iterrows()
                    values = batch.rows()

                    cur.executemany(
                        '''
                        INSERT INTO "public"."tbl_snp_alleles" (
                        "lngDogID", "strLocusID", "bytFirstAllele", "bytSecondAllele", "blnIsHomozygous", "lngSourceID")
                        VALUES (%s, %s, %s, %s, %s, %s)
                        ''',
                        values
                    )
                    logger.info(f"Inserted rows {start + 1} to {end} into tbl_snp_alleles.")
    except Exception as e:
        logger.error(f"Error inserting into tbl_snp_alleles: {e}")
        raise

    logger.info("Alleles added successfully.")