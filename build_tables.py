import pandas as pd

from db_connection import get_connection

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
    try:
        with get_connection() as conn:
            with conn.cursor() as cur:
                for _, row in loci.iterrows():
                    cur.execute(
                        '''
                        INSERT INTO "public"."tbl_loci" (
                        "lngLocusID", "intChromosome", "strLocusIdentifier", "lngDistance", "blnEmbark8", "blnVHL", "blnEmbark9", "blnMyDogDNA")
                        VALUES (%s, %s, %s, %s, %s, NULL, NULL, NULL)
                        ''',
                        (row['indexID'], row['chromossome'], row['locusID'], row['distance'], row['embark8'])
                    )
    except Exception as e:
        print(f"Error inserting into tbl_loci: {e}")
        raise
    print("Loci added successfully.")

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
    _map_bases(alleles)

    # add tbl_alleles
    try:
        with get_connection() as conn:
            with conn.cursor() as cur:
                for _, row in alleles.iterrows():
                    cur.execute(
                        '''
                        INSERT INTO "public"."tbl_alleles" (
                        "lngDogID", "strLocusID", "bytFirstAllele", "bytSecondAllele", "blnIsHomozygous", "lngSourceID")
                        VALUES (%s, %s, %s, %s, %s, %s)
                        ''',
                        (row['dogID'], row['locusID'], row['firstAllele'], row['secondAllele'], row['isHomozygous'], row['sourceID'])
                    )
    except Exception as e:
        print(f"Error inserting into tbl_alleles: {e}")
        raise
    print("Alleles added successfully.")

def _map_bases(df: pd.DataFrame) -> None:
    mapping = {
        "A": "1",
        "C": "2",
        "G": "3",
        "T": "4",
        "0": "0",
    }
    df.replace({"firstAllele": mapping, "secondAllele": mapping}, inplace=True)