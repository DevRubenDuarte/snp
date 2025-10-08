import pandas as pd

def create_tbl_loci_sql(tped) -> None:
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

    # Write SQL file
    with open("tbl_loci.sql", "w") as file:
        file.write(
            'CREATE TABLE "public"."tbl_loci" (\n'
            '   "lngLocusID" int8 NOT NULL,\n'
            '   "intChromosome" int2 NOT NULL,\n'
            '   "strLocusIdentifier" varchar(100) NOT NULL PRIMARY KEY,\n'
            '   "lngDistance" int8 NOT NULL,\n'
            '   "blnEmbark8" bool,\n'
            '   "blnVHL" bool,\n'
            '   "blnEmbark9" bool,\n'
            '   "blnMyDogDNA" bool\n'
            ');\n'
            '\n'
            'INSERT INTO "public"."tbl_loci" (\n'
            '   "lngLocusID", "intChromosome", "strLocusIdentifier", "lngDistance", "blnEmbark8", "blnVHL", "blnEmbark9", "blnMyDogDNA") VALUES\n'
        )
        for _, row in loci.iterrows():
            file.write(
                f"({row['indexID']}, {row['chromossome']}, '{row['locusID']}', {row['distance']}, {row['embark8']}, NULL, NULL, NULL),\n"
            )

def create_tbl_alleles_sql(tped, dog, source) -> None:
    # Create alleles DataFrame
    alleles = pd.DataFrame(
        columns=["dogID","locusID","firstAllele","secondAllele","isHomozygous","sourceID"]
    )

    # Assign values to alleles DataFrame from tped file
    alleles = alleles.assign(
                dogID = dog,
                locusID = tped[1],
                firstAllele = tped[4],
                secondAllele = tped[5],
                isHomozygous = tped[4] == tped[5],
                sourceID = source
            )
    map_bases(alleles)

    # Write SQL file
    with open("tbl_alleles.sql", "w") as file:
        file.write(
            'CREATE TABLE "public"."tbl_alleles" (\n'
            '   "lngDogID" int8 NOT NULL PRIMARY KEY,\n'
            '   "strLocusIdentifier" varchar(100) NOT NULL PRIMARY KEY,\n'
            '   "bytFirstAllele" int2 NOT NULL,\n'
            '   "bytSecondAllele" int2 NOT NULL\n'
            '   "blnHomozygous" bool NOT NULL\n'
            '   "lngSourceID" int2 NOT NULL\n'
            ');\n'
            '\n'
            'INSERT INTO "public"."tbl_alleles" (\n'
            '   "lngDogID", "strLocusIdentifier", "bytFirstAllele", "bytSecondAllele", "blnHomozygous", "lngSourceID") VALUES\n'
        )
        for _, row in alleles.iterrows():
            file.write(
                f"({row['dogID']}, '{row['locusID']}', {row['firstAllele']}, {row['secondAllele']}, {row['isHomozygous']}, {row['sourceID']}),\n"
            )

def map_bases(df: pd.DataFrame) -> None:
    mapping = {
        "A": "1",
        "C": "2",
        "G": "3",
        "T": "4",
        "0": "0",
    }
    df.replace({"firstAllele": mapping, "secondAllele": mapping}, inplace=True)