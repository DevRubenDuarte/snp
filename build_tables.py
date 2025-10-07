import pandas as pd

tped_file = pd.read_csv("~/dogs_global/database/barbel_ex/barbel.tped", sep=" ", header=None)


def create_tbl_loci_sql(tped) -> None:
    loci = pd.DataFrame(
        columns=["indexID","chromossome","locusID","distance","embark8","VHL","embark9","myDogDNA"]
    )

    tped[len(tped.columns)] = (tped.index + 1) * 8

    loci = loci.assign(
                indexID=tped[6],
                chromossome=tped[0],
                locusID=tped[1],
                distance=tped[3],
                embark8=True
            )

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

create_tbl_loci_sql(tped_file)