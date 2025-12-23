import polars as pl
import pandas as pd
from pathlib import Path

def parentage_test(child_file: Path, mom_file: Path, dad_file: Path) -> str:
    columns = ["chromosome", "snp_id", "genetic_distance", "position"]

    # TODO: File handling should occur in utils, this func should receive dataframes
    child = pl.from_pandas(pd.read_csv(
        child_file, sep="\\s+", names=columns + ["child_firstAllele", "child_secondAllele"])
        .drop("genetic_distance", axis=1))
    mom = pl.from_pandas(pd.read_csv(
        mom_file, sep="\\s+", names=columns + ["mom_firstAllele", "mom_secondAllele"])
        .drop("genetic_distance", axis=1))
    dad = pl.from_pandas(pd.read_csv(
        dad_file, sep="\\s+", names=columns + ["dad_firstAllele", "dad_secondAllele"])
        .drop("genetic_distance", axis=1))

    mom_dad_joined = mom.join(dad, on=["chromosome", "snp_id", "position"])
    trio_df = mom_dad_joined.join(child, on=["chromosome", "snp_id", "position"])

    autossomes_df = trio_df.filter(pl.col("chromosome") <= 38)
    x_chromosome_df = trio_df.filter(pl.col("chromosome") == 39)
    y_chromosome_df = trio_df.filter(pl.col("chromosome") == 40)
    # Chromosome 41 is unknown or unmapped genes, not in use
    mitochondrial_df = trio_df.filter(pl.col("chromosome") == 42)

    mendellian_error_rate = _test_autossomes(autossomes_df)
    mitochondrial_error_rate = _test_mt(mitochondrial_df)
    sex_chr_error_rates = _test_sex_chr(x_chromosome_df, y_chromosome_df)

    if mendellian_error_rate < 0.01:
        return "Very strong evidence of parentage"
    elif mendellian_error_rate < 0.02:
        return "Strong evidence of parentage"
    elif mendellian_error_rate >= 0.02:
        if mitochondrial_error_rate < 0.02 and sex_chr_error_rates < 0.02:
            return "Moderate evidence of parentage"
    return "Weak evidence of parentage, unlikely match"

def _test_autossomes(df: pl.DataFrame) -> float:
    total_snps = df.height

    mendelian_errors = df.filter(
        ~(
            ((pl.col("mom_firstAllele") == pl.col("child_firstAllele")) & (pl.col("dad_firstAllele") == pl.col("child_secondAllele"))) |
            ((pl.col("mom_secondAllele") == pl.col("child_firstAllele")) & (pl.col("dad_firstAllele") == pl.col("child_secondAllele"))) |
            ((pl.col("mom_firstAllele") == pl.col("child_firstAllele")) & (pl.col("dad_secondAllele") == pl.col("child_secondAllele"))) |
            ((pl.col("mom_secondAllele") == pl.col("child_firstAllele")) & (pl.col("dad_secondAllele") == pl.col("child_secondAllele"))) |
            ((pl.col("dad_firstAllele") == pl.col("child_firstAllele")) & (pl.col("mom_firstAllele") == pl.col("child_secondAllele"))) |
            ((pl.col("dad_secondAllele") == pl.col("child_firstAllele")) & (pl.col("mom_firstAllele") == pl.col("child_secondAllele"))) |
            ((pl.col("dad_firstAllele") == pl.col("child_firstAllele")) & (pl.col("mom_secondAllele") == pl.col("child_secondAllele"))) |
            ((pl.col("dad_secondAllele") == pl.col("child_firstAllele")) & (pl.col("mom_secondAllele") == pl.col("child_secondAllele")))
        ))

    num_errors = mendelian_errors.height
    return num_errors / total_snps if total_snps > 0 else 0

def _test_sex_chr(x_df: pl.DataFrame, y_df: pl.DataFrame) -> float:
    if _child_is_male(y_df):
        y_error = _test_y(y_df)
        x_error = _test_x_male(x_df)
        return (y_error + x_error) / 2
    else:
        return _test_x_female(x_df)

def _child_is_male(y_df: pl.DataFrame) -> bool:
    if y_df.height == 0:
        return False

    allele_counts = y_df.group_by("child_firstAllele", "child_secondAllele"
                        ).agg(pl.len().alias("count")
                        ).sort("count", descending=True)
    most_common = allele_counts[0]

    if most_common["child_firstAllele"][0] != 0:
        return True
    return False

def _test_y(y_df: pl.DataFrame) -> float:
    total_snps = y_df.height
    mendelian_errors = y_df.filter(
        ~(
            (pl.col("dad_firstAllele") == pl.col("child_firstAllele"))
        )
    )

    num_errors = mendelian_errors.height
    return num_errors / total_snps if total_snps > 0 else 0

def _test_x_male(x_df: pl.DataFrame) -> float:
    total_snps = x_df.height

    mendelian_errors = x_df.filter(
        ~(
            (pl.col("mom_firstAllele") == pl.col("child_firstAllele")) |
            (pl.col("mom_secondAllele") == pl.col("child_firstAllele"))
        )
    )

    num_errors = mendelian_errors.height
    return num_errors / total_snps if total_snps > 0 else 0

def _test_x_female(x_df: pl.DataFrame) -> float:
    total_snps = x_df.height

    mendelian_errors = x_df.filter(
        ~(
            ((pl.col("mom_firstAllele") == pl.col("child_firstAllele")) & (pl.col("dad_firstAllele") == pl.col("child_secondAllele"))) |
            ((pl.col("mom_secondAllele") == pl.col("child_firstAllele")) & (pl.col("dad_firstAllele") == pl.col("child_secondAllele"))) |
            ((pl.col("dad_firstAllele") == pl.col("child_firstAllele")) & (pl.col("mom_firstAllele") == pl.col("child_secondAllele"))) |
            ((pl.col("dad_firstAllele") == pl.col("child_firstAllele")) & (pl.col("mom_secondAllele") == pl.col("child_secondAllele")))
        )
    )

    num_errors = mendelian_errors.height
    return num_errors / total_snps if total_snps > 0 else 0

def _test_mt(mt_df: pl.DataFrame) -> float:
    total_snps = mt_df.height

    mendelian_errors = mt_df.filter(
        ~(pl.col("mom_firstAllele") == pl.col("child_firstAllele"))
    )

    num_errors = mendelian_errors.height
    return num_errors / total_snps if total_snps > 0 else 0