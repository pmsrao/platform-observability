from pyspark.sql import functions as F

def yyyymmdd(col):
    return F.date_format(F.to_date(col), "yyyyMMdd").cast("int")

def sha256_concat(cols):
    return F.sha2(F.concat_ws("||", *[F.coalesce(F.col(c).cast("string"), F.lit("")) for c in cols]), 256)
