"""
=====================================================================
PySpark Daily Practice — ETL Scenario Template (Day 22 onward)
=====================================================================
Day 26 — stg_replenishment_request  (Medium-Hard)

ETL layer : L1 staging / cleansing
Domain    : inventory replenishment

BUSINESS CONTEXT
----------------
Store associates raise replenishment requests on handheld scanners: "send
store S01 four cases of SKU-101 in the 6-pack". The handheld writes each
request straight into a landing table, keys typed or scanned exactly as the
associate entered them. This job runs every hour and publishes the staging
table the order-allocation service reads: every request conformed against
the reference data, converted from cases to units, and routed to its serving
distribution centre — or explicitly quarantined with a reason. Allocation
only ever reads ACCEPTED rows; the store-ops team works the QUARANTINED rows
by hand. A request that is in neither pile is a store that never gets its
stock and nobody who knows why.

INPUT TABLES
------------
raw_replenishment_request(request_id: string, store_code: string, sku: string,
                          pack_code: string, qty_cases: int)
    -- append-only landing feed from the store handhelds, one row per
       request, `request_id` unique. Keys are stored exactly as entered:
       casing and surrounding whitespace are not normalized. `pack_code` is
       the pack configuration ordered; it is NULL when the SKU is sold in a
       single pack configuration — the handheld leaves the field empty
       because there is nothing to choose. `qty_cases` is always a positive
       integer.

store_dim(store_code: string, dc_code: string, store_name: string)
    -- daily snapshot from store operations, one row per open store.
       `dc_code` is the distribution centre that replenishes the store and is
       never NULL. `store_name` is a display attribute; allocation has no use
       for it.

sku_master(sku: string, sku_status: string, buyer_id: string)
    -- daily snapshot from merchandising, one row per SKU. `sku_status` is
       'ACTIVE' or 'DISCONTINUED' and is never NULL. `buyer_id` is the
       merchandising owner; allocation has no use for it.

sku_pack(sku: string, pack_code: string, units_per_case: int)
    -- daily snapshot from merchandising, one row per (sku, pack_code). A SKU
       sold in several pack configurations has one row per pack code. A SKU
       sold in a single pack configuration has exactly one row, with
       `pack_code` NULL. Upstream guarantee: the handheld only offers the
       pack codes sku_pack holds for that SKU, so every request whose `sku`
       exists in sku_master names a (sku, pack_code) that sku_pack holds.

Values in store_dim, sku_master and sku_pack are already trimmed and
upper-cased. In every box below, NULL is a real SQL NULL, never the string
'NULL'.

raw_replenishment_request:

    +------------+------------+---------+-----------+-----------+
    | request_id | store_code | sku     | pack_code | qty_cases |
    +------------+------------+---------+-----------+-----------+
    | R01        | S01        | SKU-101 | CS6       |         4 |
    | R02        |  s02       | sku-101 | CS24      |         2 |
    | R03        | S03        | SKU-102 | NULL      |         3 |
    | R04        | S04        | SKU-104 | CS8       |         5 |
    | R05        | s01        | SKU-999 | CS6       |         1 |
    | R06        | S02        | SKU-103 | NULL      |         6 |
    | R07        | S03        | sku-104 | cs8       |         2 |
    | R08        | S09        | SKU-777 | CS6       |         1 |
    +------------+------------+---------+-----------+-----------+

    Notes: whitespace is part of the raw value and the box cannot show a
    trailing blank, so exactly:
      R02.store_code = ' s02'   (one leading space)
      R05.store_code = 's01 '   (one trailing space)
    Every other raw value has no surrounding whitespace.

store_dim:

    +------------+---------+--------------+
    | store_code | dc_code | store_name   |
    +------------+---------+--------------+
    | S01        | DC-EAST | Midtown      |
    | S02        | DC-EAST | Harbor Point |
    | S03        | DC-WEST | Lakeside     |
    +------------+---------+--------------+

sku_master:

    +---------+--------------+----------+
    | sku     | sku_status   | buyer_id |
    +---------+--------------+----------+
    | SKU-101 | ACTIVE       | B07      |
    | SKU-102 | ACTIVE       | B03      |
    | SKU-103 | DISCONTINUED | B03      |
    | SKU-104 | ACTIVE       | B07      |
    +---------+--------------+----------+

sku_pack:

    +---------+-----------+----------------+
    | sku     | pack_code | units_per_case |
    +---------+-----------+----------------+
    | SKU-101 | CS6       |              6 |
    | SKU-101 | CS24      |             24 |
    | SKU-102 | NULL      |             12 |
    | SKU-103 | NULL      |             10 |
    | SKU-104 | CS8       |              8 |
    | SKU-104 | CS16      |             16 |
    +---------+-----------+----------------+

OUTPUT CONTRACT
---------------
Table    : stg_replenishment_request
Grain    : one row per request_id — every request in the landing batch,
           ACCEPTED or QUARANTINED
Columns  : request_id: string, store_code: string, sku: string,
           pack_code: string, dc_code: string, qty_units: int,
           load_status: string, reject_reason: string
           (this exact order)
Ordering : irrelevant — check() sorts both sides

Conforming and routing rules:
  N1  store_code, sku and pack_code are trimmed and upper-cased before any
      lookup. The output carries the normalized values. A NULL stays NULL.
  R1  A request is routed by the FIRST rule below that fires; exactly one
      reason per quarantined request, never two rows for one request:
        1. UNKNOWN_STORE     its store_code has no row in store_dim
        2. UNKNOWN_SKU       its sku has no row in sku_master
        3. DISCONTINUED_SKU  its sku's sku_status is 'DISCONTINUED'
      A request no rule fires on is ACCEPTED.

Column semantics:
  store_code / sku / pack_code   the N1-normalized request values, on every
                                 row, ACCEPTED or QUARANTINED
  dc_code        the store's `dc_code` from store_dim. ACCEPTED rows only;
                 NULL on every QUARANTINED row.
  qty_units      `qty_cases` x the `units_per_case` of the pack the request
                 ordered, from sku_pack. ACCEPTED rows only; NULL on every
                 QUARANTINED row.
  load_status    'ACCEPTED' or 'QUARANTINED'
  reject_reason  the R1 reason for a QUARANTINED row; NULL when ACCEPTED

Expected:

    +------------+------------+---------+-----------+---------+-----------+-------------+------------------+
    | request_id | store_code | sku     | pack_code | dc_code | qty_units | load_status | reject_reason    |
    +------------+------------+---------+-----------+---------+-----------+-------------+------------------+
    | R01        | S01        | SKU-101 | CS6       | DC-EAST |        24 | ACCEPTED    | NULL             |
    | R02        | S02        | SKU-101 | CS24      | DC-EAST |        48 | ACCEPTED    | NULL             |
    | R03        | S03        | SKU-102 | NULL      | DC-WEST |        36 | ACCEPTED    | NULL             |
    | R04        | S04        | SKU-104 | CS8       | NULL    |      NULL | QUARANTINED | UNKNOWN_STORE    |
    | R05        | S01        | SKU-999 | CS6       | NULL    |      NULL | QUARANTINED | UNKNOWN_SKU      |
    | R06        | S02        | SKU-103 | NULL      | NULL    |      NULL | QUARANTINED | DISCONTINUED_SKU |
    | R07        | S03        | SKU-104 | CS8       | DC-WEST |        16 | ACCEPTED    | NULL             |
    | R08        | S09        | SKU-777 | CS6       | NULL    |      NULL | QUARANTINED | UNKNOWN_STORE    |
    +------------+------------+---------+-----------+---------+-----------+-------------+------------------+

    Note: R08 fires both rule 1 and rule 2; rule 1 comes first.

PRODUCTION CONSTRAINTS
----------------------
P1. Every request in the landing batch must come out of the job exactly
    once — ACCEPTED or QUARANTINED, never dropped, never duplicated. Before
    returning, the job must assert it: count(ACCEPTED) + count(QUARANTINED)
    equals the row count of raw_replenishment_request, and `request_id` is
    unique in the output.
P2. Before returning, the job must also assert its routing is consistent:
    every ACCEPTED row has a non-NULL `dc_code` and `qty_units` and a NULL
    `reject_reason`; every QUARANTINED row has a non-NULL `reject_reason` and
    a NULL `dc_code` and `qty_units`. A silent bad publish is worse than a
    failed job.
P3. Read only the columns this job needs from the three reference tables.
    `store_name` and `buyer_id` are operational metadata and must not enter
    the pipeline.

These are requirements, not hints. Missing one fails the tests.

WORKFLOW (v2)
-------------
Stage 1  SOLVE   : Implement the two Part 1 functions. Run this file.
Stage 2  GENERATE: Run /genprompt and paste the emitted prompt into a
                   SEPARATE incognito conversation. Never ask for the
                   solution inside this repo.
Stage 3  REVIEW  : Paste the AI answer into the Part 3 zone, UNMODIFIED.
                   Review by reading only. Fill REVIEW_NOTES and commit a
                   VERDICT BEFORE running anything.
Stage 4  VERIFY  : Un-comment the Stage-4 lines in Part 2 and run.
Stage 5  DIGEST  : Fill Part 5, then run /review and /digest.

Reference answers and concept takeaways live in
refs/day26_replenishment_staging_ref.md. Do not open it before Stage 5.
=====================================================================
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import StringType


# #####################################################################
# ##                                                                 ##
# ##   PART 1 — YOUR JOB                                             ##
# ##                                                                 ##
# ##   Stage 1 touches this block and nothing else in this file.     ##
# ##                                                                 ##
# #####################################################################

# ------------------------------ 1a. DataFrame API --------------------
def build_stg_replenishment_request_dsl(
    raw_replenishment_request: DataFrame,
    store_dim: DataFrame,
    sku_master: DataFrame,
    sku_pack: DataFrame,
) -> DataFrame:
    """Conform, route and quarantine one landing batch of store requests.

    Hint: count the requests on the way in; the job is not done until the
    same number comes out.
    """
    # TODO: implement
    cols = ['store_code','sku', 'pack_code']
    cleaned_raw = (
        raw_replenishment_request.select(
            'request_id',
            *[F.upper(F.trim(F.col(c))).alias(c) if isinstance(raw_replenishment_request.schema[c].dataType, StringType) else F.col(c) for c in cols],
        'qty_cases'
        )
    )

    store = store_dim.select('store_code', 'dc_code')
    sku = sku_master.select(F.col('sku').alias('master_sku'), 'sku_status')
    sku_pack = sku_pack.select(
        F.col('sku').alias('pack_sku'),
        'pack_code',
        'units_per_case'
    )

    req_store = (
        cleaned_raw.join(
            F.broadcast(store),
            'store_code',
            'left'
        ).select(
            'request_id',
            'store_code',
            'sku',
            'pack_code',
            'qty_cases',
            'dc_code'
        )
    )
    # join sku master with pack df to avoid fan-out and filter out sku not in master df
    sku_in_effect = (
        sku.join(
            F.broadcast(sku_pack),
            sku.master_sku == sku_pack.pack_sku,
            'left'
        ).select(
            F.col('master_sku'),
            'sku_status',
            'pack_code',
            'units_per_case'
        )
    )

    join_cond = (req_store.sku == sku_in_effect.master_sku) & (req_store.pack_code.eqNullSafe(sku_in_effect.pack_code))

    wide_df = (
        req_store.join(
            F.broadcast(sku_in_effect),
            join_cond,
            'left'
        ).withColumn(
            'qty_units',
            F.col('qty_cases') * F.col('units_per_case')
        ).select(
            'request_id',
            'store_code',
            req_store.sku.alias('sku'),
            'master_sku',
            req_store.pack_code.alias('pack_code'),
            'dc_code',
            'sku_status',
            F.col('qty_units').cast('int')
        )
    )

    wide_df = wide_df.withColumn(
        'reject_reason',
        F.when(F.col('dc_code').isNull(), F.lit('UNKNOWN_STORE'))
        .when(F.col('master_sku').isNull(), F.lit('UNKNOWN_SKU'))
        .when(F.col('sku_status') == 'DISCONTINUED', F.lit('DISCONTINUED_SKU'))
        .otherwise(F.lit(None))
    ).withColumn(
        'load_status',
        F.when(
            F.col('reject_reason').isNotNull(), F.lit('QUARANTINED')
        ).otherwise(F.lit('ACCEPTED'))
    ).withColumn(
        'dc_code',
        F.when(F.col('load_status') == 'QUARANTINED', F.lit(None)).otherwise(F.col('dc_code'))
    ).withColumn(
        'qty_units',
         F.when(F.col('load_status') == 'QUARANTINED', F.lit(None)).otherwise(F.col('qty_units'))
    ).select(
        'request_id',
        'store_code',
        'sku',
        'pack_code',
        'dc_code',
        'qty_units',
        'load_status',
        'reject_reason'
    )

    # CONSTRAINTS VALIDATION

    # P1. Every request in the landing batch must come out of the job exactly
    #     once — ACCEPTED or QUARANTINED, never dropped, never duplicated. Before
    #     returning, the job must assert it: count(ACCEPTED) + count(QUARANTINED)
    #     equals the row count of raw_replenishment_request, and `request_id` is
    #     unique in the output.
    row_cnt_reconciliation = (
        wide_df.agg(
            F.count(F.when(F.col('load_status') == "ACCEPTED", F.lit(1))).alias('cnt_accepted'),
            F.count(F.when(F.col('load_status') == "QUARANTINED", F.lit(1))).alias('cnt_quarantined'),
        )
    )

    left = row_cnt_reconciliation.select('cnt_accepted', 'cnt_quarantined').first()
    right = raw_replenishment_request.agg(F.count(F.lit(1)).alias('total_cnt')).select('total_cnt').first()['total_cnt']

    unique_req = wide_df.groupBy('request_id').agg(F.count(F.lit(1)).alias('cnt')).filter(F.col('cnt') > 1)

    assert left['cnt_accepted'] + left['cnt_quarantined'] == right, f"cnt of accepted plus cnt of quarantined is not equal to total cnt"
    assert unique_req, f"req id is not unique"

    # P2. Before returning, the job must also assert its routing is consistent:
    #     every ACCEPTED row has a non-NULL `dc_code` and `qty_units` and a NULL
    #     `reject_reason`; every QUARANTINED row has a non-NULL `reject_reason` and
    #     a NULL `dc_code` and `qty_units`. A silent bad publish is worse than a
    #     failed job.

    null_cond_accepted =(F.col('dc_code').isNull() | F.col('qty_units').isNull()) & (F.col('reject_reason').isNotNull())
    cnt_null_accepted_dc_qty = (
        wide_df.filter(
            (F.col('load_status') == 'ACCEPTED') & null_cond_accepted).count()
        )
    
    null_cond_quar = (F.col('dc_code').isNotNull() | F.col('qty_units').isNotNull()) & (F.col('reject_reason').isNull())
    cnt_null_quar_dc_qty = (
         wide_df.filter(
                    (F.col('load_status') == 'QUARANTINED') & null_cond_quar).count()
    )
    
    assert cnt_null_accepted_dc_qty == 0 and cnt_null_quar_dc_qty == 0, f'invalid values for load_status, accepted values is {cnt_null_accepted_dc_qty}, quar values is {cnt_null_quar_dc_qty}'

    return wide_df

# ------------------------------ 1b. Spark SQL ------------------------
def build_stg_replenishment_request_sql(
    spark: SparkSession,
    raw_replenishment_request: DataFrame,
    store_dim: DataFrame,
    sku_master: DataFrame,
    sku_pack: DataFrame,
) -> DataFrame:
    """Same job, Spark SQL.

    Hint: SQL computes the table; the P1/P2 assertions run in Python on the
    DataFrame that spark.sql() returns.
    """
    raw_replenishment_request.createOrReplaceTempView("raw_replenishment_request")
    store_dim.createOrReplaceTempView("store_dim")
    sku_master.createOrReplaceTempView("sku_master")
    sku_pack.createOrReplaceTempView("sku_pack")

    sql = """
        -- write your SQL here
        with cleaned_req as (
            select 
                request_id,
                upper(trim(store_code)) as store_code,
                upper(trim(sku)) as sku,
                upper(trim(pack_code)) as pack_code,
                qty_cases
            from raw_replenishment_request
        ),

        store as (
            select 
                store_code as s_store_code,
                dc_code 
            from 
                store_dim
        ),

        sku as (
            select 
                sku as m_sku,
                sku_status
        from sku_master
        ),

        pack as (
            select 
                sku as p_sku,
                pack_code,
                units_per_case
            from 
                sku_pack
        ),

        req_store as (
            select 
                request_id,
                store_code,
                s_store_code,
                sku,
                dc_code,
                pack_code,
                qty_cases
            from 
                cleaned_req cr left join store s on cr.store_code = s_store_code
        ),

        sku_with_pack as (
            select 
                m_sku,
                sku_status,
                pack_code,
                units_per_case
            from 
                sku left join pack p on sku.m_sku = p.p_sku
        ),

        agg as (
            select 
                request_id,
                store_code,
                sku,
                rs.pack_code as pack_code,
                dc_code,
                case 
                    when s_store_code is null then 'UNKNOWN_STORE'
                    when m_sku is null then 'UNKNOWN_SKU'
                    when sku_status = 'DISCONTINUED' then 'DISCONTINUED_SKU'
                    end as reject_reason,
                cast(qty_cases * units_per_case as bigint) as qty_units
            from req_store rs left join sku_with_pack s on rs.sku = s.m_sku and rs.pack_code <=> s.pack_code
        ),

        load as (
            select 
                request_id,
                store_code,
                sku,
                pack_code,
                dc_code, 
                qty_units,
                case 
                    when reject_reason is not null then 'QUARANTINED' else 'ACCEPTED' end as load_status,
                reject_reason
            from agg
        )

        select 
            request_id,
            store_code,
            sku,
            pack_code,
            case when load_status = 'QUARANTINED' then null else dc_code end as dc_code, 
            case when load_status = 'QUARANTINED' then null else qty_units end as qty_units,
            load_status,
            reject_reason
        from 
            load
    """

    res = spark.sql(sql)
    row_cnt_reconciliation = (
            res.agg(
                F.count(F.when(F.col('load_status') == "ACCEPTED", F.lit(1))).alias('cnt_accepted'),
                F.count(F.when(F.col('load_status') == "QUARANTINED", F.lit(1))).alias('cnt_quarantined'),
            )
        )
    # P1
    left = row_cnt_reconciliation.select('cnt_accepted', 'cnt_quarantined').first()
    right = raw_replenishment_request.agg(F.count(F.lit(1)).alias('total_cnt')).select('total_cnt').first()['total_cnt']

    unique_req = res.groupBy('request_id').agg(F.count(F.lit(1)).alias('cnt')).filter(F.col('cnt') > 1)

    assert left['cnt_accepted'] + left['cnt_quarantined'] == right, f"cnt of accepted plus cnt of quarantined is not equal to total cnt"
    assert unique_req, f"req id is not unique"

    # P2
    null_cond_accepted =(F.col('dc_code').isNull() | F.col('qty_units').isNull()) & (F.col('reject_reason').isNotNull())
    cnt_null_accepted_dc_qty = (
            res.filter(
                (F.col('load_status') == 'ACCEPTED') & null_cond_accepted).count()
            )
        
    null_cond_quar = (F.col('dc_code').isNotNull() | F.col('qty_units').isNotNull()) & (F.col('reject_reason').isNull())
    cnt_null_quar_dc_qty = (
             res.filter(
                        (F.col('load_status') == 'QUARANTINED') & null_cond_quar).count()
        )
        
    assert cnt_null_accepted_dc_qty == 0 and cnt_null_quar_dc_qty == 0, f'invalid values for load_status, accepted values is {cnt_null_accepted_dc_qty}, quar values is {cnt_null_quar_dc_qty}'
    
    return res 


# #####################################################################
# ##   END OF PART 1                                                 ##
# #####################################################################


# #####################################################################
# ##                                                                 ##
# ##   PART 3 — PASTE THE INCOGNITO AI ANSWER BELOW                  ##
# ##                                                                 ##
# ##   UNMODIFIED. Two functions, `ai_` prefix, signatures identical ##
# ##   to Part 1. Do not reformat it. Do not fix anything you spot — ##
# ##   spotting it is exactly what Stage 3 measures.                 ##
# ##                                                                 ##
# #####################################################################

# >>> PASTE BEGIN
def ai_build_stg_replenishment_request_dsl(
    raw_replenishment_request: DataFrame,
    store_dim: DataFrame,
    sku_master: DataFrame,
    sku_pack: DataFrame,
) -> DataFrame:
    raw = raw_replenishment_request.select(
        "request_id", "store_code", "sku", "pack_code", "qty_cases"
    ).localCheckpoint(eager=True)
    raw_count = raw.count()
 
    req = raw.select(
        F.col("request_id"),
        F.upper(F.trim(F.col("store_code"))).alias("store_code"),
        F.upper(F.trim(F.col("sku"))).alias("sku"),
        F.upper(F.trim(F.col("pack_code"))).alias("pack_code"),
        F.col("qty_cases"),
    )
 
    st = store_dim.select(
        F.col("store_code").alias("st_store_code"),
        F.col("dc_code").alias("st_dc_code"),
    )
    sm = sku_master.select(
        F.col("sku").alias("sm_sku"),
        F.col("sku_status").alias("sm_sku_status"),
    )
    sp = sku_pack.select(
        F.col("sku").alias("sp_sku"),
        F.col("pack_code").alias("sp_pack_code"),
        F.col("units_per_case").alias("sp_units_per_case"),
    )
 
    joined = (
        req.join(st, req["store_code"] == st["st_store_code"], "left")
        .join(sm, req["sku"] == sm["sm_sku"], "left")
        .join(
            sp,
            (req["sku"] == sp["sp_sku"])
            & req["pack_code"].eqNullSafe(sp["sp_pack_code"]),
            "left",
        )
    )
 
    reason = (
        F.when(F.col("st_store_code").isNull(), F.lit("UNKNOWN_STORE"))
        .when(F.col("sm_sku").isNull(), F.lit("UNKNOWN_SKU"))
        .when(F.col("sm_sku_status") == "DISCONTINUED", F.lit("DISCONTINUED_SKU"))
    )
 
    routed = joined.withColumn("reject_reason", reason)
    accepted = F.col("reject_reason").isNull()
 
    out = routed.select(
        F.col("request_id"),
        F.col("store_code"),
        F.col("sku"),
        F.col("pack_code"),
        F.when(accepted, F.col("st_dc_code")).cast("string").alias("dc_code"),
        F.when(accepted, F.col("qty_cases") * F.col("sp_units_per_case"))
        .cast("int")
        .alias("qty_units"),
        F.when(accepted, F.lit("ACCEPTED"))
        .otherwise(F.lit("QUARANTINED"))
        .alias("load_status"),
        F.col("reject_reason").cast("string").alias("reject_reason"),
    ).localCheckpoint(eager=True)
 
    s = out.agg(
        F.count(F.lit(1)).alias("n_rows"),
        F.countDistinct("request_id").alias("n_ids"),
        F.count_if(F.col("load_status") == "ACCEPTED").alias("n_acc"),
        F.count_if(F.col("load_status") == "QUARANTINED").alias("n_quar"),
        F.count_if(
            (F.col("load_status") == "ACCEPTED")
            & (
                F.col("dc_code").isNull()
                | F.col("qty_units").isNull()
                | F.col("reject_reason").isNotNull()
            )
        ).alias("n_bad_acc"),
        F.count_if(
            (F.col("load_status") == "QUARANTINED")
            & (
                F.col("reject_reason").isNull()
                | F.col("dc_code").isNotNull()
                | F.col("qty_units").isNotNull()
            )
        ).alias("n_bad_quar"),
    ).first()
 
    if s["n_acc"] + s["n_quar"] != raw_count or s["n_rows"] != raw_count:
        raise AssertionError(
            f"P1 violated: accepted={s['n_acc']} quarantined={s['n_quar']} "
            f"output_rows={s['n_rows']} raw_rows={raw_count}"
        )
    if s["n_ids"] != s["n_rows"]:
        raise AssertionError(
            f"P1 violated: request_id not unique/non-null "
            f"(distinct={s['n_ids']} rows={s['n_rows']})"
        )
    if s["n_bad_acc"] != 0 or s["n_bad_quar"] != 0:
        raise AssertionError(
            f"P2 violated: inconsistent ACCEPTED rows={s['n_bad_acc']} "
            f"inconsistent QUARANTINED rows={s['n_bad_quar']}"
        )
 
    return out
 
 
def ai_build_stg_replenishment_request_sql(
    spark: SparkSession,
    raw_replenishment_request: DataFrame,
    store_dim: DataFrame,
    sku_master: DataFrame,
    sku_pack: DataFrame,
) -> DataFrame:
    raw = spark.sql(
        """
        SELECT request_id, store_code, sku, pack_code, qty_cases
        FROM {raw_src}
        """,
        raw_src=raw_replenishment_request,
    ).localCheckpoint(eager=True)
 
    raw_count = spark.sql("SELECT COUNT(*) AS n FROM {raw}", raw=raw).first()["n"]
 
    out = spark.sql(
        """
        WITH req AS (
            SELECT request_id,
                   UPPER(TRIM(store_code)) AS store_code,
                   UPPER(TRIM(sku))        AS sku,
                   UPPER(TRIM(pack_code))  AS pack_code,
                   qty_cases
            FROM {raw}
        ),
        st AS (SELECT store_code, dc_code FROM {store_dim}),
        sm AS (SELECT sku, sku_status FROM {sku_master}),
        sp AS (SELECT sku, pack_code, units_per_case FROM {sku_pack}),
        routed AS (
            SELECT r.request_id,
                   r.store_code,
                   r.sku,
                   r.pack_code,
                   r.qty_cases,
                   st.dc_code,
                   sp.units_per_case,
                   CASE
                       WHEN st.store_code IS NULL           THEN 'UNKNOWN_STORE'
                       WHEN sm.sku IS NULL                  THEN 'UNKNOWN_SKU'
                       WHEN sm.sku_status = 'DISCONTINUED'  THEN 'DISCONTINUED_SKU'
                   END AS reject_reason
            FROM req r
            LEFT JOIN st ON r.store_code = st.store_code
            LEFT JOIN sm ON r.sku = sm.sku
            LEFT JOIN sp ON r.sku = sp.sku AND r.pack_code <=> sp.pack_code
        )
        SELECT request_id,
               store_code,
               sku,
               pack_code,
               CAST(CASE WHEN reject_reason IS NULL THEN dc_code END AS STRING) AS dc_code,
               CAST(CASE WHEN reject_reason IS NULL THEN qty_cases * units_per_case END AS INT) AS qty_units,
               CASE WHEN reject_reason IS NULL THEN 'ACCEPTED' ELSE 'QUARANTINED' END AS load_status,
               CAST(reject_reason AS STRING) AS reject_reason
        FROM routed
        """,
        raw=raw,
        store_dim=store_dim,
        sku_master=sku_master,
        sku_pack=sku_pack,
    ).localCheckpoint(eager=True)
 
    s = spark.sql(
        """
        SELECT COUNT(*)                                          AS n_rows,
               COUNT(DISTINCT request_id)                        AS n_ids,
               COUNT_IF(load_status = 'ACCEPTED')                AS n_acc,
               COUNT_IF(load_status = 'QUARANTINED')             AS n_quar,
               COUNT_IF(load_status = 'ACCEPTED'
                        AND (dc_code IS NULL
                             OR qty_units IS NULL
                             OR reject_reason IS NOT NULL))      AS n_bad_acc,
               COUNT_IF(load_status = 'QUARANTINED'
                        AND (reject_reason IS NULL
                             OR dc_code IS NOT NULL
                             OR qty_units IS NOT NULL))          AS n_bad_quar
        FROM {out}
        """,
        out=out,
    ).first()
 
    if s["n_acc"] + s["n_quar"] != raw_count or s["n_rows"] != raw_count:
        raise AssertionError(
            f"P1 violated: accepted={s['n_acc']} quarantined={s['n_quar']} "
            f"output_rows={s['n_rows']} raw_rows={raw_count}"
        )
    if s["n_ids"] != s["n_rows"]:
        raise AssertionError(
            f"P1 violated: request_id not unique/non-null "
            f"(distinct={s['n_ids']} rows={s['n_rows']})"
        )
    if s["n_bad_acc"] != 0 or s["n_bad_quar"] != 0:
        raise AssertionError(
            f"P2 violated: inconsistent ACCEPTED rows={s['n_bad_acc']} "
            f"inconsistent QUARANTINED rows={s['n_bad_quar']}"
        )
 
    return out
# >>> PASTE END

# ---------------------------------------------------------------------
# REVIEW_NOTES — fill in BEFORE running anything.
# Severity order: runtime break > wrong results on dirty data
#               > production robustness > performance > portability > style
# ---------------------------------------------------------------------
# [ ] Correctness — ties? nulls? empty groups? duplicate keys? boundary rows?
#     notes: LGTM, joining by equalNullSafe is implemented on pack_code
# [ ] API usage — wrong signatures, hallucinated functions, deprecated calls,
#     ambiguous column references in joins, SQL syntax slips?
#     notes: variables named exactly, no ambiguous here,(like st_store_code is store_code from dim store, which is distinguished from req table)
# [ ] ANSI behavior — does any COALESCE/fallback assume a NULL that ANSI mode
#     will not deliver (cast, division, array index, element_at)?
#     notes: I only cast numeric value here but AI cast all including string
# [ ] Production — walk P1/P2/P3 one at a time, each on its own line.
#     Re-run idempotent? Late data attributed to the event date? The same
#     metric computed the same way in every branch? Quality assertions there?
#     notes: LGTM, but it looks like I missed distinct req id check here
# [ ] Performance — extra Exchange? extra scan? window without partitionBy?
#     (reading-stage hypothesis only — verified with .explain() later,
#     never asserted from memory)
#     notes: I joined sku master with sku pack first then joining with req and store, but AI join these table at the same time, I am not sure if this will result in fan-out for sku tables
# [ ] Robustness — hardcoded values, assumptions not in the output contract?
#     notes: no, this part is pretty good for AI's solution, it assign var for accepted status and use it in following DSL to aviod more codes
# [ ] Style/clarity — would you approve this in a real code review?
#     notes: yes 
#
# VERDICT (commit before running): PASS — because: looks good, but I am not sure if it will lead to fan-out if not merge sku master with pack first
# ACTUAL RESULT (after Stage 4 run):
# GAP ANALYSIS: did the run reveal anything the reading missed?


# #####################################################################
# ##   PART 2 — HARNESS.  Do not edit.                               ##
# #####################################################################
def check(actual: DataFrame, expected_rows: list, label: str) -> None:
    """Order-insensitive comparison; sorted() absorbs tie-order instability."""
    actual_rows = sorted([tuple(r) for r in actual.collect()])
    expected = sorted(expected_rows)
    status = "PASS" if actual_rows == expected else "FAIL"
    print(f"[{status}] {label}")
    if status == "FAIL":
        print("  expected:", expected)
        print("  actual  :", actual_rows)


if __name__ == "__main__":
    spark = (
        SparkSession.builder
        .appName("daily-practice")
        .master("local[2]")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")

    raw_replenishment_request = spark.createDataFrame(
        [
            ("R01", "S01", "SKU-101", "CS6", 4),
            ("R02", " s02", "sku-101", "CS24", 2),
            ("R03", "S03", "SKU-102", None, 3),
            ("R04", "S04", "SKU-104", "CS8", 5),
            ("R05", "s01 ", "SKU-999", "CS6", 1),
            ("R06", "S02", "SKU-103", None, 6),
            ("R07", "S03", "sku-104", "cs8", 2),
            ("R08", "S09", "SKU-777", "CS6", 1),
        ],
        schema=(
            "request_id string, store_code string, sku string, "
            "pack_code string, qty_cases int"
        ),
    )
    store_dim = spark.createDataFrame(
        [
            ("S01", "DC-EAST", "Midtown"),
            ("S02", "DC-EAST", "Harbor Point"),
            ("S03", "DC-WEST", "Lakeside"),
        ],
        schema="store_code string, dc_code string, store_name string",
    )
    sku_master = spark.createDataFrame(
        [
            ("SKU-101", "ACTIVE", "B07"),
            ("SKU-102", "ACTIVE", "B03"),
            ("SKU-103", "DISCONTINUED", "B03"),
            ("SKU-104", "ACTIVE", "B07"),
        ],
        schema="sku string, sku_status string, buyer_id string",
    )
    sku_pack = spark.createDataFrame(
        [
            ("SKU-101", "CS6", 6),
            ("SKU-101", "CS24", 24),
            ("SKU-102", None, 12),
            ("SKU-103", None, 10),
            ("SKU-104", "CS8", 8),
            ("SKU-104", "CS16", 16),
        ],
        schema="sku string, pack_code string, units_per_case int",
    )

    expected = [
        ("R01", "S01", "SKU-101", "CS6", "DC-EAST", 24, "ACCEPTED", None),
        ("R02", "S02", "SKU-101", "CS24", "DC-EAST", 48, "ACCEPTED", None),
        ("R03", "S03", "SKU-102", None, "DC-WEST", 36, "ACCEPTED", None),
        ("R04", "S04", "SKU-104", "CS8", None, None, "QUARANTINED", "UNKNOWN_STORE"),
        ("R05", "S01", "SKU-999", "CS6", None, None, "QUARANTINED", "UNKNOWN_SKU"),
        ("R06", "S02", "SKU-103", None, None, None, "QUARANTINED", "DISCONTINUED_SKU"),
        ("R07", "S03", "SKU-104", "CS8", "DC-WEST", 16, "ACCEPTED", None),
        ("R08", "S09", "SKU-777", "CS6", None, None, "QUARANTINED", "UNKNOWN_STORE"),
    ]

    check(
        build_stg_replenishment_request_dsl(
            raw_replenishment_request, store_dim, sku_master, sku_pack
        ),
        expected,
        "DSL",
    )
    check(
        build_stg_replenishment_request_sql(
            spark, raw_replenishment_request, store_dim, sku_master, sku_pack
        ),
        expected,
        "SQL",
    )

    # -----------------------------------------------------------------
    # Stage 4 — run the AI answer through the SAME harness.
    # Un-comment only AFTER committing a VERDICT in Part 3.
    # -----------------------------------------------------------------
    check(ai_build_stg_replenishment_request_dsl(
              raw_replenishment_request, store_dim, sku_master, sku_pack),
          expected, "AI-DSL (post-review verification)")
    check(ai_build_stg_replenishment_request_sql(
              spark, raw_replenishment_request, store_dim, sku_master,
              sku_pack),
          expected, "AI-SQL (post-review verification)")

    spark.stop()


# #####################################################################
# ##   PART 5 — Review takeaways (fill at Stage 5, before /review)   ##
# #####################################################################
# - What did the AI get wrong, or suspiciously right?
# - Which production constraint (P#) did either side miss, and why was it
#   missable by reading?
# - Did I catch it by reading, or only by running?
# - What review heuristic should I add to log/04 next time?
#
# Concept takeaways for this problem:
# see refs/day26_replenishment_staging_ref.md.
