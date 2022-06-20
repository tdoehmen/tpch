from duckdb_queries import utils
import duckdb

Q_NUM = 2


def q():
    con = duckdb.connect(":memory:")
    part = utils.get_part_ds
    supplier = utils.get_supplier_ds
    partsupp = utils.get_part_supp_ds
    nation = utils.get_nation_ds
    region = utils.get_region_ds
    # first call one time to cache in case we don't include the IO times

    part(con)
    supplier(con)
    partsupp(con)
    nation(con)
    region(con)

    def query():
        nonlocal part
        nonlocal supplier
        nonlocal partsupp
        nonlocal nation
        nonlocal region

        con = duckdb.connect(":memory:")
        con = part(con)
        con = supplier(con)
        con = partsupp(con)
        con = nation(con)
        con = region(con)

        q = """SELECT
    s_acctbal,
    s_name,
    n_name,
    p_partkey,
    p_mfgr,
    s_address,
    s_phone,
    s_comment
FROM
    part,
    supplier,
    partsupp,
    nation,
    region
WHERE
    p_partkey = ps_partkey
    AND s_suppkey = ps_suppkey
    AND p_size = 15
    AND p_type LIKE '%BRASS'
    AND s_nationkey = n_nationkey
    AND n_regionkey = r_regionkey
    AND r_name = 'EUROPE'
    AND ps_supplycost = (
        SELECT
            min(ps_supplycost)
        FROM
            partsupp,
            supplier,
            nation,
            region
        WHERE
            p_partkey = ps_partkey
            AND s_suppkey = ps_suppkey
            AND s_nationkey = n_nationkey
            AND n_regionkey = r_regionkey
            AND r_name = 'EUROPE')
ORDER BY
    s_acctbal DESC,
    n_name,
    s_name,
    p_partkey
LIMIT 100;"""

        result_df = con.execute(q).fetchdf()

        return result_df

    utils.run_query(Q_NUM, query)


if __name__ == "__main__":
    q()
