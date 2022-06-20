from duckdb_queries import utils
import duckdb

Q_NUM = 1


def q():
    con = duckdb.connect(":memory:")
    lineitem = utils.get_line_item_ds
    # first call one time to cache in case we don't include the IO times
    lineitem(con)

    def query():
        nonlocal lineitem
        con = duckdb.connect(":memory:")
        con = lineitem(con)

        q = """SELECT
    l_returnflag,
    l_linestatus,
    sum(l_quantity) AS sum_qty,
    sum(l_extendedprice) AS sum_base_price,
    sum(l_extendedprice * (1 - l_discount)) AS sum_disc_price,
    sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)) AS sum_charge,
    avg(l_quantity) AS avg_qty,
    avg(l_extendedprice) AS avg_price,
    avg(l_discount) AS avg_disc,
    count(*) AS count_order
FROM
    lineitem
WHERE
    l_shipdate <= CAST('1998-09-02' AS date)
GROUP BY
    l_returnflag,
    l_linestatus
ORDER BY
    l_returnflag,
    l_linestatus;"""

        result_df = con.execute(q).fetchdf()

        return result_df

    utils.run_query(Q_NUM, query)


if __name__ == "__main__":
    q()
