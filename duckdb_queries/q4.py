from duckdb_queries import utils
import duckdb

Q_NUM = 4


def q():
    con = duckdb.connect(":memory:")
    orders = utils.get_orders_ds
    lineitem = utils.get_line_item_ds
    # first call one time to cache in case we don't include the IO times
    orders(con)
    lineitem(con)

    def query():
        nonlocal orders
        nonlocal lineitem

        con = duckdb.connect(":memory:")
        con = orders(con)
        con = lineitem(con)

        q = """SELECT
    o_orderpriority,
    count(*) AS order_count
FROM
    orders
WHERE
    o_orderdate >= CAST('1993-07-01' AS date)
    AND o_orderdate < CAST('1993-10-01' AS date)
    AND EXISTS (
        SELECT
            *
        FROM
            lineitem
        WHERE
            l_orderkey = o_orderkey
            AND l_commitdate < l_receiptdate)
GROUP BY
    o_orderpriority
ORDER BY
    o_orderpriority;"""

        result_df = con.execute(q).fetchdf()

        return result_df

    utils.run_query(Q_NUM, query)


if __name__ == "__main__":
    q()
