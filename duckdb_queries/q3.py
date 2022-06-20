from duckdb_queries import utils
import duckdb

Q_NUM = 3


def q():
    con = duckdb.connect(":memory:")
    customer = utils.get_customer_ds
    orders = utils.get_orders_ds
    lineitem = utils.get_line_item_ds
    # first call one time to cache in case we don't include the IO times
    customer(con)
    orders(con)
    lineitem(con)

    def query():
        nonlocal customer
        nonlocal orders
        nonlocal lineitem

        con = duckdb.connect(":memory:")
        con = customer(con)
        con = orders(con)
        con = lineitem(con)

        q = """SELECT
    l_orderkey,
    sum(l_extendedprice * (1 - l_discount)) AS revenue,
    o_orderdate,
    o_shippriority
FROM
    customer,
    orders,
    lineitem
WHERE
    c_mktsegment = 'BUILDING'
    AND c_custkey = o_custkey
    AND l_orderkey = o_orderkey
    AND o_orderdate < CAST('1995-03-15' AS date)
    AND l_shipdate > CAST('1995-03-15' AS date)
GROUP BY
    l_orderkey,
    o_orderdate,
    o_shippriority
ORDER BY
    revenue DESC,
    o_orderdate
LIMIT 10;"""

        result_df = con.execute(q).fetchdf()

        return result_df

    utils.run_query(Q_NUM, query)


if __name__ == "__main__":
    q()
