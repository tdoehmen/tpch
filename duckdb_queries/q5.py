from duckdb_queries import utils
import duckdb

Q_NUM = 5


def q():
    con = duckdb.connect(":memory:")
    customer = utils.get_customer_ds
    orders = utils.get_orders_ds
    lineitem = utils.get_line_item_ds
    supplier = utils.get_supplier_ds
    nation = utils.get_nation_ds
    regio = utils.get_region_ds
    # first call one time to cache in case we don't include the IO times
    customer(con)
    orders(con)
    lineitem(con)
    supplier(con)
    nation(con)
    regio(con)

    def query():
        nonlocal customer
        nonlocal orders
        nonlocal lineitem
        nonlocal supplier
        nonlocal nation
        nonlocal regio

        con = duckdb.connect(":memory:")
        con = customer(con)
        con = orders(con)
        con = lineitem(con)
        con = supplier(con)
        con = nation(con)
        con = regio(con)

        q = """SELECT
    n_name,
    sum(l_extendedprice * (1 - l_discount)) AS revenue
FROM
    customer,
    orders,
    lineitem,
    supplier,
    nation,
    region
WHERE
    c_custkey = o_custkey
    AND l_orderkey = o_orderkey
    AND l_suppkey = s_suppkey
    AND c_nationkey = s_nationkey
    AND s_nationkey = n_nationkey
    AND n_regionkey = r_regionkey
    AND r_name = 'ASIA'
    AND o_orderdate >= CAST('1994-01-01' AS date)
    AND o_orderdate < CAST('1995-01-01' AS date)
GROUP BY
    n_name
ORDER BY
    revenue DESC;"""

        result_df = con.execute(q).fetchdf()

        return result_df

    utils.run_query(Q_NUM, query)


if __name__ == "__main__":
    q()
