from duckdb_queries import utils
import duckdb

Q_NUM = 7


def q():
    con = duckdb.connect(":memory:")
    supplier = utils.get_supplier_ds
    lineitem = utils.get_line_item_ds
    orders = utils.get_orders_ds
    customer = utils.get_customer_ds
    nation = utils.get_nation_ds
    # first call one time to cache in case we don't include the IO times
    supplier(con)
    lineitem(con)
    orders(con)
    customer(con)
    nation(con)

    def query():
        nonlocal supplier
        nonlocal lineitem
        nonlocal orders
        nonlocal customer
        nonlocal nation
        con = duckdb.connect(":memory:")
        con = supplier(con)
        con = lineitem(con)
        con = orders(con)
        con = customer(con)
        con = nation(con)

        q = """SELECT
    supp_nation,
    cust_nation,
    l_year,
    sum(volume) AS revenue
FROM (
    SELECT
        n1.n_name AS supp_nation,
        n2.n_name AS cust_nation,
        extract(year FROM l_shipdate) AS l_year,
        l_extendedprice * (1 - l_discount) AS volume
    FROM
        supplier,
        lineitem,
        orders,
        customer,
        nation n1,
        nation n2
    WHERE
        s_suppkey = l_suppkey
        AND o_orderkey = l_orderkey
        AND c_custkey = o_custkey
        AND s_nationkey = n1.n_nationkey
        AND c_nationkey = n2.n_nationkey
        AND ((n1.n_name = 'FRANCE'
                AND n2.n_name = 'GERMANY')
            OR (n1.n_name = 'GERMANY'
                AND n2.n_name = 'FRANCE'))
        AND l_shipdate BETWEEN CAST('1995-01-01' AS date)
        AND CAST('1996-12-31' AS date)) AS shipping
GROUP BY
    supp_nation,
    cust_nation,
    l_year
ORDER BY
    supp_nation,
    cust_nation,
    l_year;"""

        result_df = con.execute(q).fetchdf()

        return result_df

    utils.run_query(Q_NUM, query)


if __name__ == "__main__":
    q()
