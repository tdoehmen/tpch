from duckdb_queries import utils
import duckdb

Q_NUM = 6


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
    sum(l_extendedprice * l_discount) AS revenue
FROM
    lineitem
WHERE
    l_shipdate >= CAST('1994-01-01' AS date)
    AND l_shipdate < CAST('1995-01-01' AS date)
    AND l_discount BETWEEN 0.05
    AND 0.07
    AND l_quantity < 24;"""

        result_df = con.execute(q).fetchdf()

        return result_df

    utils.run_query(Q_NUM, query)


if __name__ == "__main__":
    q()
