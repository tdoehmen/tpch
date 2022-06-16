import os
from os.path import join

import polars as pl
from linetimer import CodeTimer, linetimer

from utils import (
    INCLUDE_IO,
    SHOW_RESULTS,
    __default_answers_base_dir,
    __default_dataset_base_dir,
)

SHOW_PLAN = os.environ.get("SHOW_PLAN", False)


def __scan_parquet_ds(path: str):
    scan = pl.scan_parquet(path)
    if INCLUDE_IO:
        return scan
    return scan.collect().lazy()


def get_query_answer(
    query: int, base_dir: str = __default_answers_base_dir
) -> pl.LazyFrame:
    answer_ldf = pl.scan_csv(
        join(base_dir, f"q{query}.out"), sep="|", has_header=True, parse_dates=True
    )
    cols = answer_ldf.columns
    answer_ldf = answer_ldf.select(
        [pl.col(c).alias(c.strip()) for c in cols]
    ).with_columns([pl.col(pl.datatypes.Utf8).str.strip().keep_name()])

    return answer_ldf


def test_results(q_num: int, result_df: pl.DataFrame):
    with CodeTimer(name=f"Testing result of Query {q_num}", unit="s"):
        answer = get_query_answer(q_num).collect()
        pl.testing.assert_frame_equal(left=result_df, right=answer, check_dtype=False)


def get_line_item_ds(base_dir: str = __default_dataset_base_dir) -> pl.LazyFrame:
    return __scan_parquet_ds(join(base_dir, "lineitem.parquet"))


def get_orders_ds(base_dir: str = __default_dataset_base_dir) -> pl.LazyFrame:
    return __scan_parquet_ds(join(base_dir, "orders.parquet"))


def get_customer_ds(base_dir: str = __default_dataset_base_dir) -> pl.LazyFrame:
    return __scan_parquet_ds(join(base_dir, "customer.parquet"))


def get_region_ds(base_dir: str = __default_dataset_base_dir) -> pl.LazyFrame:
    return __scan_parquet_ds(join(base_dir, "region.parquet"))


def get_nation_ds(base_dir: str = __default_dataset_base_dir) -> pl.LazyFrame:
    return __scan_parquet_ds(join(base_dir, "nation.parquet"))


def get_supplier_ds(base_dir: str = __default_dataset_base_dir) -> pl.LazyFrame:
    return __scan_parquet_ds(join(base_dir, "supplier.parquet"))


def get_part_ds(base_dir: str = __default_dataset_base_dir) -> pl.LazyFrame:
    return __scan_parquet_ds(join(base_dir, "part.parquet"))


def get_part_supp_ds(base_dir: str = __default_dataset_base_dir) -> pl.LazyFrame:
    return __scan_parquet_ds(join(base_dir, "partsupp.parquet"))


def run_query(q_num: int, lp: pl.LazyFrame):
    @linetimer(name=f"Overall execution of Query {q_num}", unit="s")
    def query():
        if SHOW_PLAN:
            print(lp.describe_optimized_plan())

        with CodeTimer(name=f"Get result of Query {q_num}", unit="s"):
            result = lp.collect()

        if SHOW_RESULTS:
            print(result)

        test_results(q_num, result)

    query()