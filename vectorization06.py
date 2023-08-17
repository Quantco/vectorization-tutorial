import os

import pandas as pd
import polars as pl
import sqlalchemy as sa
import ibis
import pydiverse.transform as pdt
from pydiverse.pipedag import Flow, Stage, Table, materialize
from pydiverse.transform.core.verbs import (
    left_join,
    mutate,
    select, alias,
    build_query,
)
from pydiverse.transform.eager import PandasTableImpl
from pydiverse.transform.lazy import SQLTableImpl


@pdt.verb
def transmute(tbl, **kwargs):
    return tbl >> select() >> mutate(**kwargs)


@pdt.verb
def trim_all_str(tbl):
    for col in tbl:
        if col._.dtype == "str":
            tbl[col] = col.strip()
    return tbl


def pk(x: pdt.Table):
    # This is just a placeholder.
    # Ideally there would be a global function in pydiverse transform to
    # get the primary key (and another one to get the table / col name)
    return x.pk


def pk_match(x: pdt.Table, y: pdt.Table):
    return pk(x) == pk(y)


def pk_match_sa(x: sa.Table, y: sa.Table):
    # # we lost the primary_key already or duckdb cannot reflect it
    # cond = sa.literal(True)
    # for col in y.original.primary_key:
    #     if col not in x.c:
    #         cond &= x.c[col] == y.c[col]
    return x.c.pk == y.c.pk  # hack: assume primary key is always on column pk

def get_named_tables(tables: list[pdt.Table]) -> dict[str, pdt.Table]:
    return {tbl._impl.name: tbl for tbl in tables}


@materialize(version="1.0.0")
def read_input_data(src_dir="data/pipedag_example_data"):
    return [
        Table(pd.read_csv(os.path.join(src_dir, file)), name=file.removesuffix(".csv.gz"))
        for file in os.listdir(src_dir)
        if file.endswith(".csv.gz")
    ]


@materialize(input_type=SQLTableImpl, lazy=True, nout=3)
def clean(src_tbls: list[pdt.Table]):
    out_tbls = [tbl >> trim_all_str() for tbl in src_tbls]
    named_tbls = get_named_tables(out_tbls)
    a = named_tbls["a"]
    b = named_tbls["b"]
    c = named_tbls["c"]
    return a, b, c


@materialize(input_type=pd.DataFrame, version="1.0.0")
def task_pandas(a: pd.DataFrame, b: pd.DataFrame):
    return a.merge(b, on="pk", how="left").assign(x2=lambda df: df.x * df.x)


@materialize(input_type=pl.DataFrame, version="1.0.0")
def task_polars(a: pl.DataFrame, b: pl.DataFrame):
    x = pl.col("x")
    return a.join(b, on="pk", how="left").with_columns((x * x).alias("x2"))


@materialize(input_type=PandasTableImpl, version="1.0.0")
def task_transform_df(a: pdt.Table, b: pdt.Table):
    return (
        a >> left_join(b, pk_match(a, b)) >> mutate(x2=b.x * b.x)
        >> alias("transform_df")
    )


@materialize(input_type=SQLTableImpl, lazy=True)
def task_transform_sql(a: pdt.Table, b: pdt.Table):
    return (
        a >> left_join(b, pk_match(a, b)) >> mutate(x2=b.x * b.x)
        >> alias("transform_sql")
    )


@materialize(input_type=ibis.api.Table, lazy=True)
def task_ibis(a: ibis.api.Table, b: ibis.api.Table):
    return a.left_join(b, pk_match(a, b)).mutate(x2=b.x * b.x)


@materialize(input_type=sa.Table, lazy=True)
def task_sqlalchemy(a: sa.Table, b: sa.Table):
    return sa.select(
        *a.c,
        *[c for c in b.c if c.name not in a.c],
        (b.c.x * b.c.x).label("x2"),
    ).select_from(a.outerjoin(b, pk_match_sa(a, b)))


@materialize(input_type=sa.Table, lazy=True)
def task_sql(a: sa.Table, b: sa.Table):
    return sa.text(f"""
        SELECT 
            a.*, 
            b.*, 
            b.x * b.x AS x2
        FROM {a.original.schema}.{a.name} AS a
        LEFT JOIN {b.original.schema}.{b.name} AS b
        ON a.pk = b.pk
    """)


@materialize(input_type=ibis.api.Table, version="1.0.0")
def check_x2_sum(tbls: list[ibis.api.Table]):
    all_x2_sum = None
    for tbl in tbls:
        x2_sum = tbl.x2.sum().to_pandas()
        if all_x2_sum is None:
            all_x2_sum = x2_sum
        else:
            assert x2_sum == all_x2_sum


def get_pipeline():
    tasks = [task_pandas, task_polars, task_transform_df, task_transform_sql,
             task_ibis, task_sqlalchemy, task_sql]
    with Flow("flow") as flow:
        with Stage("x1_raw_input"):
            raw_tbls = read_input_data()

        with Stage("x2_clean_input"):
            a, b, c = clean(raw_tbls)

        with Stage("x3_transformed_data"):
            out_tbls = [task(a, b) for task in tasks]

        with Stage("x4_check"):
            check_x2_sum(out_tbls)

    return flow


if __name__ == "__main__":
    import logging
    from pydiverse.pipedag.util.structlog import setup_logging

    setup_logging(log_level=logging.INFO)

    flow = get_pipeline()
    result = flow.run()
    assert result.successful