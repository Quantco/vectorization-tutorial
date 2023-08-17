import pandas as pd
import polars as pl
import sqlalchemy as sa
import ibis
from ibis import _ as col
import pydiverse.transform as pdt
from pydiverse.pipedag import Flow, Stage, Table, materialize
from pydiverse.transform import λ
from pydiverse.transform.core.functions import row_number
from pydiverse.transform.core.verbs import (
    mutate, alias, arrange, build_query,
)
from pydiverse.transform.eager import PandasTableImpl
from pydiverse.transform.lazy import SQLTableImpl


@materialize(version="1.0.0")
def read_input_data():
    titanic = pd.read_csv(
        'https://raw.githubusercontent.com/mwaskom/seaborn-data/master/titanic.csv'
    )
    return Table(titanic, name="titanic")


@materialize(input_type=pd.DataFrame, version="win1.0.0")
def task_pandas(titanic: pd.DataFrame):
    titanic.sort_values("fare", inplace=True)
    return (
        titanic
        .assign(idx=range(len(titanic)), diff_price=titanic.fare.diff())
    )


@materialize(input_type=pl.DataFrame, version="win1.0.0")
def task_polars(titanic: pl.DataFrame):
    return (
        titanic.sort("fare")
        .with_columns(idx=pl.Series(range(len(titanic))), diff_price=pl.col("fare").diff())
    )


@materialize(input_type=PandasTableImpl, version="win1.0.1")
def task_transform_df(titanic: pdt.Table):
    return (
        titanic
        >> mutate(
            idx=row_number(arrange=[λ.fare]),
            diff_price = λ.fare.shift(-1, arrange=[λ.fare])-λ.fare
        )
        >> arrange(λ.fare)
        >> alias("transform_df")
    )


@materialize(input_type=SQLTableImpl, lazy=True)
def task_transform_sql(titanic: pdt.Table):
    return (
        titanic
        >> mutate(
            idx=row_number(arrange=[λ.fare]),
            diff_price = λ.fare.shift(-1, arrange=[λ.fare])-λ.fare
        )
        >> arrange(λ.fare)
        >> alias("transform_sql")
    )


@materialize(input_type=ibis.api.Table, lazy=True)
def task_ibis(titanic: ibis.api.Table):
    w = ibis.window(order_by=col.fare)
    return (
        titanic
        .mutate(idx=ibis.row_number().over(w), diff_price=col.fare.lag(-1).over(w) - col.fare)
        .order_by(col.fare)
    )


@materialize(input_type=sa.Table, lazy=True)
def task_sqlalchemy(titanic: sa.Table):
    return sa.select(
        titanic,
        sa.func.row_number().over(order_by=titanic.c.fare).label("idx"),
        (sa.func.lead(titanic.c.fare, 1, None).over(order_by=titanic.c.fare)
            - titanic.c.fare).label("diff_price"),
    ).select_from(titanic).order_by(titanic.c.fare)


@materialize(input_type=sa.Table, lazy=True)
def task_sql(titanic: sa.Table):
    return sa.text(f"""
        SELECT tt.*,
               ROW_NUMBER()      OVER (ORDER BY tt.fare ASC NULLS LAST) AS idx,
               LEAD(tt.fare, 1, NULL) OVER (ORDER BY tt.fare ASC NULLS LAST)
                - tt.fare AS diff_price
        FROM {titanic.original.schema}.{titanic.name} AS tt
        ORDER BY tt.fare
    """)


@materialize(input_type=pd.DataFrame)
def print_tables(tbls: list[pd.DataFrame]):
    from matplotlib import pyplot as plt
    fix, axs = plt.subplots(nrows=4, ncols=2)
    for tbl, ax in zip(tbls, axs.flatten()):
        tbl.sort_values("idx", inplace=True)
        print(f"\n\n{tbl}")
        ax.plot(tbl.idx, tbl.diff_price.fillna(0))
        # limit y axis to 30
        ax.set_ylim(0, 30)
    plt.show()


def get_pipeline():
    tasks = [task_pandas, task_polars, task_transform_df, task_transform_sql,
             task_ibis, task_sqlalchemy, task_sql]
    with Flow("flow") as flow:
        with Stage("t1_raw_input"):
            titanic = read_input_data()

        with Stage("t2_transformed_data"):
            out_tbls = [task(titanic) for task in tasks]
            print_tables(out_tbls)

    return flow


if __name__ == "__main__":
    import logging
    from pydiverse.pipedag.util.structlog import setup_logging

    setup_logging(log_level=logging.INFO)

    flow = get_pipeline()
    result = flow.run()
    assert result.successful
    flow.run(flow["t2_transformed_data"]["task_transform_sql"])