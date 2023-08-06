import pandas as pd
import numpy as np
from pydiverse.pipedag.materialize import Blob, Table, materialize
import lightgbm
from sklearn.metrics import mean_squared_error
from sklearn.model_selection import train_test_split
import math
from pydiverse.pipedag.core import Flow, PipedagConfig, Stage


@materialize(version="1.0.0")
def read_input_data():
    data_df = pd.read_csv('data/taxi_data/train.csv.gz')
    return Table(data_df, name="input_data")


@materialize(version="1.0.0", input_type=pd.DataFrame)
def feature_trip_distance(df: pd.DataFrame):
    start_lat = np.radians(df["pickup_latitude"])
    start_lng = np.radians(df["pickup_longitude"])
    dest_lat = np.radians(df["dropoff_latitude"])
    dest_lng = np.radians(df["dropoff_longitude"])

    d = (
        np.sin(dest_lat / 2 - start_lat / 2) ** 2
        + np.cos(start_lat)
        * np.cos(dest_lat)
        * np.sin(dest_lng / 2 - start_lng / 2) ** 2
    )

    return Table(pd.DataFrame(
        dict(
            id=df["id"],
            # 6,371 km is the earth radius
            haversine_distance = 2 * 6371 * np.arcsin(np.sqrt(d))
        )
    ), name="trip_distance")

@materialize(version="1.0.0", input_type=pd.DataFrame)
def feature_split_pickup_datetime(df: pd.DataFrame):
    tpep_pickup_datetime = pd.to_datetime(df["pickup_datetime"])

    return Table(pd.DataFrame(
        dict(
            id=df["id"],
            pickup_dayofweek=tpep_pickup_datetime.dt.dayofweek,
            pickup_hour=tpep_pickup_datetime.dt.hour,
            pickup_minute=tpep_pickup_datetime.dt.minute,
        )
    ), name="pickup_datetime")


@materialize(nout=2, version="1.0.0", input_type=pd.DataFrame)
def get_feature_df(df: pd.DataFrame, features: list[pd.DataFrame], target_col="trip_duration"):
    final_df = df[["id"] + [col for col in df.columns if col != target_col and df[col].dtype in (int, float, bool)]]
    for feature_df in features:
        final_df = final_df.merge(feature_df, on="id")
    return (
        Table(final_df, name="features"),
        Table(df[["id", target_col]], name="target"),
    )


def combine_features(data_df):
    features = [
        feature_trip_distance(data_df),
        feature_split_pickup_datetime(data_df),
    ]
    return get_feature_df(data_df, features)


@materialize(nout=4, version="1.0.0", input_type=pd.DataFrame)
def split_train_test(features_df: pd.DataFrame, target_df: pd.DataFrame):
    features_df.sort_values("id", inplace=True)
    target_df.sort_values("id", inplace=True)
    (
        features_train,
        features_test,
        target_train,
        target_test,
    ) = train_test_split(features_df, target_df, test_size=0.1)
    return (
        Table(features_train, name="features_train"),
        Table(features_test, name="features_test"),
        Table(target_train, name="target_train"),
        Table(target_test, name="target_test"),
    )


@materialize(version="1.0.0", input_type=pd.DataFrame)
def train_model(features_train: pd.DataFrame, target_train: pd.DataFrame):
    features_train.sort_values("id", inplace=True)
    target_train.sort_values("id", inplace=True)
    del features_train["id"]
    del target_train["id"]
    model = lightgbm.LGBMRegressor(objective="regression_l1")
    model.fit(features_train, target_train)
    return Blob(model, name="model")


@materialize(version="1.0.0", input_type=pd.DataFrame)
def evaluate_model(features_test: pd.DataFrame, target_test: pd.DataFrame, model: lightgbm.LGBMRegressor):
    features_test.sort_values("id", inplace=True)
    target_test.sort_values("id", inplace=True)
    del features_test["id"]
    del target_test["id"]
    predicted = model.predict(features_test)
    print(model.score(features_test, target_test))
    print(math.sqrt(mean_squared_error(target_test, predicted)))
    lightgbm.plot_importance(model)


def get_pipeline():
    with Flow("vectorization") as flow:
        with Stage("01_raw_input"):
            data_df = read_input_data()
        with Stage("02_features"):
            features_df, target_df = combine_features(data_df)
            (
                features_train,
                features_test,
                target_train,
                target_test,
            ) = split_train_test(features_df, target_df)
        with Stage("03_model"):
            model = train_model(features_train, target_train)
        with Stage("04_evaluation"):
            evaluate_model(features_test, target_test, model)
    return flow


if __name__ == "__main__":
    import logging
    from pydiverse.pipedag.util.structlog import setup_logging

    setup_logging(log_level=logging.INFO)

    flow = get_pipeline()
    result = flow.run()
    assert result.successful