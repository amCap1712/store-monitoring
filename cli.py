import pandas as pd
import typer

cli = typer.Typer()


@cli.command(name="import")
def import_(store_timezone_path: str, store_timings_path: str, store_observations_path: str):
    """
    Import store data and observations into the database from csv files.

    :param store_timezone_path: path to the csv file containing information about store timezones
    :param store_timings_path: path to the csv file containing information about the store's business hours
    :param store_observations_path: path to the csv file containing information about the store's status
    """
    store_timezone_df = pd.read_csv(store_timezone_path)
    store_timings_df = pd.read_csv(store_timings_path)
    store_observations_df = pd.read_csv(store_observations_path)

    store_timezone_df = store_timezone_df.rename(columns={"store_id": "id"})

    from app.database import engine
    with engine.connect() as conn:
        store_timezone_df.to_sql("store", con=conn, index=False, if_exists="append")
        store_timings_df.to_sql("store_timing", con=conn, index=False, if_exists="append")
        store_observations_df.to_sql("store_observation", con=conn, index=False, if_exists="append")


if __name__ == "__main__":
    cli()
