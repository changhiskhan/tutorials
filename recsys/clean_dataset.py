"""

Unfortunately, the dataset is not properly formatted. We simply pass it through pandas
to get a clean CSV we can import in the Flow using duckdb.

"""

import lance
import pandas as pd
import pyarrow as pa

import random


def clean_dataset():
    df_playlist = pd.read_csv(
        'spotify_dataset.csv', 
        on_bad_lines='skip', 
        # if you want to get a smaller dataset, you can subsample at the source here
        # skiprows=lambda i: i>0 and random.random() > 0.50
        )
    # clean up the col names
    df_playlist.columns = df_playlist.columns.str.replace('"', '')
    df_playlist.columns = df_playlist.columns.str.replace('name', '')
    df_playlist.columns = df_playlist.columns.str.replace(' ', '')
    # add a row id
    df_playlist.insert(0, 'row_id', range(0, len(df_playlist)))
    # show the df
    print(df_playlist.head())
    # print the final length for the df before writing to disk
    print("Total rows: {}".format(len(df_playlist)))
    # dump to parquet (better than csv for duckdb)

    df_playlist.to_parquet('cleaned_spotify_dataset.parquet')

    # TODO we can make this shorter and add a pandas accessor like df.lance.write("dataset.lance")
    # df_playlist['playlist'] = df_playlist['playlist'].astype('category')
    # df_playlist['track'] = df_playlist['track'].astype('category')
    # df_playlist['artist'] = df_playlist['artist'].astype('category')
    # df_playlist['user_id'] = df_playlist['user_id'].astype('category')
    tbl = pa.Table.from_pandas(df_playlist)
    lance.write_dataset(tbl, "cleaned_spotify_dataset.lance")

    print("All done\n\nSee you, space cowboy\n")

    return


if __name__ == "__main__":
    clean_dataset()