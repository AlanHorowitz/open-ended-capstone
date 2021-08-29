import os

import pandas as pd

STAGE_DIRECTORY_PREFIX = "/tmp/warehouse/stage/batch"


def clean_stage_dir(batch_id):

    out_path = STAGE_DIRECTORY_PREFIX + str(batch_id)
    if os.path.exists(out_path):
        for f in os.listdir(out_path):
            os.remove(os.path.join(out_path, f))
    else:
        os.mkdir(out_path)


def get_stage_dir(batch_id):
    return STAGE_DIRECTORY_PREFIX + str(batch_id)


def get_new_keys(incremental_keys: pd.Series, dimension: pd.DataFrame, key=None) -> pd.Index:

    merged = pd.merge(incremental_keys, dimension, on=key, how="left")
    new_mask = merged["surrogate_key"].isna()
    new_keys = pd.Index(merged[new_mask][key])
    return new_keys