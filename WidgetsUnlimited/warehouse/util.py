import os

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
