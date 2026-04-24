import pickle as pk


def save_pickle(filename, object):
    with open(filename, "wb") as f:
        pk.dump(object, f)
