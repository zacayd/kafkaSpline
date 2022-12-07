import pandas as pd
from tabulate import tabulate
class NotADictionary(Exception):
    pass


def flatten(obj: dict) -> dict:
    """
    This function takes a dictionary with arbitrary levels of nested
    lists and dictionaries and flattens it.
    Raises NotADictionary if the input is invalid.
    """

    if not isinstance(obj, dict):
        raise NotADictionary

    flattened_data = {}

    def flatten_json(json_data, name=''):
        if type(json_data) is dict:
            for key in json_data:
                if not bool(json_data[key]):  # handle empty dict values
                    flattened_data[name + key + '.'[:-1]] = json_data[key]
                else:
                    flatten_json(json_data[key], name + key + '.')
        elif type(json_data) is list:
            i = 0
            for key in json_data:
                flatten_json(key, name + str(i) + '.')
                i += 1
        else:
            flattened_data[name[:-1]] = json_data

    try:
        flatten_json(obj)

    except Exception as e:
        raise e

    return flattened_data


if __name__ == '__main__':
    wc_data = {"name": "Cap'n Chuck",
               "aliases": ["Chuck Force 1", "Whistlepig"],
               "physical": {"height_in": 27, "weight_lb": 17},
               "wood_chucked_lbs": 2219}
    dx=flatten(wc_data)

    result = pd.json_normalize(dx )
    print(tabulate(result, headers='keys', tablefmt='psql'))
