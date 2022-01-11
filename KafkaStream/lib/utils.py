import json


def read_json_file(input_data):
    """Read data source file. File is in json format"""
    with open(input_data, 'r') as src:
        data = json.load(src)
    return data



def dict_to_binary(json_dict):
    return json.dumps(json_dict).encode("utf-8")


