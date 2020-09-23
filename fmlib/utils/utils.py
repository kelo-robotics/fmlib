import yaml
import json
from importlib_resources import open_text
from ropod.structs.status import ActionStatus, TaskStatus


def load_file_from_module(module, file_name):
    config_file = open_text(module, file_name)
    return config_file


# YAML config files
def load_yaml(yaml_file):
    data = yaml.safe_load(yaml_file)
    return data


def load_json(json_file):
    data = json.load(json_file)
    return data


def load_yaml_config_file(file_name):
    with open(file_name, 'r') as file_handle:
        config = load_yaml(file_handle)
    return config


task_status_names = {value: name for name, value in vars(TaskStatus).items() if name.isupper()}
action_status_names = {value: name for name, value in vars(ActionStatus).items() if name.isupper()}
