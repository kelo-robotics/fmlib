import json
import os
import re

import yaml
from importlib_resources import open_text
from ropod.structs.status import ActionStatus, TaskStatus


def load_file_from_module(module, file_name):
    config_file = open_text(module, file_name)
    return config_file


def load_yaml(yaml_file, tag='!ENV'):
    """ Loads YAML config files and resolves any environment variables
    The environment variables must have !ENV before them and be in this format
    to be parsed: ${VAR_NAME}
    Example:
        map_name: !ENV ${MAP}

    Based on: https://medium.com/swlh/python-yaml-configuration-with-environment-variables-parsing-77930f4273ac
    """
    # pattern for global vars: look for ${word}
    pattern = re.compile('.*?\${(\w+)}.*?')
    loader = yaml.SafeLoader
    loader.add_implicit_resolver(tag, pattern, None)

    def constructor_env_variables(loader, node):
        """
        Extracts the environment variable from the node's value
        Args:
            loader (yaml.Loader): the yaml loader
            node: the current node in the yaml
        Return: the parsed string that contains the value of the environment variable
        """
        value = loader.construct_scalar(node)
        match = pattern.findall(value)  # to find all env variables in line
        if match:
            full_value = value
            for g in match:
                full_value = full_value.replace(
                    f'${{{g}}}', os.environ.get(g, g)
                )
            return full_value
        return value

    loader.add_constructor(tag, constructor_env_variables)

    data = yaml.load(yaml_file, Loader=loader)
    return data


def load_json(json_file):
    data = json.load(json_file)
    return data


def load_yaml_config_file(file_name):
    try:
        with open(file_name, 'r') as file_handle:
            config = load_yaml(file_handle)
        return config
    except FileNotFoundError as error:
        raise


def log_time_to_file(text, file_name="timeit.txt"):
    with open(file_name, "a") as f:
        f.write(text)


task_status_names = {value: name for name, value in vars(TaskStatus).items() if name.isupper()}
action_status_names = {value: name for name, value in vars(ActionStatus).items() if name.isupper()}
