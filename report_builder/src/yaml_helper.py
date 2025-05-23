import ruamel


def init_yaml():
    from ruamel.yaml import YAML
    yaml = YAML()
    yaml.default_flow_style = False
    yaml.allow_duplicate_keys = True
    yaml.indent(mapping=2, sequence=4, offset=2)
    return yaml


yaml_client = init_yaml()
