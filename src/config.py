import yaml


def load_config(config_path='/Users/ningyu/Desktop/Projets_data/ETL_Panthera_leo/config/config.yaml'):
    with open(config_path, 'r') as file:
        config = yaml.safe_load(file)
    return config
