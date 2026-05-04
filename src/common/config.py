import yaml
from pathlib import Path

def load_config(config_path ="configs/app_config.yaml"):

    project_root = Path(__file__).resolve().parents[2]
    full_path = project_root / config_path
    with open(full_path, "r") as file:
        config = yaml.safe_load(file)

    for key,val in config['paths'].items():
        config['paths'][key] = str(project_root / val)

    return config