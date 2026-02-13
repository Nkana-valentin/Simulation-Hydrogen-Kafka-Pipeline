# Load device registry
import os
import json


with open(os.path.join(os.path.dirname(__file__), "device_registry.json"), "r") as f:
    REGISTRY = json.load(f)