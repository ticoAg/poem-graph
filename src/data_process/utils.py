# -*- encoding: utf-8 -*-
"""
@Time    :   2024-09-12 00:36:41
@desc    :   
@Author  :   ticoAg
@Contact :   1627635056@qq.com
"""
import json
import uuid
from typing import Dict

from tqdm import tqdm


def get_uuid() -> str:
    return str(uuid.uuid4())


def save_csv(filename: str, data: list):
    with open(filename, "w", encoding="utf-8") as f:
        for node in tqdm(data, total=len(data), desc="Saving csv..."):
            f.write(",".join(node) + "\n")


def loadJS(filename: str) -> Dict:
    return json.load(open(filename, "r", encoding="utf-8"))


def dumpJS(data: Dict) -> str:
    return '"' + json.dumps(data, ensure_ascii=False).replace('"', '""') + '"'
