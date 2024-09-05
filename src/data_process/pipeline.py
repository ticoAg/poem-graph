# -*- encoding: utf-8 -*-
"""
@Time    :   2024-09-05 21:52:50
@desc    :   
@Author  :   ticoAg
@Contact :   1627635056@qq.com
"""
import json
import time
from collections import defaultdict
from pathlib import Path
from typing import Dict

from tqdm import tqdm

from src.adaptor import *


class dataCollection:
    chapter_set = set()
    section_set = set()
    title_set = set()
    pid_set = set()
    chapter_section_rel = defaultdict(set)
    section_pid_rel = defaultdict(set)
    data: list
    idmap: Dict[str, str] = {}


def save_csv(filename: str, data: list):
    with open(filename, "w", encoding="utf-8") as f:
        for node in data:
            f.write(",".join(node) + "\n")


def _compose_title_node_type(dc: dataCollection):
    nodes = [[":ID", "name", "content", ":LABEL"]]
    for chapter in dc.chapter_set:
        nodes.append([0, chapter, "", "chapter"])
    for section in dc.section_set:
        nodes.append([0, section, "", "section"])
    for title in dc.title_set:
        nodes.append([0, title, "", "title"])
    for poem in dc.data:
        nodes.append([0, poem.pid, poem.content, "pid"])

    for i in range(len(nodes)):
        if i == 0:
            continue
        nodes[i][0] = str(i)
        dc.idmap[nodes[i][1]] = nodes[i][0]
    filename = "data/neo4j/node.csv"
    save_csv(filename, nodes)


def _compose_relation(dc):
    rel = [[":START_ID", ":END_ID", ":TYPE"]]
    for chapter, sections in dc.chapter_section_rel.items():
        for section in sections:
            id1, id2 = dc.idmap[section], dc.idmap[chapter]
            rel.append([id1, id2, "belongsTo"])

    for section, pids in dc.section_pid_rel.items():
        for pid in pids:
            id1, id2 = dc.idmap[pid], dc.idmap[section]
            rel.append([id1, id2, "belongsTo"])

    for poem in dc.data:
        id1, id2, id3 = dc.idmap[poem.pid], dc.idmap[poem.title], dc.idmap[poem.chapter]
        rel.append([id1, id2, "belongsTo"])
        rel.append([id1, id3, "belongsTo"])

    filename = "data/neo4j/relation.csv"
    save_csv(filename, rel)


def _ext_shijing():
    path = Path("data/chinese-poetry/诗经")
    filepaths = list(path.rglob("*.json"))
    all_data = []
    for filepath in filepaths:
        data = json.load(open(filepath, "r", encoding="utf-8"))
        all_data.extend(data)
    dc = dataCollection()
    for idx, item in enumerate(
        tqdm(all_data, total=len(all_data), desc="Extracting shijing...")
    ):
        poem = ShiJingModel(**item)
        poem.pid = "-".join([poem.chapter, poem.section, poem.title])
        poem.content = "".join(poem.content)
        dc.chapter_set.add(poem.chapter)
        dc.section_set.add(poem.section)
        dc.title_set.add(poem.title)
        dc.pid_set.add(poem.pid)
        dc.chapter_section_rel[poem.chapter].add(poem.section)
        dc.section_pid_rel[poem.section].add(poem.pid)
        all_data[idx] = poem
    dc.data = all_data
    _compose_title_node_type(dc)
    _compose_relation(dc)


def run():
    _ext_shijing()
