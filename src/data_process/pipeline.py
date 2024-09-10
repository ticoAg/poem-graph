# -*- encoding: utf-8 -*-
"""
@Time    :   2024-09-05 21:52:50
@desc    :   
@Author  :   ticoAg
@Contact :   1627635056@qq.com
"""
import json
import uuid
from collections import defaultdict
from copy import deepcopy
from pathlib import Path
from typing import Dict, List

from loguru import logger
from tqdm import tqdm

from adaptor.shijing import ShiJingModel
from src.adaptor import *


class dataCollection:
    chapter_set = set()
    section_set = set()
    title_set = set()
    author_set = set()
    pid_set = set()
    chapter_section_rel = defaultdict(set)
    section_pid_rel = defaultdict(set)
    data: List = []


def save_csv(filename: str, data: list):
    with open(filename, "w", encoding="utf-8") as f:
        for node in tqdm(data, total=len(data), desc="Saving csv..."):
            f.write(",".join(node) + "\n")


def loadJS(filename: str) -> Dict:
    return json.load(open(filename, "r", encoding="utf-8"))


def dumpJS(data: Dict) -> str:
    return '"' + json.dumps(data, ensure_ascii=False).replace('"', '""') + '"'


def get_uuid() -> str:
    return str(uuid.uuid4())


class Extractor:
    root_path: Path = Path("data/chinese-poetry")
    node_path: Path = Path("data/neo4j/node.csv")
    rel_path: Path = Path("data/neo4j/rel.csv")
    nodes: List = [[":ID", "name", "内容", "章", "节", "题目", "作者", ":LABEL"]]
    rel = [[":START_ID", ":END_ID", ":TYPE", "desc"]]

    idmap: Dict[str, str] = {}

    def __init__(self) -> None:
        self.dc: dataCollection = deepcopy(dataCollection())
        self.dcbak = deepcopy(dataCollection())

    def reset_dc(self):
        self.dc.author_set = set()
        self.dc.chapter_section_rel = defaultdict(set)
        self.dc.chapter_set = set()
        self.dc.data = []
        self.dc.pid_set = set()
        self.dc.section_pid_rel = defaultdict(set)
        self.dc.section_set = set()
        self.dc.title_set = set()

    def update_node(
        self,
        name: str = "",
        content: str = "",
        chapter: str = "",
        section: str = "",
        title: str = "",
        author: str = "",
        label: str = "",
    ):
        if self.idmap.get(name):
            logger.warning(f"{name} has been added!")
        else:
            self.nodes.append(
                [get_uuid(), name, content, chapter, section, title, author, label]
            )
            self.idmap[name] = self.nodes[-1][0]

    def update_rel(self, head: str, tail: str, _type: str, desc: str):
        self.rel.append([self.idmap[head], self.idmap[tail], _type, desc])

    def export(self):
        logger.info("Start to export csv...")
        save_csv(self.node_path, self.nodes)
        save_csv(self.rel_path, self.rel)
        logger.info("Export csv success!")

    def _compose_node_shijing(self):
        for chapter in self.dc.chapter_set:
            self.update_node(name=chapter, label="chapter")
        for section in self.dc.section_set:
            self.update_node(name=section, label="section")
        for title in self.dc.title_set:
            self.update_node(name=title, label="title")
        for poem in self.dc.data:
            self.update_node(
                name=poem.pid,
                content=poem.content,
                label="诗经",
                section=poem.section,
                chapter=poem.chapter,
            )

    def _compose_rel_shijing(self):
        """构建关系"""
        for chapter, sections in self.dc.chapter_section_rel.items():
            for section in sections:
                self.update_rel(chapter, section, "belongsTo", "chapter->section")

        for section, pids in self.dc.section_pid_rel.items():
            for pid in pids:
                self.update_rel(pid, section, "belongsTo", "section->pid")

        for poem in self.dc.data:
            self.update_rel(poem.pid, poem.title, "belongsTo", "pid->title")
            self.update_rel(poem.pid, poem.chapter, "belongsTo", "pid->chapter")

    def _ext_shi_jing(self):
        path = self.root_path / "诗经"
        filepaths = list(path.rglob("*.json"))
        all_data = [i for filepath in filepaths for i in loadJS(filepath)]
        for idx, item in enumerate(
            tqdm(all_data, total=len(all_data), desc="Extracting shijing...")
        ):
            poem = ShiJingModel(**item)
            poem.pid = "-".join([poem.chapter, poem.section, poem.title])
            poem.content = dumpJS(poem.content)
            self.dc.chapter_set.add(poem.chapter)
            self.dc.section_set.add(poem.section)
            self.dc.title_set.add(poem.title)
            self.dc.pid_set.add(poem.pid)
            self.dc.chapter_section_rel[poem.chapter].add(poem.section)
            self.dc.section_pid_rel[poem.section].add(poem.pid)
            all_data[idx] = poem
        self.dc.data = all_data
        self._compose_node_shijing()
        self._compose_rel_shijing()

    def _compose_node_caocao(self):
        for chapter in self.dc.chapter_set:
            self.update_node(name=chapter, label="chapter")
        for section in self.dc.section_set:
            self.update_node(name=section, label="section")
        for title in self.dc.title_set:
            self.update_node(name=title, label="title")
        for poem in self.dc.data:
            self.update_node(name=poem.pid, content=poem.content, label="诗经")

    def _ext_cao_cao(self):
        path = self.root_path / "曹操诗集"
        filepaths = list(path.rglob("*.json"))
        all_data = [i for filepath in filepaths for i in loadJS(filepath)]
        self.reset_dc()
        for idx, item in enumerate(
            tqdm(all_data, total=len(all_data), desc="Extracting shijing...")
        ):
            item["paragraphs"] = dumpJS(item["paragraphs"])
            self.dc.data.append(item)
        self.update_node(name="曹操", label="作者")
        for poem in self.dc.data:
            self.update_node(
                name=f"{poem['title']}(曹操)",
                title=poem["title"],
                content=poem["paragraphs"],
                label="诗",
                author="曹操",
            )

    def _ext_lun_yu(self):
        path = self.root_path / "论语"
        filepaths = list(path.rglob("*.json"))
        all_data = [i for filepath in filepaths for i in loadJS(filepath)]
        self.reset_dc()
        for idx, item in enumerate(
            tqdm(all_data, total=len(all_data), desc="Extracting 论语...")
        ):
            item["paragraphs"] = dumpJS(item["paragraphs"])
            self.dc.data.append(item)
        for node in self.dc.data:
            self.update_node(
                name=f"论语-{node['chapter']}",
                content=node["paragraphs"],
                label="论语",
                title=node["chapter"],
            )
    def _ext_meng_xue(self):
        path = self.root_path / "蒙学"
        # TODO

    def _ext_na_lan_xing_de(self):
        path = self.root_path / "纳兰性德"
        filepaths = list(path.rglob("*.json"))
        all_data = [i for filepath in filepaths for i in loadJS(filepath)]
        self.reset_dc()
        for idx, item in enumerate(
            tqdm(all_data, total=len(all_data), desc="Extracting shijing...")
        ):
            item["para"] = dumpJS(item["para"])
            self.dc.data.append(item)
        self.update_node(name="纳兰性德", label="作者")
        for poem in self.dc.data:
            self.update_node(
                name=f"{poem['title']}(纳兰性德)",
                title=poem["title"],
                content=poem["para"],
                label="诗",
                author=poem["author"],
            )
        
    def _ext_chu_ci(self):
        path = self.root_path / "楚辞"
        filepaths = list(path.rglob("*.json"))
        all_data = [i for filepath in filepaths for i in loadJS(filepath)]
        self.reset_dc()
        for idx, item in enumerate(
            tqdm(all_data, total=len(all_data), desc="Extracting shijing...")
        ):
            item["content"] = dumpJS(item["content"])
            self.dc.title_set.add(item["title"])
            self.dc.section_set.add(item["section"])
            self.dc.author_set.add(item["author"])
            self.dc.data.append(item)

        for node in self.dc.section_set:
            self.update_node(name=node, label="section")
        for node in self.dc.title_set:
            self.update_node(name=node, label="title")
        for node in self.dc.author_set:
            self.update_node(name=node, label="author")
        for node in self.dc.data:
            unique_id = "-".join([node["section"], node["title"], node["author"]])
            self.update_node(
                name=unique_id,
                title=node["title"],
                section=node["section"],
                content=node["content"],
                label="楚辞",
                author=node["author"],
            )
            self.update_rel(
                node["title"], node["section"], "belongsTo", "title->section"
            )


def run():
    extractor = Extractor()
    extractor._ext_shi_jing()
    extractor._ext_cao_cao()
    extractor._ext_chu_ci()
    extractor._ext_lun_yu()
    extractor._ext_na_lan_xing_de()
    extractor.export()
