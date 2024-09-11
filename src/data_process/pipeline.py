# -*- encoding: utf-8 -*-
"""
@Time    :   2024-09-05 21:52:50
@desc    :   
@Author  :   ticoAg
@Contact :   1627635056@qq.com
"""
import json
from collections import defaultdict
from copy import deepcopy
from pathlib import Path
from typing import Dict, List

from loguru import logger
from tqdm import tqdm

from data_process.protocal import DictToObject, Node, PoemNode, TypeNode, dataCollection
from data_process.utils import dumpJS, loadJS, save_csv


class Extractor:
    root_path: Path = Path("data/chinese-poetry")
    save_node_path: Path = Path("data/neo4j")
    rel_path: Path = Path("data/neo4j/rel.csv")
    nodes_map: Dict = {
        "type_node": [[":ID", "name", ":LABEL"]],
        "poem_node": [[":ID", "name", "内容", "章", "节", "题目", "作者", ":LABEL"]],
    }
    rel = [[":START_ID", ":END_ID", ":TYPE", "desc"]]

    idmap = defaultdict(dict)

    def __init__(self) -> None:
        self.dc: dataCollection = deepcopy(dataCollection())
        self.dcbak = deepcopy(dataCollection())

    def find_id(self, type, name):
        return self.idmap[type].get(name)

    def reset_dc(self):
        self.dc.author_set = set()
        self.dc.chapter_section_rel = defaultdict(set)
        self.dc.chapter_set = set()
        self.dc.data = []
        self.dc.pid_set = set()
        self.dc.section_pid_rel = defaultdict(set)
        self.dc.section_set = set()
        self.dc.title_set = set()

    def update_node(self, node: Node):
        if self.idmap[node.label].get(node.name):
            logger.warning(f"{node.label}-{node.name} has been added!")
        else:
            self.nodes_map[node.type].append(node.__dict__)
            self.idmap[node.label][node.name] = node.id

    # def update_rel(self, head: str, tail: str, _type: str, desc: str):
    #     self.rel.append([self.idmap[head], self.idmap[tail], _type, desc])
    def update_rel(self, hid: str, tid: str, _type: str, desc: str):
        self.rel.append([hid, tid, _type, desc])

    def export(self):
        logger.info("Start to export csv...")
        for node_type, nodes in self.nodes_map.items():
            if node_type == "type_node":
                data = [nodes[0]] + [
                    [n["id"], n["name"], n["label"]] for n in nodes[1:]
                ]
            elif node_type == "poem_node":
                data = [nodes[0]] + [
                    [
                        n["id"],
                        n["name"],
                        n["content"],
                        n["chapter"],
                        n["section"],
                        n["title"],
                        n["author"],
                        n["label"],
                    ]
                    for n in nodes[1:]
                ]
            savepath = self.save_node_path / f"{node_type}.csv"
            save_csv(savepath, data)

        save_csv(self.rel_path, self.rel)
        logger.info("Export csv success!")

    def _ext_shi_jing(self):
        path = self.root_path / "诗经"
        filepaths = list(path.rglob("*.json"))
        all_data = [DictToObject(i) for filepath in filepaths for i in loadJS(filepath)]
        chapter_set, section_set, title_set = set(), set(), set()
        chapter_section_rel, section_pid_rel = defaultdict(set), defaultdict(set)

        poem_nodes = []
        for idx, poem in enumerate(
            tqdm(all_data, total=len(all_data), desc="Extracting shijing...")
        ):
            poem.pid = "-".join([poem.chapter, poem.section, poem.title])
            poem.content = dumpJS(poem.content)
            chapter_set.add(poem.chapter)
            section_set.add(poem.section)
            title_set.add(poem.title)
            chapter_section_rel[poem.chapter].add(poem.section)
            section_pid_rel[poem.section].add(poem.pid)
            poem_nodes.append(PoemNode(**poem.__dict__, name=poem.pid, label="诗经"))
        for chapter in chapter_set:
            self.update_node(TypeNode(name=chapter, label="章"))
        for section in section_set:
            self.update_node(TypeNode(name=section, label="节"))
        for title in title_set:
            self.update_node(TypeNode(name=title, label="标题"))
        for poem_node in poem_nodes:
            self.update_node(poem_node)

        for chapter, sections in chapter_section_rel.items():
            for section in sections:
                self.update_rel(
                    self.find_id("章", chapter),
                    self.find_id("节", section),
                    "belongsTo",
                    "chapter->section",
                )

        for section, pids in self.dc.section_pid_rel.items():
            for pid in pids:
                self.update_rel(
                    self.find_id("诗经", pid),
                    self.find_id("节", section),
                    "belongsTo",
                    "section->pid",
                )

        for poem in poem_nodes:
            self.update_rel(
                self.find_id("诗经", poem.name),
                self.find_id("标题", poem.title),
                "belongsTo",
                "pid->title",
            )
            self.update_rel(
                self.find_id("诗经", poem.name),
                self.find_id("节", poem.section),
                "belongsTo",
                "pid->section",
            )
            self.update_rel(
                self.find_id("诗经", poem.name),
                self.find_id("章", poem.chapter),
                "belongsTo",
                "pid->chapter",
            )

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

    def _ext_quan_tang_shi(self):
        path = self.root_path / "全唐诗"
        filepaths = list(path.rglob("poet.*.json"))
        all_data = [i for filepath in filepaths for i in loadJS(filepath)]
        self.reset_dc()
        for idx, item in enumerate(
            tqdm(all_data, total=len(all_data), desc="Extracting shijing...")
        ):
            item["content"] = dumpJS(item["content"])
            self.dc.data.append(item)
        for node in self.dc.data:
            unique_id = "-".join([node["author"], node["title"]])
            self.update_node(
                name=unique_id,
            )


def run():
    extractor = Extractor()
    extractor._ext_shi_jing()
    # extractor._ext_cao_cao()
    # extractor._ext_chu_ci()
    # extractor._ext_lun_yu()
    # extractor._ext_na_lan_xing_de()
    extractor.export()
