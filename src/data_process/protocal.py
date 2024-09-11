# -*- encoding: utf-8 -*-
"""
@Time    :   2024-09-11 21:20:32
@desc    :   定义各种元数据
@Author  :   ticoAg
@Contact :   1627635056@qq.com
"""


import uuid
from typing import Literal

from pydantic import BaseModel, Field
from collections import defaultdict
from typing import List

from data_process.utils import get_uuid

class dataCollection:
    chapter_set = set()
    section_set = set()
    title_set = set()
    author_set = set()
    pid_set = set()
    chapter_section_rel = defaultdict(set)
    section_pid_rel = defaultdict(set)
    data: List = []


class Node(BaseModel):
    type: str = "node"
    id: str = Field(get_uuid(), description="Node唯一标识")
    name: str = Field(..., description="节点名称")
    label: str = Field(..., description="节点类型")


class TypeNode(Node):
    type: str = "type_node"
    # id: str = Field(get_uuid(), description="Node唯一标识")
    # name: str = Field(..., description="节点名称")
    label: Literal["章", "节", "标题"] = Field(..., description="标题类型")


class PoemNode(Node):
    # [[":ID", "name", "内容", "章", "节", "题目", "作者", ":LABEL"]]
    type: str = "poem_node"
    # id: str = Field(get_uuid(), description="Node唯一标识")
    # name: str = Field(..., description="节点名称")
    content: str = Field(..., description="主要内容")
    chapter: str = Field(..., description="章")
    section: str = Field(..., description="节")
    title: str = Field(..., description="题目")
    author: str = Field("", description="作者")
    label: Literal["诗", "诗经", "楚辞", "论语"] = Field(..., description="诗词类型")


class AuthorNode(Node):
    # id: str = Field(get_uuid(), description="Node唯一标识")
    name: str = Field(..., description="节点名称")
    label: str = "作者"


class DictToObject:
    def __init__(self, entries: dict = {}):
        for k, v in entries.items():
            if isinstance(v, dict):
                self.__dict__[k] = DictToObject(v)
            else:
                self.__dict__[k] = v
