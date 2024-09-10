from typing import List, Union

from pydantic import BaseModel, Field


class ShiJingModel(BaseModel):
    pid: str = Field(None, description="唯一标识标题拼接")
    title: str = Field(..., description="标题")
    chapter: str = Field(..., description="章节")
    section: str = Field(..., description="节")
    content: Union[str, List[str]] = Field(..., description="内容")

class CaocaoModel(BaseModel):
    author: str = Field("曹操", description="作者")
    title: str = Field(..., description="标题")
    paragraphs: str = Field(..., description="段落")

