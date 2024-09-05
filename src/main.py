# -*- encoding: utf-8 -*-
"""
@Time    :   2024-09-05 21:04:30
@desc    :   
@Author  :   ticoAg
@Contact :   1627635056@qq.com
"""
import sys
from pathlib import Path

sys.path.append(Path(__file__).parents[1].absolute().__str__())
from src.data_process.pipeline import run


def main():
    run()


if __name__ == "__main__":
    main()
