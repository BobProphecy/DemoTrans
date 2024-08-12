from prophecy.config import ConfigBase


class Config(ConfigBase):

    def __init__(self, OP_AGG_RPT_FILE: str=None, **kwargs):
        self.spark = None
        self.update(OP_AGG_RPT_FILE)

    def update(self, OP_AGG_RPT_FILE: str="", **kwargs):
        prophecy_spark = self.spark
        self.OP_AGG_RPT_FILE = OP_AGG_RPT_FILE
        pass
