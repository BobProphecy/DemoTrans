from prophecy.config import ConfigBase


class Config(ConfigBase):

    def __init__(
            self,
            SOURCE_DIR: str=None,
            STG_DIR: str=None,
            HASH_DIR: str=None,
            FMT_DIR: str=None,
            BANK_NUM: str=None,
            DATA_DATE: str=None,
            DATE: str=None,
            **kwargs
    ):
        self.spark = None
        self.update(SOURCE_DIR, STG_DIR, HASH_DIR, FMT_DIR, BANK_NUM, DATA_DATE, DATE)

    def update(
            self,
            SOURCE_DIR: str="/nas_pp/dev/dw/data/conv_253",
            STG_DIR: str="/nas_pp/dev/dw/stg",
            HASH_DIR: str="/nas_pp/dev/dw/hash",
            FMT_DIR: str="/nas_pp/dev/dw/fmt",
            BANK_NUM: str="101",
            DATA_DATE: str="20060806",
            DATE: str="1724085510696",
            **kwargs
    ):
        prophecy_spark = self.spark
        self.SOURCE_DIR = SOURCE_DIR
        self.STG_DIR = STG_DIR
        self.HASH_DIR = HASH_DIR
        self.FMT_DIR = FMT_DIR
        self.BANK_NUM = BANK_NUM
        self.DATA_DATE = DATA_DATE
        self.DATE = DATE
        pass
