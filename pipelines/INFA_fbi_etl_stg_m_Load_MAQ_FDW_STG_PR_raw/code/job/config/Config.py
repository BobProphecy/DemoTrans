from prophecy.config import ConfigBase


class Config(ConfigBase):

    def __init__(self, PROC_RUN_ID: str=None, FEED_TYPE: str=None, MAQ_FILE_NAME: str=None, **kwargs):
        self.spark = None
        self.update(PROC_RUN_ID, FEED_TYPE, MAQ_FILE_NAME)

    def update(self, PROC_RUN_ID: str="", FEED_TYPE: str="", MAQ_FILE_NAME: str="", **kwargs):
        prophecy_spark = self.spark
        self.PROC_RUN_ID = PROC_RUN_ID
        self.FEED_TYPE = FEED_TYPE
        self.MAQ_FILE_NAME = MAQ_FILE_NAME
        pass
