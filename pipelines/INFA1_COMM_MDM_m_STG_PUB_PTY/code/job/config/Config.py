from prophecy.config import ConfigBase


class Config(ConfigBase):

    def __init__(
            self,
            RECEIVED_FILE_ID: str=None,
            BATCH_RUN_ID: str=None,
            DATA_DT: str=None,
            TARGET_TABLE1: str=None,
            **kwargs
    ):
        self.spark = None
        self.update(RECEIVED_FILE_ID, BATCH_RUN_ID, DATA_DT, TARGET_TABLE1)

    def update(
            self,
            RECEIVED_FILE_ID: str="0",
            BATCH_RUN_ID: str="0",
            DATA_DT: str="",
            TARGET_TABLE1: str="",
            **kwargs
    ):
        prophecy_spark = self.spark
        self.RECEIVED_FILE_ID = RECEIVED_FILE_ID
        self.BATCH_RUN_ID = BATCH_RUN_ID
        self.DATA_DT = DATA_DT
        self.TARGET_TABLE1 = TARGET_TABLE1
        pass
