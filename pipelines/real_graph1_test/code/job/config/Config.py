from prophecy.config import ConfigBase


class Config(ConfigBase):

    def __init__(self, CUSTOMER_FILE: str=None, ORDER_FILE: str=None, OP_AGG_RPT_FILE: str=None, **kwargs):
        self.spark = None
        self.update(CUSTOMER_FILE, ORDER_FILE, OP_AGG_RPT_FILE)

    def update(self, CUSTOMER_FILE: str="", ORDER_FILE: str="", OP_AGG_RPT_FILE: str="", **kwargs):
        prophecy_spark = self.spark
        self.CUSTOMER_FILE = CUSTOMER_FILE
        self.ORDER_FILE = ORDER_FILE
        self.OP_AGG_RPT_FILE = OP_AGG_RPT_FILE
        pass
