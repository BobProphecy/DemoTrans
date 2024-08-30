from prophecy.config import ConfigBase


class Config(ConfigBase):

    def __init__(
            self,
            jdbcUrl_DSN_r2s_prod_edw_alt: str=None,
            username_DSN_r2s_prod_edw_alt: str=None,
            password_DSN_r2s_prod_edw_alt: str=None,
            jdbcUrl_DSN_r2s_prod_edw_alt_301: str=None,
            username_DSN_r2s_prod_edw_alt_301: str=None,
            password_DSN_r2s_prod_edw_alt_301: str=None,
            jdbcUrl_DSN_r2s_prod_edw_alt_298: str=None,
            username_DSN_r2s_prod_edw_alt_298: str=None,
            password_DSN_r2s_prod_edw_alt_298: str=None,
            jdbcUrl_aka_miscsql_Query_se: str=None,
            username_aka_miscsql_Query_se: str=None,
            password_aka_miscsql_Query_se: str=None,
            jdbcUrl_aka_miscsql_Query_se_168: str=None,
            username_aka_miscsql_Query_se_168: str=None,
            password_aka_miscsql_Query_se_168: str=None,
            jdbcUrl_aka_bisql_Query_sele: str=None,
            username_aka_bisql_Query_sele: str=None,
            password_aka_bisql_Query_sele: str=None,
            jdbcUrl_aka_bisql_Query_sele_183: str=None,
            username_aka_bisql_Query_sele_183: str=None,
            password_aka_bisql_Query_sele_183: str=None,
            jdbcUrl_aka_bisql_Query_sele_185: str=None,
            username_aka_bisql_Query_sele_185: str=None,
            password_aka_bisql_Query_sele_185: str=None,
            jdbcUrl_PartSeriesPlusOption: str=None,
            username_PartSeriesPlusOption: str=None,
            password_PartSeriesPlusOption: str=None,
            jdbcUrl_ManufacturingOrder_y: str=None,
            username_ManufacturingOrder_y: str=None,
            password_ManufacturingOrder_y: str=None,
            jdbcUrl_ProcessOrderTransact: str=None,
            username_ProcessOrderTransact: str=None,
            password_ProcessOrderTransact: str=None,
            jdbcUrl_ProcessOrder_yxdb: str=None,
            username_ProcessOrder_yxdb: str=None,
            password_ProcessOrder_yxdb: str=None,
            jdbcUrl_DSN_r2s_prod_edw_alt_297: str=None,
            username_DSN_r2s_prod_edw_alt_297: str=None,
            password_DSN_r2s_prod_edw_alt_297: str=None,
            jdbcUrl_aka_bisql_Query_sele_158: str=None,
            username_aka_bisql_Query_sele_158: str=None,
            password_aka_bisql_Query_sele_158: str=None,
            jdbcUrl_aka_bisql_Query_sele_154: str=None,
            username_aka_bisql_Query_sele_154: str=None,
            password_aka_bisql_Query_sele_154: str=None,
            jdbcUrl_aka_bisql_Query_sele_232: str=None,
            username_aka_bisql_Query_sele_232: str=None,
            password_aka_bisql_Query_sele_232: str=None,
            jdbcUrl_DSN_r2s_prod_aa_alte: str=None,
            username_DSN_r2s_prod_aa_alte: str=None,
            password_DSN_r2s_prod_aa_alte: str=None,
            jdbcUrl_DSN_edw_Query_manufa: str=None,
            username_DSN_edw_Query_manufa: str=None,
            password_DSN_edw_Query_manufa: str=None,
            jdbcUrl_MachineCapacity_yxdb: str=None,
            username_MachineCapacity_yxdb: str=None,
            password_MachineCapacity_yxdb: str=None,
            **kwargs
    ):
        self.spark = None
        self.update(
            jdbcUrl_DSN_r2s_prod_edw_alt, 
            username_DSN_r2s_prod_edw_alt, 
            password_DSN_r2s_prod_edw_alt, 
            jdbcUrl_DSN_r2s_prod_edw_alt_301, 
            username_DSN_r2s_prod_edw_alt_301, 
            password_DSN_r2s_prod_edw_alt_301, 
            jdbcUrl_DSN_r2s_prod_edw_alt_298, 
            username_DSN_r2s_prod_edw_alt_298, 
            password_DSN_r2s_prod_edw_alt_298, 
            jdbcUrl_aka_miscsql_Query_se, 
            username_aka_miscsql_Query_se, 
            password_aka_miscsql_Query_se, 
            jdbcUrl_aka_miscsql_Query_se_168, 
            username_aka_miscsql_Query_se_168, 
            password_aka_miscsql_Query_se_168, 
            jdbcUrl_aka_bisql_Query_sele, 
            username_aka_bisql_Query_sele, 
            password_aka_bisql_Query_sele, 
            jdbcUrl_aka_bisql_Query_sele_183, 
            username_aka_bisql_Query_sele_183, 
            password_aka_bisql_Query_sele_183, 
            jdbcUrl_aka_bisql_Query_sele_185, 
            username_aka_bisql_Query_sele_185, 
            password_aka_bisql_Query_sele_185, 
            jdbcUrl_PartSeriesPlusOption, 
            username_PartSeriesPlusOption, 
            password_PartSeriesPlusOption, 
            jdbcUrl_ManufacturingOrder_y, 
            username_ManufacturingOrder_y, 
            password_ManufacturingOrder_y, 
            jdbcUrl_ProcessOrderTransact, 
            username_ProcessOrderTransact, 
            password_ProcessOrderTransact, 
            jdbcUrl_ProcessOrder_yxdb, 
            username_ProcessOrder_yxdb, 
            password_ProcessOrder_yxdb, 
            jdbcUrl_DSN_r2s_prod_edw_alt_297, 
            username_DSN_r2s_prod_edw_alt_297, 
            password_DSN_r2s_prod_edw_alt_297, 
            jdbcUrl_aka_bisql_Query_sele_158, 
            username_aka_bisql_Query_sele_158, 
            password_aka_bisql_Query_sele_158, 
            jdbcUrl_aka_bisql_Query_sele_154, 
            username_aka_bisql_Query_sele_154, 
            password_aka_bisql_Query_sele_154, 
            jdbcUrl_aka_bisql_Query_sele_232, 
            username_aka_bisql_Query_sele_232, 
            password_aka_bisql_Query_sele_232, 
            jdbcUrl_DSN_r2s_prod_aa_alte, 
            username_DSN_r2s_prod_aa_alte, 
            password_DSN_r2s_prod_aa_alte, 
            jdbcUrl_DSN_edw_Query_manufa, 
            username_DSN_edw_Query_manufa, 
            password_DSN_edw_Query_manufa, 
            jdbcUrl_MachineCapacity_yxdb, 
            username_MachineCapacity_yxdb, 
            password_MachineCapacity_yxdb
        )

    def update(
            self,
            jdbcUrl_DSN_r2s_prod_edw_alt: str="odbc:DSN=r2s-prod-edw-alteryx",
            username_DSN_r2s_prod_edw_alt: str="",
            password_DSN_r2s_prod_edw_alt: str="",
            jdbcUrl_DSN_r2s_prod_edw_alt_301: str="odbc:DSN=r2s-prod-edw-alteryx",
            username_DSN_r2s_prod_edw_alt_301: str="",
            password_DSN_r2s_prod_edw_alt_301: str="",
            jdbcUrl_DSN_r2s_prod_edw_alt_298: str="odbc:DSN=r2s-prod-edw-alteryx",
            username_DSN_r2s_prod_edw_alt_298: str="",
            password_DSN_r2s_prod_edw_alt_298: str="",
            jdbcUrl_aka_miscsql_Query_se: str="aka:64a54600381c38b000ad7b5b",
            username_aka_miscsql_Query_se: str="",
            password_aka_miscsql_Query_se: str="",
            jdbcUrl_aka_miscsql_Query_se_168: str="aka:64a54600381c38b000ad7b5b",
            username_aka_miscsql_Query_se_168: str="",
            password_aka_miscsql_Query_se_168: str="",
            jdbcUrl_aka_bisql_Query_sele: str="aka:64a2f038381c38b000cdece3",
            username_aka_bisql_Query_sele: str="",
            password_aka_bisql_Query_sele: str="",
            jdbcUrl_aka_bisql_Query_sele_183: str="aka:64a2f038381c38b000cdece3",
            username_aka_bisql_Query_sele_183: str="",
            password_aka_bisql_Query_sele_183: str="",
            jdbcUrl_aka_bisql_Query_sele_185: str="aka:64a2f038381c38b000cdece3",
            username_aka_bisql_Query_sele_185: str="",
            password_aka_bisql_Query_sele_185: str="",
            jdbcUrl_PartSeriesPlusOption: str="X:\\ADW\\Common\\Part Series Plus Option.yxdb",
            username_PartSeriesPlusOption: str="",
            password_PartSeriesPlusOption: str="",
            jdbcUrl_ManufacturingOrder_y: str="X:\\source\\shop floor control\\Manufacturing Order.yxdb",
            username_ManufacturingOrder_y: str="",
            password_ManufacturingOrder_y: str="",
            jdbcUrl_ProcessOrderTransact: str="X:\\source\\shop floor control\\Process Order Transaction.yxdb",
            username_ProcessOrderTransact: str="",
            password_ProcessOrderTransact: str="",
            jdbcUrl_ProcessOrder_yxdb: str="X:\\source\\shop floor control\\Process Order.yxdb",
            username_ProcessOrder_yxdb: str="",
            password_ProcessOrder_yxdb: str="",
            jdbcUrl_DSN_r2s_prod_edw_alt_297: str="odbc:DSN=r2s-prod-edw-alteryx",
            username_DSN_r2s_prod_edw_alt_297: str="",
            password_DSN_r2s_prod_edw_alt_297: str="",
            jdbcUrl_aka_bisql_Query_sele_158: str="aka:64a2f038381c38b000cdece3",
            username_aka_bisql_Query_sele_158: str="",
            password_aka_bisql_Query_sele_158: str="",
            jdbcUrl_aka_bisql_Query_sele_154: str="aka:64a2f038381c38b000cdece3",
            username_aka_bisql_Query_sele_154: str="",
            password_aka_bisql_Query_sele_154: str="",
            jdbcUrl_aka_bisql_Query_sele_232: str="aka:64a2f038381c38b000cdece3",
            username_aka_bisql_Query_sele_232: str="",
            password_aka_bisql_Query_sele_232: str="",
            jdbcUrl_DSN_r2s_prod_aa_alte: str="rsbl:DSN=r2s-prod-aa-alteryx;Bucket=samtec-datalake-working/user-data/alteryx/;Access=AKIARO4LBS2DXY7W3CGV;Secret=4B422370B0AC40658E643D23F6B8CE853FB29C20F4A9352C4253A13D6F28104BBD1F0CE1A999819881B119621775BA3CE4F337BF914850FBF6079CA840E654EFDEF9AB233DE73B2DD5247CB1F5CC1B0474ECB3507;URL=Default",
            username_DSN_r2s_prod_aa_alte: str="",
            password_DSN_r2s_prod_aa_alte: str="",
            jdbcUrl_DSN_edw_Query_manufa: str="rsbl:DSN=edw;UID=alteryx;PWD=__EncPwd1__;Bucket=samtec-datalake-working/user-data/alteryx/;Access=AKIARO4LBS2DXY7W3CGV;Secret=4B422370B0AC40658E643D23F6B8CE853FB29C20F4A9352C4253A13D6F28104BBD1F0CE1A999819881B119621775BA3CE4F337BF914850FBF6079CA840E654EFDEF9AB233DE73B2DD5247CB1F5CC1B0474ECB3507;URL=Default",
            username_DSN_edw_Query_manufa: str="",
            password_DSN_edw_Query_manufa: str="",
            jdbcUrl_MachineCapacity_yxdb: str="X:\\ADW\\Operations\\Manufacturing\\Machine Capacity.yxdb",
            username_MachineCapacity_yxdb: str="",
            password_MachineCapacity_yxdb: str="",
            **kwargs
    ):
        prophecy_spark = self.spark
        self.jdbcUrl_DSN_r2s_prod_edw_alt = jdbcUrl_DSN_r2s_prod_edw_alt
        self.username_DSN_r2s_prod_edw_alt = username_DSN_r2s_prod_edw_alt
        self.password_DSN_r2s_prod_edw_alt = password_DSN_r2s_prod_edw_alt
        self.jdbcUrl_DSN_r2s_prod_edw_alt_301 = jdbcUrl_DSN_r2s_prod_edw_alt_301
        self.username_DSN_r2s_prod_edw_alt_301 = username_DSN_r2s_prod_edw_alt_301
        self.password_DSN_r2s_prod_edw_alt_301 = password_DSN_r2s_prod_edw_alt_301
        self.jdbcUrl_DSN_r2s_prod_edw_alt_298 = jdbcUrl_DSN_r2s_prod_edw_alt_298
        self.username_DSN_r2s_prod_edw_alt_298 = username_DSN_r2s_prod_edw_alt_298
        self.password_DSN_r2s_prod_edw_alt_298 = password_DSN_r2s_prod_edw_alt_298
        self.jdbcUrl_aka_miscsql_Query_se = jdbcUrl_aka_miscsql_Query_se
        self.username_aka_miscsql_Query_se = username_aka_miscsql_Query_se
        self.password_aka_miscsql_Query_se = password_aka_miscsql_Query_se
        self.jdbcUrl_aka_miscsql_Query_se_168 = jdbcUrl_aka_miscsql_Query_se_168
        self.username_aka_miscsql_Query_se_168 = username_aka_miscsql_Query_se_168
        self.password_aka_miscsql_Query_se_168 = password_aka_miscsql_Query_se_168
        self.jdbcUrl_aka_bisql_Query_sele = jdbcUrl_aka_bisql_Query_sele
        self.username_aka_bisql_Query_sele = username_aka_bisql_Query_sele
        self.password_aka_bisql_Query_sele = password_aka_bisql_Query_sele
        self.jdbcUrl_aka_bisql_Query_sele_183 = jdbcUrl_aka_bisql_Query_sele_183
        self.username_aka_bisql_Query_sele_183 = username_aka_bisql_Query_sele_183
        self.password_aka_bisql_Query_sele_183 = password_aka_bisql_Query_sele_183
        self.jdbcUrl_aka_bisql_Query_sele_185 = jdbcUrl_aka_bisql_Query_sele_185
        self.username_aka_bisql_Query_sele_185 = username_aka_bisql_Query_sele_185
        self.password_aka_bisql_Query_sele_185 = password_aka_bisql_Query_sele_185
        self.jdbcUrl_PartSeriesPlusOption = jdbcUrl_PartSeriesPlusOption
        self.username_PartSeriesPlusOption = username_PartSeriesPlusOption
        self.password_PartSeriesPlusOption = password_PartSeriesPlusOption
        self.jdbcUrl_ManufacturingOrder_y = jdbcUrl_ManufacturingOrder_y
        self.username_ManufacturingOrder_y = username_ManufacturingOrder_y
        self.password_ManufacturingOrder_y = password_ManufacturingOrder_y
        self.jdbcUrl_ProcessOrderTransact = jdbcUrl_ProcessOrderTransact
        self.username_ProcessOrderTransact = username_ProcessOrderTransact
        self.password_ProcessOrderTransact = password_ProcessOrderTransact
        self.jdbcUrl_ProcessOrder_yxdb = jdbcUrl_ProcessOrder_yxdb
        self.username_ProcessOrder_yxdb = username_ProcessOrder_yxdb
        self.password_ProcessOrder_yxdb = password_ProcessOrder_yxdb
        self.jdbcUrl_DSN_r2s_prod_edw_alt_297 = jdbcUrl_DSN_r2s_prod_edw_alt_297
        self.username_DSN_r2s_prod_edw_alt_297 = username_DSN_r2s_prod_edw_alt_297
        self.password_DSN_r2s_prod_edw_alt_297 = password_DSN_r2s_prod_edw_alt_297
        self.jdbcUrl_aka_bisql_Query_sele_158 = jdbcUrl_aka_bisql_Query_sele_158
        self.username_aka_bisql_Query_sele_158 = username_aka_bisql_Query_sele_158
        self.password_aka_bisql_Query_sele_158 = password_aka_bisql_Query_sele_158
        self.jdbcUrl_aka_bisql_Query_sele_154 = jdbcUrl_aka_bisql_Query_sele_154
        self.username_aka_bisql_Query_sele_154 = username_aka_bisql_Query_sele_154
        self.password_aka_bisql_Query_sele_154 = password_aka_bisql_Query_sele_154
        self.jdbcUrl_aka_bisql_Query_sele_232 = jdbcUrl_aka_bisql_Query_sele_232
        self.username_aka_bisql_Query_sele_232 = username_aka_bisql_Query_sele_232
        self.password_aka_bisql_Query_sele_232 = password_aka_bisql_Query_sele_232
        self.jdbcUrl_DSN_r2s_prod_aa_alte = jdbcUrl_DSN_r2s_prod_aa_alte
        self.username_DSN_r2s_prod_aa_alte = username_DSN_r2s_prod_aa_alte
        self.password_DSN_r2s_prod_aa_alte = password_DSN_r2s_prod_aa_alte
        self.jdbcUrl_DSN_edw_Query_manufa = jdbcUrl_DSN_edw_Query_manufa
        self.username_DSN_edw_Query_manufa = username_DSN_edw_Query_manufa
        self.password_DSN_edw_Query_manufa = password_DSN_edw_Query_manufa
        self.jdbcUrl_MachineCapacity_yxdb = jdbcUrl_MachineCapacity_yxdb
        self.username_MachineCapacity_yxdb = username_MachineCapacity_yxdb
        self.password_MachineCapacity_yxdb = password_MachineCapacity_yxdb
        pass
