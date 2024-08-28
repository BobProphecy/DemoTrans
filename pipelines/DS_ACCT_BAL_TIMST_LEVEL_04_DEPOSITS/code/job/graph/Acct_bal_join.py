from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def Acct_bal_join(spark: SparkSession, PASS_IN: DataFrame, ACCT_Lkp: DataFrame, ODH_LKP: DataFrame) -> DataFrame:
    from typing import Optional, List, Dict
    from dataclasses import dataclass, field
    from abc import ABC
    
    from pyspark.sql.column import Column
    from pyspark.sql.functions import col
    from dataclasses import dataclass
    from typing import Optional, List, Dict
    from pyspark.sql.column import Column as sparkColumn


    @dataclass(frozen = True)
    class SColumn:
        expression: Optional[Column] = None

        @staticmethod
        def getSColumn(column: str):
            return SColumn(col(column))

        def column(self) -> sparkColumn:
            return self.expression

        def columnName(self) -> str:
            return self.expression._jc.toString()


    @dataclass(frozen = True)
    class SColumnExpression:
        target: str
        expression: SColumn
        description: str
        _row_id: Optional[str] = None

        @staticmethod
        def remove_backticks(s):
            if s.startswith("`") and s.endswith("`"):
                return s[1:- 1]
            else:
                return s

        @staticmethod
        def getColumnExpression(column: str):
            return SColumnExpression(column, SColumn.getSColumn(col(column)), "")

        @staticmethod
        def getColumnsFromColumnExpressionList(columnExpressions: list):
            columnList = []

            for expression in columnExpressions:
                columnList.append(expression.expression)

            return columnList

        def column(self) -> Column:

            if (self.expression.columnName() == SColumnExpression.remove_backticks(self.target)):
                return self.expression.expression

            return self.expression.expression.alias(self.target)


    @dataclass(frozen = True)
    class Hint:
        id: str
        alias: str
        hintType: str
        propagateColumns: bool


    @dataclass(frozen = True)
    class JoinCondition:
        alias: str
        expression: SColumn
        joinType: str


    @dataclass(frozen = True)
    class JoinProperties():
        activeTab: str = "conditions"
        columnsSelector: List[str] = field(default_factory = list)
        conditions: List[JoinCondition] = field(default_factory = lambda : [JoinCondition("in1", SColumn(""), "inner")])
        expressions: List[SColumnExpression] = field(default_factory = list)
        headAlias: str = "in0"
        whereClause: Optional[SColumn] = None
        allIn0: Optional[bool] = None
        allIn1: Optional[bool] = None
        hints: Optional[List[Hint]] = field(
            default_factory = lambda : [Hint("in0", "in0", "none", False), Hint("in1", "in1", "none", False)]
        )

    props = JoinProperties(  #skiptraversal
        activeTab = "conditions", 
        columnsSelector = [], 
        conditions = [JoinCondition(
           alias = "ACCT_Lkp", 
           expression = SColumn(
             (
               (col("ACCT_Lkp.ACCT_ID") == col("PASS_IN.ACCT_ID"))
               & (col("ACCT_Lkp.AS_OF_DT") == col("PASS_IN.AS_OF_DT"))
             )
           ), 
           joinType = "inner"
         ),          JoinCondition(
           alias = "ODH_LKP", 
           expression = SColumn((col("ODH_LKP.DATA_DATE") == col("DATA_DATE"))), 
           joinType = "inner"
         )], 
        expressions = [SColumnExpression(
           _row_id = "1126207400", 
           target = "PAPER_STMT_CNT", 
           expression = SColumn(col("PASS_IN.PAPER_STMT_CNT")), 
           description = ""
         ),          SColumnExpression(
           _row_id = "1273234655", 
           target = "TIMST_ACCOUNT", 
           expression = SColumn(col("PASS_IN.TIMST_ACCOUNT")), 
           description = ""
         ),          SColumnExpression(
           _row_id = "974223719", 
           target = "GL_QTD_AVG_BANK_SHR_BAL", 
           expression = SColumn(col("PASS_IN.GL_QTD_AVG_BANK_SHR_BAL")), 
           description = ""
         ),          SColumnExpression(
           _row_id = "707378773", 
           target = "GL_MTD_AVG_CUR_BAL", 
           expression = SColumn(col("PASS_IN.GL_MTD_AVG_CUR_BAL")), 
           description = ""
         ),          SColumnExpression(
           _row_id = "1663943919", 
           target = "GL_YTD_AGGR_BANK_SHR_BAL", 
           expression = SColumn(col("PASS_IN.GL_YTD_AGGR_BANK_SHR_BAL")), 
           description = ""
         ),          SColumnExpression(
           _row_id = "1511168602", 
           target = "GL_MTD_AGGR_BANK_SHR_INT_AMT", 
           expression = SColumn(col("PASS_IN.GL_MTD_AGGR_BANK_SHR_INT_AMT")), 
           description = ""
         ),          SColumnExpression(
           _row_id = "887870277", 
           target = "INT_ACCR_BAL", 
           expression = SColumn(col("PASS_IN.INT_ACCR_BAL")), 
           description = ""
         ),          SColumnExpression(
           _row_id = "130734550", 
           target = "YTD_AVG_BAL", 
           expression = SColumn(col("PASS_IN.YTD_AVG_BAL")), 
           description = ""
         ),          SColumnExpression(
           _row_id = "1195556359", 
           target = "LAST_INT_REPRC_DT", 
           expression = SColumn(col("PASS_IN.LAST_INT_REPRC_DT")), 
           description = ""
         ),          SColumnExpression(
           _row_id = "2057826480", 
           target = "MTD_INT_ACCR_AMT", 
           expression = SColumn(col("PASS_IN.MTD_INT_ACCR_AMT")), 
           description = ""
         ),          SColumnExpression(
           _row_id = "606338131", 
           target = "NEXT_INT_REPRC_DT", 
           expression = SColumn(col("PASS_IN.NEXT_INT_REPRC_DT")), 
           description = ""
         ),          SColumnExpression(
           _row_id = "1125009177", 
           target = "MTD_LOWEST_CUR_ACCT_BAL", 
           expression = SColumn(col("PASS_IN.MTD_LOWEST_CUR_ACCT_BAL")), 
           description = ""
         ),          SColumnExpression(
           _row_id = "986158312", 
           target = "INT_RATE", 
           expression = SColumn(col("PASS_IN.INT_RATE")), 
           description = ""
         ),          SColumnExpression(
           _row_id = "583157651", 
           target = "AS_OF_DT", 
           expression = SColumn(col("PASS_IN.AS_OF_DT")), 
           description = ""
         ),          SColumnExpression(
           _row_id = "1135182385", 
           target = "DATA_DATE", 
           expression = SColumn(col("ODH_LKP.DATA_DATE")), 
           description = ""
         ),          SColumnExpression(
           _row_id = "905503578", 
           target = "RPT_FA_LOCTN_ID", 
           expression = SColumn(col("ACCT_Lkp.RPT_FA_LOCTN_ID")), 
           description = ""
         ),          SColumnExpression(
           _row_id = "881388644", 
           target = "RP_IND", 
           expression = SColumn(col("ACCT_Lkp.RP_IND")), 
           description = ""
         ),          SColumnExpression(
           _row_id = "2137733566", 
           target = "STMT_IMAGE_DESC", 
           expression = SColumn(col("ACCT_Lkp.STMT_IMAGE_DESC")), 
           description = ""
         ),          SColumnExpression(
           _row_id = "438455774", 
           target = "STMT_ACCT_NBR", 
           expression = SColumn(col("ACCT_Lkp.STMT_ACCT_NBR")), 
           description = ""
         ),          SColumnExpression(
           _row_id = "254499222", 
           target = "SRC_CONVERTED_REC_CD", 
           expression = SColumn(col("ACCT_Lkp.SRC_CONVERTED_REC_CD")), 
           description = ""
         ),          SColumnExpression(
           _row_id = "1234481372", 
           target = "NAICS_DESC", 
           expression = SColumn(col("ACCT_Lkp.NAICS_DESC")), 
           description = ""
         ),          SColumnExpression(
           _row_id = "563965040", 
           target = "SCNDY_LVL_INT_MRGN_ACTION_CD", 
           expression = SColumn(col("ACCT_Lkp.SCNDY_LVL_INT_MRGN_ACTION_CD")), 
           description = ""
         ),          SColumnExpression(
           _row_id = "1824164078", 
           target = "INT_ACCR_BASIS_DESC", 
           expression = SColumn(col("ACCT_Lkp.INT_ACCR_BASIS_DESC")), 
           description = ""
         ),          SColumnExpression(
           _row_id = "258564423", 
           target = "SRC_INT_REPRC_FREQ_TERM", 
           expression = SColumn(col("ACCT_Lkp.SRC_INT_REPRC_FREQ_TERM")), 
           description = ""
         ),          SColumnExpression(
           _row_id = "1450505521", 
           target = "INT_INDX_CD", 
           expression = SColumn(col("ACCT_Lkp.INT_INDX_CD")), 
           description = ""
         ),          SColumnExpression(
           _row_id = "1769400266", 
           target = "INT_RATE_TYP", 
           expression = SColumn(col("ACCT_Lkp.INT_RATE_TYP")), 
           description = ""
         ),          SColumnExpression(
           _row_id = "1739247042", 
           target = "SRC_EMP_CD", 
           expression = SColumn(col("ACCT_Lkp.SRC_EMP_CD")), 
           description = ""
         ),          SColumnExpression(
           _row_id = "1219379865", 
           target = "SHORT_NAME", 
           expression = SColumn(col("ACCT_Lkp.SHORT_NAME")), 
           description = ""
         ),          SColumnExpression(
           _row_id = "932672535", 
           target = "CUR_TERM", 
           expression = SColumn(col("ACCT_Lkp.CUR_TERM")), 
           description = ""
         ),          SColumnExpression(
           _row_id = "907511056", 
           target = "MAT_DT", 
           expression = SColumn(col("ACCT_Lkp.MAT_DT")), 
           description = ""
         ),          SColumnExpression(
           _row_id = "517812741", 
           target = "INT_BEG_DT", 
           expression = SColumn(col("ACCT_Lkp.INT_BEG_DT")), 
           description = ""
         ),          SColumnExpression(
           _row_id = "699356083", 
           target = "OPEN_DT", 
           expression = SColumn(col("ACCT_Lkp.OPEN_DT")), 
           description = ""
         ),          SColumnExpression(
           _row_id = "545018904", 
           target = "OPEN_CLOSE_DESC", 
           expression = SColumn(col("ACCT_Lkp.OPEN_CLOSE_DESC")), 
           description = ""
         ),          SColumnExpression(
           _row_id = "1739932610", 
           target = "APPL_CD", 
           expression = SColumn(col("ACCT_Lkp.APPL_CD")), 
           description = ""
         ),          SColumnExpression(
           _row_id = "828130019", 
           target = "RPT_CC_ID", 
           expression = SColumn(col("ACCT_Lkp.RPT_CC_ID")), 
           description = ""
         ),          SColumnExpression(
           _row_id = "703345986", 
           target = "NEXT_INT_RATE_CHG_DT", 
           expression = SColumn(col("ACCT_Lkp.NEXT_INT_RATE_CHG_DT")), 
           description = ""
         ),          SColumnExpression(
           _row_id = "606525681", 
           target = "COMP_ID", 
           expression = SColumn(col("ACCT_Lkp.COMP_ID")), 
           description = ""
         ),          SColumnExpression(
           _row_id = "269684595", 
           target = "LAST_STMT_DT", 
           expression = SColumn(col("ACCT_Lkp.LAST_STMT_DT")), 
           description = ""
         ),          SColumnExpression(
           _row_id = "304437077", 
           target = "ACCT_TYP", 
           expression = SColumn(col("ACCT_Lkp.ACCT_TYP")), 
           description = ""
         ),          SColumnExpression(
           _row_id = "1413098601", 
           target = "APPL_DESC", 
           expression = SColumn(col("ACCT_Lkp.APPL_DESC")), 
           description = ""
         ),          SColumnExpression(
           _row_id = "1672639840", 
           target = "SRC_DTL_STAT_CD", 
           expression = SColumn(col("ACCT_Lkp.SRC_DTL_STAT_CD")), 
           description = ""
         ),          SColumnExpression(
           _row_id = "1791624102", 
           target = "CLOSE_DT", 
           expression = SColumn(col("ACCT_Lkp.CLOSE_DT")), 
           description = ""
         ),          SColumnExpression(
           _row_id = "1805847519", 
           target = "RNWL_PRCS_DT", 
           expression = SColumn(col("ACCT_Lkp.RNWL_PRCS_DT")), 
           description = ""
         ),          SColumnExpression(
           _row_id = "70815200", 
           target = "OPEN_AMT", 
           expression = SColumn(col("ACCT_Lkp.OPEN_AMT")), 
           description = ""
         ),          SColumnExpression(
           _row_id = "1421817528", 
           target = "INT_MRGN_RATE", 
           expression = SColumn(col("ACCT_Lkp.INT_MRGN_RATE")), 
           description = ""
         ),          SColumnExpression(
           _row_id = "1952914236", 
           target = "SRC_TAX_ID_TYP", 
           expression = SColumn(col("ACCT_Lkp.SRC_TAX_ID_TYP")), 
           description = ""
         ),          SColumnExpression(
           _row_id = "1759612630", 
           target = "SRC_EMP_DESC", 
           expression = SColumn(col("ACCT_Lkp.SRC_EMP_DESC")), 
           description = ""
         ),          SColumnExpression(
           _row_id = "1307111263", 
           target = "SRC_INT_MRGN_ACTION_CD", 
           expression = SColumn(col("ACCT_Lkp.SRC_INT_MRGN_ACTION_CD")), 
           description = ""
         ),          SColumnExpression(
           _row_id = "597021385", 
           target = "INT_INDX_DESC", 
           expression = SColumn(col("ACCT_Lkp.INT_INDX_DESC")), 
           description = ""
         ),          SColumnExpression(
           _row_id = "1522069929", 
           target = "INT_REPRC_FREQ_TERM", 
           expression = SColumn(col("ACCT_Lkp.INT_REPRC_FREQ_TERM")), 
           description = ""
         ),          SColumnExpression(
           _row_id = "256270963", 
           target = "NAICS_CD", 
           expression = SColumn(col("ACCT_Lkp.NAICS_CD")), 
           description = ""
         ),          SColumnExpression(
           _row_id = "1933733757", 
           target = "SRC_RPT_BRNCH_NBR", 
           expression = SColumn(col("ACCT_Lkp.SRC_RPT_BRNCH_NBR")), 
           description = ""
         ),          SColumnExpression(
           _row_id = "1877989704", 
           target = "RPT_REC_IND", 
           expression = SColumn(col("ACCT_Lkp.RPT_REC_IND")), 
           description = ""
         ),          SColumnExpression(
           _row_id = "166458492", 
           target = "GL_CC_ID", 
           expression = SColumn(col("ACCT_Lkp.GL_CC_ID")), 
           description = ""
         ),          SColumnExpression(
           _row_id = "2035174264", 
           target = "CUTOFF_STMT_FREQ_CD", 
           expression = SColumn(col("ACCT_Lkp.CUTOFF_STMT_FREQ_CD")), 
           description = ""
         ),          SColumnExpression(
           _row_id = "1809867960", 
           target = "PROD_DTL_ID", 
           expression = SColumn(col("ACCT_Lkp.PROD_DTL_ID")), 
           description = ""
         ),          SColumnExpression(
           _row_id = "1445520179", 
           target = "ENOTICE_IND", 
           expression = SColumn(col("ACCT_Lkp.ENOTICE_IND")), 
           description = ""
         ),          SColumnExpression(
           _row_id = "1924001484", 
           target = "RPT_FA_LOCTN_EFF_DT", 
           expression = SColumn(col("ACCT_Lkp.RPT_FA_LOCTN_EFF_DT")), 
           description = ""
         ),          SColumnExpression(
           _row_id = "974212119", 
           target = "ODATE", 
           expression = SColumn(col("ODH_LKP.ODATE")), 
           description = ""
         ),          SColumnExpression(
           _row_id = "445056530", 
           target = "DATA_DT", 
           expression = SColumn(col("PASS_IN.DATA_DT")), 
           description = ""
         ),          SColumnExpression(
           _row_id = "987203997", 
           target = "INT_DT", 
           expression = SColumn(col("PASS_IN.INT_DT")), 
           description = ""
         ),          SColumnExpression(
           _row_id = "749998458", 
           target = "YTD_LOWEST_CUR_ACCT_BAL", 
           expression = SColumn(col("PASS_IN.YTD_LOWEST_CUR_ACCT_BAL")), 
           description = ""
         ),          SColumnExpression(
           _row_id = "456942241", 
           target = "ENTRY_DT", 
           expression = SColumn(col("PASS_IN.ENTRY_DT")), 
           description = ""
         ),          SColumnExpression(
           _row_id = "1284655112", 
           target = "MTD_AVG_BAL", 
           expression = SColumn(col("PASS_IN.MTD_AVG_BAL")), 
           description = ""
         ),          SColumnExpression(
           _row_id = "911526030", 
           target = "MTD_AGGR_INT_RATE", 
           expression = SColumn(col("PASS_IN.MTD_AGGR_INT_RATE")), 
           description = ""
         ),          SColumnExpression(
           _row_id = "381216975", 
           target = "YTD_AVG_BANK_SHR_BAL", 
           expression = SColumn(col("PASS_IN.YTD_AVG_BANK_SHR_BAL")), 
           description = ""
         ),          SColumnExpression(
           _row_id = "2064505280", 
           target = "BANK_SHR_INT_PERDIEM_AMT", 
           expression = SColumn(col("PASS_IN.BANK_SHR_INT_PERDIEM_AMT")), 
           description = ""
         ),          SColumnExpression(
           _row_id = "182758516", 
           target = "GL_MTD_AGGR_BANK_SHR_BAL", 
           expression = SColumn(col("PASS_IN.GL_MTD_AGGR_BANK_SHR_BAL")), 
           description = ""
         ),          SColumnExpression(
           _row_id = "222090704", 
           target = "GL_YTD_AGGR_INT_AMT", 
           expression = SColumn(col("PASS_IN.GL_YTD_AGGR_INT_AMT")), 
           description = ""
         ),          SColumnExpression(
           _row_id = "1239758514", 
           target = "GL_YTD_AVG_BANK_SHR_BAL", 
           expression = SColumn(col("PASS_IN.GL_YTD_AVG_BANK_SHR_BAL")), 
           description = ""
         ),          SColumnExpression(
           _row_id = "936491318", 
           target = "GL_QTD_AGGR_CUR_BAL", 
           expression = SColumn(col("PASS_IN.GL_QTD_AGGR_CUR_BAL")), 
           description = ""
         ),          SColumnExpression(
           _row_id = "934035242", 
           target = "TIMST_INTRATE", 
           expression = SColumn(col("PASS_IN.TIMST_INTRATE")), 
           description = ""
         ),          SColumnExpression(
           _row_id = "1039288178", 
           target = "TIMST_INTINDEX", 
           expression = SColumn(col("PASS_IN.TIMST_INTINDEX")), 
           description = ""
         ),          SColumnExpression(
           _row_id = "1517749085", 
           target = "RptRecInd", 
           expression = SColumn(col("PASS_IN.RptRecInd")), 
           description = ""
         ),          SColumnExpression(
           _row_id = "257466331", 
           target = "ESTMT_CNT", 
           expression = SColumn(col("PASS_IN.ESTMT_CNT")), 
           description = ""
         ),          SColumnExpression(
           _row_id = "576167683", 
           target = "TIMST_INST", 
           expression = SColumn(col("PASS_IN.TIMST_INST")), 
           description = ""
         ),          SColumnExpression(
           _row_id = "324646550", 
           target = "GL_QTD_AGGR_BANK_SHR_BAL", 
           expression = SColumn(col("PASS_IN.GL_QTD_AGGR_BANK_SHR_BAL")), 
           description = ""
         ),          SColumnExpression(
           _row_id = "646638961", 
           target = "GL_YTD_AVG_CUR_BAL", 
           expression = SColumn(col("PASS_IN.GL_YTD_AVG_CUR_BAL")), 
           description = ""
         ),          SColumnExpression(
           _row_id = "1200093132", 
           target = "GL_MTD_AGGR_CUR_BAL", 
           expression = SColumn(col("PASS_IN.GL_MTD_AGGR_CUR_BAL")), 
           description = ""
         ),          SColumnExpression(
           _row_id = "412001757", 
           target = "GL_YTD_AGGR_BANK_SHR_INT_AMT", 
           expression = SColumn(col("PASS_IN.GL_YTD_AGGR_BANK_SHR_INT_AMT")), 
           description = ""
         ),          SColumnExpression(
           _row_id = "1102725209", 
           target = "YTD_AGGR_INT_RATE", 
           expression = SColumn(col("PASS_IN.YTD_AGGR_INT_RATE")), 
           description = ""
         ),          SColumnExpression(
           _row_id = "1475076238", 
           target = "YTD_AGGR_BAL", 
           expression = SColumn(col("PASS_IN.YTD_AGGR_BAL")), 
           description = ""
         ),          SColumnExpression(
           _row_id = "683845091", 
           target = "MTD_AVG_WT_INT_RATE", 
           expression = SColumn(col("PASS_IN.MTD_AVG_WT_INT_RATE")), 
           description = ""
         ),          SColumnExpression(
           _row_id = "1960752258", 
           target = "MTD_AGGR_BAL", 
           expression = SColumn(col("PASS_IN.MTD_AGGR_BAL")), 
           description = ""
         ),          SColumnExpression(
           _row_id = "1102801050", 
           target = "YTD_HIGHEST_CUR_ACCT_BAL", 
           expression = SColumn(col("PASS_IN.YTD_HIGHEST_CUR_ACCT_BAL")), 
           description = ""
         ),          SColumnExpression(
           _row_id = "1903915156", 
           target = "INT_PERDIEM_AMT", 
           expression = SColumn(col("PASS_IN.INT_PERDIEM_AMT")), 
           description = ""
         ),          SColumnExpression(
           _row_id = "540576782", 
           target = "BANK_SHR_BAL", 
           expression = SColumn(col("PASS_IN.BANK_SHR_BAL")), 
           description = ""
         ),          SColumnExpression(
           _row_id = "1706680280", 
           target = "ACCT_ID", 
           expression = SColumn(col("PASS_IN.ACCT_ID")), 
           description = ""
         ),          SColumnExpression(
           _row_id = "735299643", 
           target = "RPT_FA_LOB_EFF_DT", 
           expression = SColumn(col("ACCT_Lkp.RPT_FA_LOB_EFF_DT")), 
           description = ""
         ),          SColumnExpression(
           _row_id = "1183288197", 
           target = "RPT_FA_CC_EFF_DT", 
           expression = SColumn(col("ACCT_Lkp.RPT_FA_CC_EFF_DT")), 
           description = ""
         ),          SColumnExpression(
           _row_id = "874435770", 
           target = "ALLOW_RP_IND", 
           expression = SColumn(col("ACCT_Lkp.ALLOW_RP_IND")), 
           description = ""
         ),          SColumnExpression(
           _row_id = "847889248", 
           target = "STMT_IMAGE_CD", 
           expression = SColumn(col("ACCT_Lkp.STMT_IMAGE_CD")), 
           description = ""
         ),          SColumnExpression(
           _row_id = "1408214346", 
           target = "MST_ACCT_EFF_DT", 
           expression = SColumn(col("ACCT_Lkp.MST_ACCT_EFF_DT")), 
           description = ""
         ),          SColumnExpression(
           _row_id = "1017120486", 
           target = "CONVERTED_REC_CD", 
           expression = SColumn(col("ACCT_Lkp.CONVERTED_REC_CD")), 
           description = ""
         ),          SColumnExpression(
           _row_id = "998599409", 
           target = "SRC_SCNDY_OFCR_CD", 
           expression = SColumn(col("ACCT_Lkp.SRC_SCNDY_OFCR_CD")), 
           description = ""
         ),          SColumnExpression(
           _row_id = "805094799", 
           target = "SCNDY_LVL_INT_MRGN_RATE", 
           expression = SColumn(col("ACCT_Lkp.SCNDY_LVL_INT_MRGN_RATE")), 
           description = ""
         ),          SColumnExpression(
           _row_id = "1776033565", 
           target = "INT_ACCR_BASIS_CD", 
           expression = SColumn(col("ACCT_Lkp.INT_ACCR_BASIS_CD")), 
           description = ""
         ),          SColumnExpression(
           _row_id = "1982217426", 
           target = "SRC_INT_REPRC_DESC", 
           expression = SColumn(col("ACCT_Lkp.SRC_INT_REPRC_DESC")), 
           description = ""
         ),          SColumnExpression(
           _row_id = "773500960", 
           target = "SRC_INT_INDX_CD", 
           expression = SColumn(col("ACCT_Lkp.SRC_INT_INDX_CD")), 
           description = ""
         ),          SColumnExpression(
           _row_id = "1308480952", 
           target = "EMP_DESC", 
           expression = SColumn(col("ACCT_Lkp.EMP_DESC")), 
           description = ""
         ),          SColumnExpression(
           _row_id = "1592215815", 
           target = "TIN", 
           expression = SColumn(col("ACCT_Lkp.TIN")), 
           description = ""
         ),          SColumnExpression(
           _row_id = "1350934908", 
           target = "ORIG_INT_INDX_BASE_RATE", 
           expression = SColumn(col("ACCT_Lkp.ORIG_INT_INDX_BASE_RATE")), 
           description = ""
         ),          SColumnExpression(
           _row_id = "1516074761", 
           target = "ORIG_INT_RATE", 
           expression = SColumn(col("ACCT_Lkp.ORIG_INT_RATE")), 
           description = ""
         ),          SColumnExpression(
           _row_id = "1559170796", 
           target = "ORIG_MAT_DT", 
           expression = SColumn(col("ACCT_Lkp.ORIG_MAT_DT")), 
           description = ""
         ),          SColumnExpression(
           _row_id = "991709754", 
           target = "ORIG_INT_BEG_DT", 
           expression = SColumn(col("ACCT_Lkp.ORIG_INT_BEG_DT")), 
           description = ""
         ),          SColumnExpression(
           _row_id = "582651888", 
           target = "ORIG_OPEN_DT", 
           expression = SColumn(col("ACCT_Lkp.ORIG_OPEN_DT")), 
           description = ""
         ),          SColumnExpression(
           _row_id = "836311368", 
           target = "PRGD_IND", 
           expression = SColumn(col("ACCT_Lkp.PRGD_IND")), 
           description = ""
         ),          SColumnExpression(
           _row_id = "33178827", 
           target = "SRC_BANK_NBR_DESC", 
           expression = SColumn(col("ACCT_Lkp.SRC_BANK_NBR_DESC")), 
           description = ""
         ),          SColumnExpression(
           _row_id = "1709066765", 
           target = "RATE_INDX_ID", 
           expression = SColumn(col("ACCT_Lkp.RATE_INDX_ID")), 
           description = ""
         ),          SColumnExpression(
           _row_id = "1145856833", 
           target = "LAST_INT_RATE_CHG_DT", 
           expression = SColumn(col("ACCT_Lkp.LAST_INT_RATE_CHG_DT")), 
           description = ""
         ),          SColumnExpression(
           _row_id = "1646718397", 
           target = "GL_LDGR_ACCT_ID", 
           expression = SColumn(col("ACCT_Lkp.GL_LDGR_ACCT_ID")), 
           description = ""
         ),          SColumnExpression(
           _row_id = "1657539229", 
           target = "NEXT_STMT_DT", 
           expression = SColumn(col("ACCT_Lkp.NEXT_STMT_DT")), 
           description = ""
         ),          SColumnExpression(
           _row_id = "1296982438", 
           target = "SRC_BANK_NBR", 
           expression = SColumn(col("ACCT_Lkp.SRC_BANK_NBR")), 
           description = ""
         ),          SColumnExpression(
           _row_id = "88419528", 
           target = "SRC_ACCT_NBR", 
           expression = SColumn(col("ACCT_Lkp.SRC_ACCT_NBR")), 
           description = ""
         ),          SColumnExpression(
           _row_id = "387558911", 
           target = "SRC_DTL_STAT_DESC", 
           expression = SColumn(col("ACCT_Lkp.SRC_DTL_STAT_DESC")), 
           description = ""
         ),          SColumnExpression(
           _row_id = "1221110724", 
           target = "PRCS_DT", 
           expression = SColumn(col("ACCT_Lkp.PRCS_DT")), 
           description = ""
         ),          SColumnExpression(
           _row_id = "213506244", 
           target = "RNWL_DT", 
           expression = SColumn(col("ACCT_Lkp.RNWL_DT")), 
           description = ""
         ),          SColumnExpression(
           _row_id = "1206358555", 
           target = "ORIG_BANK_SHR_AMT", 
           expression = SColumn(col("ACCT_Lkp.ORIG_BANK_SHR_AMT")), 
           description = ""
         ),          SColumnExpression(
           _row_id = "1479963254", 
           target = "ORIG_TERM", 
           expression = SColumn(col("ACCT_Lkp.ORIG_TERM")), 
           description = ""
         ),          SColumnExpression(
           _row_id = "1322216515", 
           target = "TAX_ID_TYP", 
           expression = SColumn(col("ACCT_Lkp.TAX_ID_TYP")), 
           description = ""
         ),          SColumnExpression(
           _row_id = "722743496", 
           target = "EMP_CD", 
           expression = SColumn(col("ACCT_Lkp.EMP_CD")), 
           description = ""
         ),          SColumnExpression(
           _row_id = "308195483", 
           target = "INT_MRGN_ACTION_CD", 
           expression = SColumn(col("ACCT_Lkp.INT_MRGN_ACTION_CD")), 
           description = ""
         ),          SColumnExpression(
           _row_id = "1432558853", 
           target = "SRC_INT_REPRC_FREQ_CD", 
           expression = SColumn(col("ACCT_Lkp.SRC_INT_REPRC_FREQ_CD")), 
           description = ""
         ),          SColumnExpression(
           _row_id = "53659897", 
           target = "SRC_INT_ACCR_BASIS_CD", 
           expression = SColumn(col("ACCT_Lkp.SRC_INT_ACCR_BASIS_CD")), 
           description = ""
         ),          SColumnExpression(
           _row_id = "1995932296", 
           target = "SIC_CD", 
           expression = SColumn(col("ACCT_Lkp.SIC_CD")), 
           description = ""
         ),          SColumnExpression(
           _row_id = "1745700157", 
           target = "SRC_PRMY_OFCR_CD", 
           expression = SColumn(col("ACCT_Lkp.SRC_PRMY_OFCR_CD")), 
           description = ""
         ),          SColumnExpression(
           _row_id = "1605177466", 
           target = "CONVERTED_REC_OLD_ACCT_NBR", 
           expression = SColumn(col("ACCT_Lkp.CONVERTED_REC_OLD_ACCT_NBR")), 
           description = ""
         ),          SColumnExpression(
           _row_id = "168869615", 
           target = "MST_ACCT_ID", 
           expression = SColumn(col("ACCT_Lkp.MST_ACCT_ID")), 
           description = ""
         ),          SColumnExpression(
           _row_id = "2037301980", 
           target = "CUTOFF_STMT_TERM", 
           expression = SColumn(col("ACCT_Lkp.CUTOFF_STMT_TERM")), 
           description = ""
         ),          SColumnExpression(
           _row_id = "342688992", 
           target = "INT_RATE_RESET_DAY_CNT", 
           expression = SColumn(col("ACCT_Lkp.INT_RATE_RESET_DAY_CNT")), 
           description = ""
         ),          SColumnExpression(
           _row_id = "1341925544", 
           target = "RPT_FA_CC_ID", 
           expression = SColumn(col("ACCT_Lkp.RPT_FA_CC_ID")), 
           description = ""
         ),          SColumnExpression(
           _row_id = "1483985470", 
           target = "RPT_FA_LOB_ID", 
           expression = SColumn(col("ACCT_Lkp.RPT_FA_LOB_ID")), 
           description = ""
         ),          SColumnExpression(
           _row_id = "229658170", 
           target = "PREV_ODATE", 
           expression = SColumn(col("ODH_LKP.PREV_ODATE")), 
           description = ""
         ),          SColumnExpression(
           _row_id = "2068940388", 
           target = "CUR_BAL", 
           expression = SColumn(col("PASS_IN.CUR_BAL")), 
           description = ""
         ),          SColumnExpression(
           _row_id = "409836747", 
           target = "INT_INDX_BASE_RATE", 
           expression = SColumn(col("PASS_IN.INT_INDX_BASE_RATE")), 
           description = ""
         ),          SColumnExpression(
           _row_id = "313208538", 
           target = "MTD_HIGHEST_CUR_ACCT_BAL", 
           expression = SColumn(col("PASS_IN.MTD_HIGHEST_CUR_ACCT_BAL")), 
           description = ""
         ),          SColumnExpression(
           _row_id = "778423377", 
           target = "MTD_AGGR_BANK_SHR_BAL", 
           expression = SColumn(col("PASS_IN.MTD_AGGR_BANK_SHR_BAL")), 
           description = ""
         ),          SColumnExpression(
           _row_id = "1178028483", 
           target = "MTD_AVG_BANK_BAL", 
           expression = SColumn(col("PASS_IN.MTD_AVG_BANK_BAL")), 
           description = ""
         ),          SColumnExpression(
           _row_id = "1937939718", 
           target = "YTD_INT_ACCR_AMT", 
           expression = SColumn(col("PASS_IN.YTD_INT_ACCR_AMT")), 
           description = ""
         ),          SColumnExpression(
           _row_id = "729919840", 
           target = "YTD_AGGR_BANK_SHR_BAL", 
           expression = SColumn(col("PASS_IN.YTD_AGGR_BANK_SHR_BAL")), 
           description = ""
         ),          SColumnExpression(
           _row_id = "1666836392", 
           target = "BANK_SHR_INT_ACCR_BAL", 
           expression = SColumn(col("PASS_IN.BANK_SHR_INT_ACCR_BAL")), 
           description = ""
         ),          SColumnExpression(
           _row_id = "1725531867", 
           target = "GL_YTD_AGGR_CUR_BAL", 
           expression = SColumn(col("PASS_IN.GL_YTD_AGGR_CUR_BAL")), 
           description = ""
         ),          SColumnExpression(
           _row_id = "749685631", 
           target = "GL_MTD_AGGR_INT_AMT", 
           expression = SColumn(col("PASS_IN.GL_MTD_AGGR_INT_AMT")), 
           description = ""
         ),          SColumnExpression(
           _row_id = "1242164274", 
           target = "GL_MTD_AVG_BANK_BAL", 
           expression = SColumn(col("PASS_IN.GL_MTD_AVG_BANK_BAL")), 
           description = ""
         ),          SColumnExpression(
           _row_id = "1751345146", 
           target = "GL_QTD_AVG_CUR_BAL", 
           expression = SColumn(col("PASS_IN.GL_QTD_AVG_CUR_BAL")), 
           description = ""
         ),          SColumnExpression(
           _row_id = "190637152", 
           target = "OPEN_CLOSE_CD", 
           expression = SColumn(col("PASS_IN.OPEN_CLOSE_CD")), 
           description = ""
         )], 
        headAlias = "PASS_IN", 
        whereClause = None, 
        allIn0 = None, 
        allIn1 = None, 
        hints = []
    )
    in0 = PASS_IN
    in1 = ACCT_Lkp
    inDFs = [ODH_LKP]
    df_in0 = in0.alias(props.headAlias)

    if props.hints is not None and props.hints[0].hintType != "none":
        res = df_in0.hint(props.hints[0].hintType)
    else:
        res = df_in0

    propagate_cols = []

    if props.hints[0].propagateColumns:
        propagate_cols.append(col(props.hints[0].alias + ".*"))
    elif props.allIn0:
        propagate_cols.append(col("in0.*"))

    _inputs = [in1]
    _inputs.extend(inDFs)
    inputConditionPair = list(zip(_inputs, props.conditions))
    inputConditionPairWithHints = list(zip(inputConditionPair, props.hints[1:]))

    for pair in inputConditionPairWithHints:
        _pairCondition, _hint = pair
        inPort, _condition = _pairCondition

        if _hint.hintType == "none":
            nextDF = inPort.alias(_condition.alias)
        else:
            nextDF = inPort.hint(_hint.hintType).alias(_condition.alias)

        if _condition.joinType == "cross":
            res = res.crossJoin(nextDF)
        else:
            res = res.join(nextDF, _condition.expression.column(), _condition.joinType)

        if _hint.propagateColumns:
            propagate_cols.append(col(_hint.alias + ".*"))

        if _condition.alias == "in1" and props.allIn1:
            propagate_cols.append(col("in1.*"))

    if props.whereClause is None:
        resFiltered = res
    else:
        resFiltered = res.where(props.whereClause.column())

    if (  #skipEagerEvaluation
        len(props.expressions)
        > 0
    ):
        if (  #skipEagerEvaluation
            len(propagate_cols)
            > 0
        ):
            return resFiltered.select(*list(map(lambda x: x.column(), props.expressions)), *propagate_cols)
        else:
            return resFiltered.select(*list(map(lambda x: x.column(), props.expressions)))
    else:
        return resFiltered
