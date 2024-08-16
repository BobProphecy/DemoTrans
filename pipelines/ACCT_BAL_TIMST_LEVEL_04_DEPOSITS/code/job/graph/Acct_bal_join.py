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
           _row_id = "61683108", 
           target = "PAPER_STMT_CNT", 
           expression = SColumn(col("PASS_IN.PAPER_STMT_CNT")), 
           description = ""
         ),          SColumnExpression(
           _row_id = "775850926", 
           target = "TIMST_ACCOUNT", 
           expression = SColumn(col("PASS_IN.TIMST_ACCOUNT")), 
           description = ""
         ),          SColumnExpression(
           _row_id = "854326190", 
           target = "GL_QTD_AVG_BANK_SHR_BAL", 
           expression = SColumn(col("PASS_IN.GL_QTD_AVG_BANK_SHR_BAL")), 
           description = ""
         ),          SColumnExpression(
           _row_id = "1346131225", 
           target = "GL_MTD_AVG_CUR_BAL", 
           expression = SColumn(col("PASS_IN.GL_MTD_AVG_CUR_BAL")), 
           description = ""
         ),          SColumnExpression(
           _row_id = "1465280289", 
           target = "GL_YTD_AGGR_BANK_SHR_BAL", 
           expression = SColumn(col("PASS_IN.GL_YTD_AGGR_BANK_SHR_BAL")), 
           description = ""
         ),          SColumnExpression(
           _row_id = "1817159574", 
           target = "GL_MTD_AGGR_BANK_SHR_INT_AMT", 
           expression = SColumn(col("PASS_IN.GL_MTD_AGGR_BANK_SHR_INT_AMT")), 
           description = ""
         ),          SColumnExpression(
           _row_id = "355864977", 
           target = "INT_ACCR_BAL", 
           expression = SColumn(col("PASS_IN.INT_ACCR_BAL")), 
           description = ""
         ),          SColumnExpression(
           _row_id = "208336463", 
           target = "YTD_AVG_BAL", 
           expression = SColumn(col("PASS_IN.YTD_AVG_BAL")), 
           description = ""
         ),          SColumnExpression(
           _row_id = "2090144387", 
           target = "LAST_INT_REPRC_DT", 
           expression = SColumn(col("PASS_IN.LAST_INT_REPRC_DT")), 
           description = ""
         ),          SColumnExpression(
           _row_id = "181321331", 
           target = "MTD_INT_ACCR_AMT", 
           expression = SColumn(col("PASS_IN.MTD_INT_ACCR_AMT")), 
           description = ""
         ),          SColumnExpression(
           _row_id = "2083217246", 
           target = "NEXT_INT_REPRC_DT", 
           expression = SColumn(col("PASS_IN.NEXT_INT_REPRC_DT")), 
           description = ""
         ),          SColumnExpression(
           _row_id = "681889207", 
           target = "MTD_LOWEST_CUR_ACCT_BAL", 
           expression = SColumn(col("PASS_IN.MTD_LOWEST_CUR_ACCT_BAL")), 
           description = ""
         ),          SColumnExpression(
           _row_id = "609605588", 
           target = "INT_RATE", 
           expression = SColumn(col("PASS_IN.INT_RATE")), 
           description = ""
         ),          SColumnExpression(
           _row_id = "1242906935", 
           target = "AS_OF_DT", 
           expression = SColumn(col("PASS_IN.AS_OF_DT")), 
           description = ""
         ),          SColumnExpression(
           _row_id = "178861996", 
           target = "DATA_DATE", 
           expression = SColumn(col("ODH_LKP.DATA_DATE")), 
           description = ""
         ),          SColumnExpression(
           _row_id = "1601351766", 
           target = "RPT_FA_LOCTN_ID", 
           expression = SColumn(col("ACCT_Lkp.RPT_FA_LOCTN_ID")), 
           description = ""
         ),          SColumnExpression(
           _row_id = "900305406", 
           target = "RP_IND", 
           expression = SColumn(col("ACCT_Lkp.RP_IND")), 
           description = ""
         ),          SColumnExpression(
           _row_id = "1843875061", 
           target = "STMT_IMAGE_DESC", 
           expression = SColumn(col("ACCT_Lkp.STMT_IMAGE_DESC")), 
           description = ""
         ),          SColumnExpression(
           _row_id = "629656890", 
           target = "STMT_ACCT_NBR", 
           expression = SColumn(col("ACCT_Lkp.STMT_ACCT_NBR")), 
           description = ""
         ),          SColumnExpression(
           _row_id = "1468267304", 
           target = "SRC_CONVERTED_REC_CD", 
           expression = SColumn(col("ACCT_Lkp.SRC_CONVERTED_REC_CD")), 
           description = ""
         ),          SColumnExpression(
           _row_id = "312509902", 
           target = "NAICS_DESC", 
           expression = SColumn(col("ACCT_Lkp.NAICS_DESC")), 
           description = ""
         ),          SColumnExpression(
           _row_id = "335843271", 
           target = "SCNDY_LVL_INT_MRGN_ACTION_CD", 
           expression = SColumn(col("ACCT_Lkp.SCNDY_LVL_INT_MRGN_ACTION_CD")), 
           description = ""
         ),          SColumnExpression(
           _row_id = "82323789", 
           target = "INT_ACCR_BASIS_DESC", 
           expression = SColumn(col("ACCT_Lkp.INT_ACCR_BASIS_DESC")), 
           description = ""
         ),          SColumnExpression(
           _row_id = "1189053827", 
           target = "SRC_INT_REPRC_FREQ_TERM", 
           expression = SColumn(col("ACCT_Lkp.SRC_INT_REPRC_FREQ_TERM")), 
           description = ""
         ),          SColumnExpression(
           _row_id = "386737833", 
           target = "INT_INDX_CD", 
           expression = SColumn(col("ACCT_Lkp.INT_INDX_CD")), 
           description = ""
         ),          SColumnExpression(
           _row_id = "1818854488", 
           target = "INT_RATE_TYP", 
           expression = SColumn(col("ACCT_Lkp.INT_RATE_TYP")), 
           description = ""
         ),          SColumnExpression(
           _row_id = "658336153", 
           target = "SRC_EMP_CD", 
           expression = SColumn(col("ACCT_Lkp.SRC_EMP_CD")), 
           description = ""
         ),          SColumnExpression(
           _row_id = "1232419311", 
           target = "SHORT_NAME", 
           expression = SColumn(col("ACCT_Lkp.SHORT_NAME")), 
           description = ""
         ),          SColumnExpression(
           _row_id = "2125513491", 
           target = "CUR_TERM", 
           expression = SColumn(col("ACCT_Lkp.CUR_TERM")), 
           description = ""
         ),          SColumnExpression(
           _row_id = "1680659627", 
           target = "MAT_DT", 
           expression = SColumn(col("ACCT_Lkp.MAT_DT")), 
           description = ""
         ),          SColumnExpression(
           _row_id = "1521525965", 
           target = "INT_BEG_DT", 
           expression = SColumn(col("ACCT_Lkp.INT_BEG_DT")), 
           description = ""
         ),          SColumnExpression(
           _row_id = "1174499938", 
           target = "OPEN_DT", 
           expression = SColumn(col("ACCT_Lkp.OPEN_DT")), 
           description = ""
         ),          SColumnExpression(
           _row_id = "1337752194", 
           target = "OPEN_CLOSE_DESC", 
           expression = SColumn(col("ACCT_Lkp.OPEN_CLOSE_DESC")), 
           description = ""
         ),          SColumnExpression(
           _row_id = "902022978", 
           target = "APPL_CD", 
           expression = SColumn(col("ACCT_Lkp.APPL_CD")), 
           description = ""
         ),          SColumnExpression(
           _row_id = "877440715", 
           target = "RPT_CC_ID", 
           expression = SColumn(col("ACCT_Lkp.RPT_CC_ID")), 
           description = ""
         ),          SColumnExpression(
           _row_id = "2025517592", 
           target = "NEXT_INT_RATE_CHG_DT", 
           expression = SColumn(col("ACCT_Lkp.NEXT_INT_RATE_CHG_DT")), 
           description = ""
         ),          SColumnExpression(
           _row_id = "1018945288", 
           target = "COMP_ID", 
           expression = SColumn(col("ACCT_Lkp.COMP_ID")), 
           description = ""
         ),          SColumnExpression(
           _row_id = "111070390", 
           target = "LAST_STMT_DT", 
           expression = SColumn(col("ACCT_Lkp.LAST_STMT_DT")), 
           description = ""
         ),          SColumnExpression(
           _row_id = "1652163002", 
           target = "ACCT_TYP", 
           expression = SColumn(col("ACCT_Lkp.ACCT_TYP")), 
           description = ""
         ),          SColumnExpression(
           _row_id = "756762121", 
           target = "APPL_DESC", 
           expression = SColumn(col("ACCT_Lkp.APPL_DESC")), 
           description = ""
         ),          SColumnExpression(
           _row_id = "1974246631", 
           target = "SRC_DTL_STAT_CD", 
           expression = SColumn(col("ACCT_Lkp.SRC_DTL_STAT_CD")), 
           description = ""
         ),          SColumnExpression(
           _row_id = "885894390", 
           target = "CLOSE_DT", 
           expression = SColumn(col("ACCT_Lkp.CLOSE_DT")), 
           description = ""
         ),          SColumnExpression(
           _row_id = "393009655", 
           target = "RNWL_PRCS_DT", 
           expression = SColumn(col("ACCT_Lkp.RNWL_PRCS_DT")), 
           description = ""
         ),          SColumnExpression(
           _row_id = "1022716490", 
           target = "OPEN_AMT", 
           expression = SColumn(col("ACCT_Lkp.OPEN_AMT")), 
           description = ""
         ),          SColumnExpression(
           _row_id = "535164010", 
           target = "INT_MRGN_RATE", 
           expression = SColumn(col("ACCT_Lkp.INT_MRGN_RATE")), 
           description = ""
         ),          SColumnExpression(
           _row_id = "880415283", 
           target = "SRC_TAX_ID_TYP", 
           expression = SColumn(col("ACCT_Lkp.SRC_TAX_ID_TYP")), 
           description = ""
         ),          SColumnExpression(
           _row_id = "1351186166", 
           target = "SRC_EMP_DESC", 
           expression = SColumn(col("ACCT_Lkp.SRC_EMP_DESC")), 
           description = ""
         ),          SColumnExpression(
           _row_id = "385209983", 
           target = "SRC_INT_MRGN_ACTION_CD", 
           expression = SColumn(col("ACCT_Lkp.SRC_INT_MRGN_ACTION_CD")), 
           description = ""
         ),          SColumnExpression(
           _row_id = "1685663786", 
           target = "INT_INDX_DESC", 
           expression = SColumn(col("ACCT_Lkp.INT_INDX_DESC")), 
           description = ""
         ),          SColumnExpression(
           _row_id = "63375001", 
           target = "INT_REPRC_FREQ_TERM", 
           expression = SColumn(col("ACCT_Lkp.INT_REPRC_FREQ_TERM")), 
           description = ""
         ),          SColumnExpression(
           _row_id = "49210080", 
           target = "NAICS_CD", 
           expression = SColumn(col("ACCT_Lkp.NAICS_CD")), 
           description = ""
         ),          SColumnExpression(
           _row_id = "1860850690", 
           target = "SRC_RPT_BRNCH_NBR", 
           expression = SColumn(col("ACCT_Lkp.SRC_RPT_BRNCH_NBR")), 
           description = ""
         ),          SColumnExpression(
           _row_id = "1400846562", 
           target = "RPT_REC_IND", 
           expression = SColumn(col("ACCT_Lkp.RPT_REC_IND")), 
           description = ""
         ),          SColumnExpression(
           _row_id = "1549667179", 
           target = "GL_CC_ID", 
           expression = SColumn(col("ACCT_Lkp.GL_CC_ID")), 
           description = ""
         ),          SColumnExpression(
           _row_id = "760349766", 
           target = "CUTOFF_STMT_FREQ_CD", 
           expression = SColumn(col("ACCT_Lkp.CUTOFF_STMT_FREQ_CD")), 
           description = ""
         ),          SColumnExpression(
           _row_id = "1380647972", 
           target = "PROD_DTL_ID", 
           expression = SColumn(col("ACCT_Lkp.PROD_DTL_ID")), 
           description = ""
         ),          SColumnExpression(
           _row_id = "1281672396", 
           target = "ENOTICE_IND", 
           expression = SColumn(col("ACCT_Lkp.ENOTICE_IND")), 
           description = ""
         ),          SColumnExpression(
           _row_id = "160973244", 
           target = "RPT_FA_LOCTN_EFF_DT", 
           expression = SColumn(col("ACCT_Lkp.RPT_FA_LOCTN_EFF_DT")), 
           description = ""
         ),          SColumnExpression(
           _row_id = "722804985", 
           target = "ODATE", 
           expression = SColumn(col("ODH_LKP.ODATE")), 
           description = ""
         ),          SColumnExpression(
           _row_id = "85988013", 
           target = "DATA_DT", 
           expression = SColumn(col("PASS_IN.DATA_DT")), 
           description = ""
         ),          SColumnExpression(
           _row_id = "1224200972", 
           target = "INT_DT", 
           expression = SColumn(col("PASS_IN.INT_DT")), 
           description = ""
         ),          SColumnExpression(
           _row_id = "2013268804", 
           target = "YTD_LOWEST_CUR_ACCT_BAL", 
           expression = SColumn(col("PASS_IN.YTD_LOWEST_CUR_ACCT_BAL")), 
           description = ""
         ),          SColumnExpression(
           _row_id = "690107470", 
           target = "ENTRY_DT", 
           expression = SColumn(col("PASS_IN.ENTRY_DT")), 
           description = ""
         ),          SColumnExpression(
           _row_id = "2047346687", 
           target = "MTD_AVG_BAL", 
           expression = SColumn(col("PASS_IN.MTD_AVG_BAL")), 
           description = ""
         ),          SColumnExpression(
           _row_id = "1444300643", 
           target = "MTD_AGGR_INT_RATE", 
           expression = SColumn(col("PASS_IN.MTD_AGGR_INT_RATE")), 
           description = ""
         ),          SColumnExpression(
           _row_id = "359195250", 
           target = "YTD_AVG_BANK_SHR_BAL", 
           expression = SColumn(col("PASS_IN.YTD_AVG_BANK_SHR_BAL")), 
           description = ""
         ),          SColumnExpression(
           _row_id = "1863890447", 
           target = "BANK_SHR_INT_PERDIEM_AMT", 
           expression = SColumn(col("PASS_IN.BANK_SHR_INT_PERDIEM_AMT")), 
           description = ""
         ),          SColumnExpression(
           _row_id = "850606838", 
           target = "GL_MTD_AGGR_BANK_SHR_BAL", 
           expression = SColumn(col("PASS_IN.GL_MTD_AGGR_BANK_SHR_BAL")), 
           description = ""
         ),          SColumnExpression(
           _row_id = "2054299818", 
           target = "GL_YTD_AGGR_INT_AMT", 
           expression = SColumn(col("PASS_IN.GL_YTD_AGGR_INT_AMT")), 
           description = ""
         ),          SColumnExpression(
           _row_id = "1262035847", 
           target = "GL_YTD_AVG_BANK_SHR_BAL", 
           expression = SColumn(col("PASS_IN.GL_YTD_AVG_BANK_SHR_BAL")), 
           description = ""
         ),          SColumnExpression(
           _row_id = "776628216", 
           target = "GL_QTD_AGGR_CUR_BAL", 
           expression = SColumn(col("PASS_IN.GL_QTD_AGGR_CUR_BAL")), 
           description = ""
         ),          SColumnExpression(
           _row_id = "1121716924", 
           target = "TIMST_INTRATE", 
           expression = SColumn(col("PASS_IN.TIMST_INTRATE")), 
           description = ""
         ),          SColumnExpression(
           _row_id = "150270233", 
           target = "TIMST_INTINDEX", 
           expression = SColumn(col("PASS_IN.TIMST_INTINDEX")), 
           description = ""
         ),          SColumnExpression(
           _row_id = "1389174467", 
           target = "RptRecInd", 
           expression = SColumn(col("PASS_IN.RptRecInd")), 
           description = ""
         ),          SColumnExpression(
           _row_id = "313392808", 
           target = "ESTMT_CNT", 
           expression = SColumn(col("PASS_IN.ESTMT_CNT")), 
           description = ""
         ),          SColumnExpression(
           _row_id = "178778540", 
           target = "TIMST_INST", 
           expression = SColumn(col("PASS_IN.TIMST_INST")), 
           description = ""
         ),          SColumnExpression(
           _row_id = "1535245528", 
           target = "GL_QTD_AGGR_BANK_SHR_BAL", 
           expression = SColumn(col("PASS_IN.GL_QTD_AGGR_BANK_SHR_BAL")), 
           description = ""
         ),          SColumnExpression(
           _row_id = "325558360", 
           target = "GL_YTD_AVG_CUR_BAL", 
           expression = SColumn(col("PASS_IN.GL_YTD_AVG_CUR_BAL")), 
           description = ""
         ),          SColumnExpression(
           _row_id = "1171938164", 
           target = "GL_MTD_AGGR_CUR_BAL", 
           expression = SColumn(col("PASS_IN.GL_MTD_AGGR_CUR_BAL")), 
           description = ""
         ),          SColumnExpression(
           _row_id = "1937583636", 
           target = "GL_YTD_AGGR_BANK_SHR_INT_AMT", 
           expression = SColumn(col("PASS_IN.GL_YTD_AGGR_BANK_SHR_INT_AMT")), 
           description = ""
         ),          SColumnExpression(
           _row_id = "1532636307", 
           target = "YTD_AGGR_INT_RATE", 
           expression = SColumn(col("PASS_IN.YTD_AGGR_INT_RATE")), 
           description = ""
         ),          SColumnExpression(
           _row_id = "1155940706", 
           target = "YTD_AGGR_BAL", 
           expression = SColumn(col("PASS_IN.YTD_AGGR_BAL")), 
           description = ""
         ),          SColumnExpression(
           _row_id = "962766798", 
           target = "MTD_AVG_WT_INT_RATE", 
           expression = SColumn(col("PASS_IN.MTD_AVG_WT_INT_RATE")), 
           description = ""
         ),          SColumnExpression(
           _row_id = "1366224331", 
           target = "MTD_AGGR_BAL", 
           expression = SColumn(col("PASS_IN.MTD_AGGR_BAL")), 
           description = ""
         ),          SColumnExpression(
           _row_id = "1965000680", 
           target = "YTD_HIGHEST_CUR_ACCT_BAL", 
           expression = SColumn(col("PASS_IN.YTD_HIGHEST_CUR_ACCT_BAL")), 
           description = ""
         ),          SColumnExpression(
           _row_id = "1269740115", 
           target = "INT_PERDIEM_AMT", 
           expression = SColumn(col("PASS_IN.INT_PERDIEM_AMT")), 
           description = ""
         ),          SColumnExpression(
           _row_id = "1180330663", 
           target = "BANK_SHR_BAL", 
           expression = SColumn(col("PASS_IN.BANK_SHR_BAL")), 
           description = ""
         ),          SColumnExpression(
           _row_id = "324776598", 
           target = "ACCT_ID", 
           expression = SColumn(col("PASS_IN.ACCT_ID")), 
           description = ""
         ),          SColumnExpression(
           _row_id = "1275834876", 
           target = "RPT_FA_LOB_EFF_DT", 
           expression = SColumn(col("ACCT_Lkp.RPT_FA_LOB_EFF_DT")), 
           description = ""
         ),          SColumnExpression(
           _row_id = "1071261888", 
           target = "RPT_FA_CC_EFF_DT", 
           expression = SColumn(col("ACCT_Lkp.RPT_FA_CC_EFF_DT")), 
           description = ""
         ),          SColumnExpression(
           _row_id = "839320873", 
           target = "ALLOW_RP_IND", 
           expression = SColumn(col("ACCT_Lkp.ALLOW_RP_IND")), 
           description = ""
         ),          SColumnExpression(
           _row_id = "953048173", 
           target = "STMT_IMAGE_CD", 
           expression = SColumn(col("ACCT_Lkp.STMT_IMAGE_CD")), 
           description = ""
         ),          SColumnExpression(
           _row_id = "1416106923", 
           target = "MST_ACCT_EFF_DT", 
           expression = SColumn(col("ACCT_Lkp.MST_ACCT_EFF_DT")), 
           description = ""
         ),          SColumnExpression(
           _row_id = "1130406626", 
           target = "CONVERTED_REC_CD", 
           expression = SColumn(col("ACCT_Lkp.CONVERTED_REC_CD")), 
           description = ""
         ),          SColumnExpression(
           _row_id = "464915136", 
           target = "SRC_SCNDY_OFCR_CD", 
           expression = SColumn(col("ACCT_Lkp.SRC_SCNDY_OFCR_CD")), 
           description = ""
         ),          SColumnExpression(
           _row_id = "1113030131", 
           target = "SCNDY_LVL_INT_MRGN_RATE", 
           expression = SColumn(col("ACCT_Lkp.SCNDY_LVL_INT_MRGN_RATE")), 
           description = ""
         ),          SColumnExpression(
           _row_id = "1062870009", 
           target = "INT_ACCR_BASIS_CD", 
           expression = SColumn(col("ACCT_Lkp.INT_ACCR_BASIS_CD")), 
           description = ""
         ),          SColumnExpression(
           _row_id = "498631198", 
           target = "SRC_INT_REPRC_DESC", 
           expression = SColumn(col("ACCT_Lkp.SRC_INT_REPRC_DESC")), 
           description = ""
         ),          SColumnExpression(
           _row_id = "798883239", 
           target = "SRC_INT_INDX_CD", 
           expression = SColumn(col("ACCT_Lkp.SRC_INT_INDX_CD")), 
           description = ""
         ),          SColumnExpression(
           _row_id = "1558888213", 
           target = "EMP_DESC", 
           expression = SColumn(col("ACCT_Lkp.EMP_DESC")), 
           description = ""
         ),          SColumnExpression(
           _row_id = "1515704535", 
           target = "TIN", 
           expression = SColumn(col("ACCT_Lkp.TIN")), 
           description = ""
         ),          SColumnExpression(
           _row_id = "749751175", 
           target = "ORIG_INT_INDX_BASE_RATE", 
           expression = SColumn(col("ACCT_Lkp.ORIG_INT_INDX_BASE_RATE")), 
           description = ""
         ),          SColumnExpression(
           _row_id = "1249962950", 
           target = "ORIG_INT_RATE", 
           expression = SColumn(col("ACCT_Lkp.ORIG_INT_RATE")), 
           description = ""
         ),          SColumnExpression(
           _row_id = "1068993517", 
           target = "ORIG_MAT_DT", 
           expression = SColumn(col("ACCT_Lkp.ORIG_MAT_DT")), 
           description = ""
         ),          SColumnExpression(
           _row_id = "839772127", 
           target = "ORIG_INT_BEG_DT", 
           expression = SColumn(col("ACCT_Lkp.ORIG_INT_BEG_DT")), 
           description = ""
         ),          SColumnExpression(
           _row_id = "927602359", 
           target = "ORIG_OPEN_DT", 
           expression = SColumn(col("ACCT_Lkp.ORIG_OPEN_DT")), 
           description = ""
         ),          SColumnExpression(
           _row_id = "1445173394", 
           target = "PRGD_IND", 
           expression = SColumn(col("ACCT_Lkp.PRGD_IND")), 
           description = ""
         ),          SColumnExpression(
           _row_id = "1812151290", 
           target = "SRC_BANK_NBR_DESC", 
           expression = SColumn(col("ACCT_Lkp.SRC_BANK_NBR_DESC")), 
           description = ""
         ),          SColumnExpression(
           _row_id = "668753511", 
           target = "RATE_INDX_ID", 
           expression = SColumn(col("ACCT_Lkp.RATE_INDX_ID")), 
           description = ""
         ),          SColumnExpression(
           _row_id = "1590175690", 
           target = "LAST_INT_RATE_CHG_DT", 
           expression = SColumn(col("ACCT_Lkp.LAST_INT_RATE_CHG_DT")), 
           description = ""
         ),          SColumnExpression(
           _row_id = "1293357721", 
           target = "GL_LDGR_ACCT_ID", 
           expression = SColumn(col("ACCT_Lkp.GL_LDGR_ACCT_ID")), 
           description = ""
         ),          SColumnExpression(
           _row_id = "1734512017", 
           target = "NEXT_STMT_DT", 
           expression = SColumn(col("ACCT_Lkp.NEXT_STMT_DT")), 
           description = ""
         ),          SColumnExpression(
           _row_id = "1428583455", 
           target = "SRC_BANK_NBR", 
           expression = SColumn(col("ACCT_Lkp.SRC_BANK_NBR")), 
           description = ""
         ),          SColumnExpression(
           _row_id = "1492324428", 
           target = "SRC_ACCT_NBR", 
           expression = SColumn(col("ACCT_Lkp.SRC_ACCT_NBR")), 
           description = ""
         ),          SColumnExpression(
           _row_id = "1318207573", 
           target = "SRC_DTL_STAT_DESC", 
           expression = SColumn(col("ACCT_Lkp.SRC_DTL_STAT_DESC")), 
           description = ""
         ),          SColumnExpression(
           _row_id = "1229841489", 
           target = "PRCS_DT", 
           expression = SColumn(col("ACCT_Lkp.PRCS_DT")), 
           description = ""
         ),          SColumnExpression(
           _row_id = "525310969", 
           target = "RNWL_DT", 
           expression = SColumn(col("ACCT_Lkp.RNWL_DT")), 
           description = ""
         ),          SColumnExpression(
           _row_id = "1677619769", 
           target = "ORIG_BANK_SHR_AMT", 
           expression = SColumn(col("ACCT_Lkp.ORIG_BANK_SHR_AMT")), 
           description = ""
         ),          SColumnExpression(
           _row_id = "1059250471", 
           target = "ORIG_TERM", 
           expression = SColumn(col("ACCT_Lkp.ORIG_TERM")), 
           description = ""
         ),          SColumnExpression(
           _row_id = "1841477874", 
           target = "TAX_ID_TYP", 
           expression = SColumn(col("ACCT_Lkp.TAX_ID_TYP")), 
           description = ""
         ),          SColumnExpression(
           _row_id = "149258382", 
           target = "EMP_CD", 
           expression = SColumn(col("ACCT_Lkp.EMP_CD")), 
           description = ""
         ),          SColumnExpression(
           _row_id = "686266798", 
           target = "INT_MRGN_ACTION_CD", 
           expression = SColumn(col("ACCT_Lkp.INT_MRGN_ACTION_CD")), 
           description = ""
         ),          SColumnExpression(
           _row_id = "1959768787", 
           target = "SRC_INT_REPRC_FREQ_CD", 
           expression = SColumn(col("ACCT_Lkp.SRC_INT_REPRC_FREQ_CD")), 
           description = ""
         ),          SColumnExpression(
           _row_id = "457727227", 
           target = "SRC_INT_ACCR_BASIS_CD", 
           expression = SColumn(col("ACCT_Lkp.SRC_INT_ACCR_BASIS_CD")), 
           description = ""
         ),          SColumnExpression(
           _row_id = "2118520693", 
           target = "SIC_CD", 
           expression = SColumn(col("ACCT_Lkp.SIC_CD")), 
           description = ""
         ),          SColumnExpression(
           _row_id = "114092338", 
           target = "SRC_PRMY_OFCR_CD", 
           expression = SColumn(col("ACCT_Lkp.SRC_PRMY_OFCR_CD")), 
           description = ""
         ),          SColumnExpression(
           _row_id = "1585129025", 
           target = "CONVERTED_REC_OLD_ACCT_NBR", 
           expression = SColumn(col("ACCT_Lkp.CONVERTED_REC_OLD_ACCT_NBR")), 
           description = ""
         ),          SColumnExpression(
           _row_id = "980235512", 
           target = "MST_ACCT_ID", 
           expression = SColumn(col("ACCT_Lkp.MST_ACCT_ID")), 
           description = ""
         ),          SColumnExpression(
           _row_id = "2023699141", 
           target = "CUTOFF_STMT_TERM", 
           expression = SColumn(col("ACCT_Lkp.CUTOFF_STMT_TERM")), 
           description = ""
         ),          SColumnExpression(
           _row_id = "250336841", 
           target = "INT_RATE_RESET_DAY_CNT", 
           expression = SColumn(col("ACCT_Lkp.INT_RATE_RESET_DAY_CNT")), 
           description = ""
         ),          SColumnExpression(
           _row_id = "329788810", 
           target = "RPT_FA_CC_ID", 
           expression = SColumn(col("ACCT_Lkp.RPT_FA_CC_ID")), 
           description = ""
         ),          SColumnExpression(
           _row_id = "2076336655", 
           target = "RPT_FA_LOB_ID", 
           expression = SColumn(col("ACCT_Lkp.RPT_FA_LOB_ID")), 
           description = ""
         ),          SColumnExpression(
           _row_id = "137070084", 
           target = "PREV_ODATE", 
           expression = SColumn(col("ODH_LKP.PREV_ODATE")), 
           description = ""
         ),          SColumnExpression(
           _row_id = "1027799749", 
           target = "CUR_BAL", 
           expression = SColumn(col("PASS_IN.CUR_BAL")), 
           description = ""
         ),          SColumnExpression(
           _row_id = "865391639", 
           target = "INT_INDX_BASE_RATE", 
           expression = SColumn(col("PASS_IN.INT_INDX_BASE_RATE")), 
           description = ""
         ),          SColumnExpression(
           _row_id = "1108124297", 
           target = "MTD_HIGHEST_CUR_ACCT_BAL", 
           expression = SColumn(col("PASS_IN.MTD_HIGHEST_CUR_ACCT_BAL")), 
           description = ""
         ),          SColumnExpression(
           _row_id = "1762300285", 
           target = "MTD_AGGR_BANK_SHR_BAL", 
           expression = SColumn(col("PASS_IN.MTD_AGGR_BANK_SHR_BAL")), 
           description = ""
         ),          SColumnExpression(
           _row_id = "1014823646", 
           target = "MTD_AVG_BANK_BAL", 
           expression = SColumn(col("PASS_IN.MTD_AVG_BANK_BAL")), 
           description = ""
         ),          SColumnExpression(
           _row_id = "2010088756", 
           target = "YTD_INT_ACCR_AMT", 
           expression = SColumn(col("PASS_IN.YTD_INT_ACCR_AMT")), 
           description = ""
         ),          SColumnExpression(
           _row_id = "1015993866", 
           target = "YTD_AGGR_BANK_SHR_BAL", 
           expression = SColumn(col("PASS_IN.YTD_AGGR_BANK_SHR_BAL")), 
           description = ""
         ),          SColumnExpression(
           _row_id = "1397884501", 
           target = "BANK_SHR_INT_ACCR_BAL", 
           expression = SColumn(col("PASS_IN.BANK_SHR_INT_ACCR_BAL")), 
           description = ""
         ),          SColumnExpression(
           _row_id = "1600417461", 
           target = "GL_YTD_AGGR_CUR_BAL", 
           expression = SColumn(col("PASS_IN.GL_YTD_AGGR_CUR_BAL")), 
           description = ""
         ),          SColumnExpression(
           _row_id = "1159648875", 
           target = "GL_MTD_AGGR_INT_AMT", 
           expression = SColumn(col("PASS_IN.GL_MTD_AGGR_INT_AMT")), 
           description = ""
         ),          SColumnExpression(
           _row_id = "1158391026", 
           target = "GL_MTD_AVG_BANK_BAL", 
           expression = SColumn(col("PASS_IN.GL_MTD_AVG_BANK_BAL")), 
           description = ""
         ),          SColumnExpression(
           _row_id = "369818215", 
           target = "GL_QTD_AVG_CUR_BAL", 
           expression = SColumn(col("PASS_IN.GL_QTD_AVG_CUR_BAL")), 
           description = ""
         ),          SColumnExpression(
           _row_id = "1037560736", 
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
