from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from job.config.ConfigStore import *
from job.udfs.UDFs import *

def Acct_bal_Final_join(spark: SparkSession, ACCT_BAL_OUT: DataFrame, ABCDDH_LKP: DataFrame, ) -> DataFrame:
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
           alias = "ABCDDH_LKP", 
           expression = SColumn(
             (
               (col("ABCDDH_LKP.ACCT_ID") == col("ACCT_BAL_OUT.ACCT_ID"))
               & (col("ABCDDH_LKP.ODATE") == col("ACCT_BAL_OUT.ODATE"))
             )
           ), 
           joinType = "inner"
         )], 
        expressions = [SColumnExpression(
           _row_id = "1070992914", 
           target = "GL_QTD_AVG_CUR_BAL", 
           expression = SColumn(col("ABCDDH_LKP.GL_QTD_AVG_CUR_BAL")), 
           description = ""
         ),          SColumnExpression(
           _row_id = "376896663", 
           target = "GL_QTD_AVG_BANK_SHR_BAL", 
           expression = SColumn(col("ABCDDH_LKP.GL_QTD_AVG_BANK_SHR_BAL")), 
           description = ""
         ),          SColumnExpression(
           _row_id = "946503784", 
           target = "AS_OF_DT", 
           expression = SColumn(col("ABCDDH_LKP.AS_OF_DT")), 
           description = ""
         ),          SColumnExpression(
           _row_id = "731229964", 
           target = "APPL_CD", 
           expression = SColumn(col("ABCDDH_LKP.APPL_CD")), 
           description = ""
         ),          SColumnExpression(
           _row_id = "1191348252", 
           target = "INT_PERDIEM_AMT", 
           expression = SColumn(col("ABCDDH_LKP.INT_PERDIEM_AMT")), 
           description = ""
         ),          SColumnExpression(
           _row_id = "1109229407", 
           target = "CUR_BAL", 
           expression = SColumn(col("ABCDDH_LKP.CUR_BAL")), 
           description = ""
         ),          SColumnExpression(
           _row_id = "1408803734", 
           target = "GL_YTD_AVG_CUR_BAL", 
           expression = SColumn(col("ABCDDH_LKP.GL_YTD_AVG_CUR_BAL")), 
           description = ""
         ),          SColumnExpression(
           _row_id = "589429710", 
           target = "GL_YTD_AGGR_CUR_BAL", 
           expression = SColumn(col("ABCDDH_LKP.GL_YTD_AGGR_CUR_BAL")), 
           description = ""
         ),          SColumnExpression(
           _row_id = "1489621202", 
           target = "GL_YTD_AGGR_BANK_SHR_INT_AMT", 
           expression = SColumn(col("ABCDDH_LKP.GL_YTD_AGGR_BANK_SHR_INT_AMT")), 
           description = ""
         ),          SColumnExpression(
           _row_id = "1386710274", 
           target = "GL_MTD_AVG_CUR_BAL", 
           expression = SColumn(col("ABCDDH_LKP.GL_MTD_AVG_CUR_BAL")), 
           description = ""
         ),          SColumnExpression(
           _row_id = "1077900966", 
           target = "GL_MTD_AGGR_CUR_BAL", 
           expression = SColumn(col("ABCDDH_LKP.GL_MTD_AGGR_CUR_BAL")), 
           description = ""
         ),          SColumnExpression(
           _row_id = "699917867", 
           target = "GL_MTD_AGGR_BANK_SHR_INT_AMT", 
           expression = SColumn(col("ABCDDH_LKP.GL_MTD_AGGR_BANK_SHR_INT_AMT")), 
           description = ""
         ),          SColumnExpression(
           _row_id = "835467625", 
           target = "YTD_LOWEST_CUR_ACCT_BAL", 
           expression = SColumn(col("ABCDDH_LKP.YTD_LOWEST_CUR_ACCT_BAL")), 
           description = ""
         ),          SColumnExpression(
           _row_id = "1893581528", 
           target = "MTD_LOWEST_CUR_ACCT_BAL", 
           expression = SColumn(col("ABCDDH_LKP.MTD_LOWEST_CUR_ACCT_BAL")), 
           description = ""
         ),          SColumnExpression(
           _row_id = "2081107836", 
           target = "ACCT_ID", 
           expression = SColumn(col("ABCDDH_LKP.ACCT_ID")), 
           description = ""
         ),          SColumnExpression(
           _row_id = "941256235", 
           target = "ESTMT_CNT", 
           expression = SColumn(col("ACCT_BAL_OUT.ESTMT_CNT")), 
           description = ""
         ),          SColumnExpression(
           _row_id = "1407212418", 
           target = "INT_ACCR_BAL", 
           expression = SColumn(col("ACCT_BAL_OUT.INT_ACCR_BAL")), 
           description = ""
         ),          SColumnExpression(
           _row_id = "1394657645", 
           target = "MTD_AGGR_INT_RATE", 
           expression = SColumn(col("ACCT_BAL_OUT.MTD_AGGR_INT_RATE")), 
           description = ""
         ),          SColumnExpression(
           _row_id = "172007015", 
           target = "MTD_AVG_WT_INT_RATE", 
           expression = SColumn(col("ACCT_BAL_OUT.MTD_AVG_WT_INT_RATE")), 
           description = ""
         ),          SColumnExpression(
           _row_id = "1564717169", 
           target = "INT_INDX_BASE_RATE", 
           expression = SColumn(col("ACCT_BAL_OUT.INT_INDX_BASE_RATE")), 
           description = ""
         ),          SColumnExpression(
           _row_id = "1089898479", 
           target = "INT_RATE", 
           expression = SColumn(col("ACCT_BAL_OUT.INT_RATE")), 
           description = ""
         ),          SColumnExpression(
           _row_id = "1494321581", 
           target = "DATA_DT", 
           expression = SColumn(col("ACCT_BAL_OUT.DATA_DT")), 
           description = ""
         ),          SColumnExpression(
           _row_id = "511938347", 
           target = "INT_DT", 
           expression = SColumn(col("ACCT_BAL_OUT.INT_DT")), 
           description = ""
         ),          SColumnExpression(
           _row_id = "504717260", 
           target = "NEXT_INT_REPRC_DT", 
           expression = SColumn(col("ACCT_BAL_OUT.NEXT_INT_REPRC_DT")), 
           description = ""
         ),          SColumnExpression(
           _row_id = "227995848", 
           target = "LAST_INT_REPRC_DT", 
           expression = SColumn(col("ACCT_BAL_OUT.LAST_INT_REPRC_DT")), 
           description = ""
         ),          SColumnExpression(
           _row_id = "874109381", 
           target = "YTD_AGGR_INT_RATE", 
           expression = SColumn(col("ACCT_BAL_OUT.YTD_AGGR_INT_RATE")), 
           description = ""
         ),          SColumnExpression(
           _row_id = "835154747", 
           target = "BANK_SHR_INT_ACCR_BAL", 
           expression = SColumn(col("ACCT_BAL_OUT.BANK_SHR_INT_ACCR_BAL")), 
           description = ""
         ),          SColumnExpression(
           _row_id = "57197438", 
           target = "PAPER_STMT_CNT", 
           expression = SColumn(col("ACCT_BAL_OUT.PAPER_STMT_CNT")), 
           description = ""
         ),          SColumnExpression(
           _row_id = "1768632092", 
           target = "ODATE", 
           expression = SColumn(col("ABCDDH_LKP.ODATE")), 
           description = ""
         ),          SColumnExpression(
           _row_id = "1169310576", 
           target = "MTD_HIGHEST_CUR_ACCT_BAL", 
           expression = SColumn(col("ABCDDH_LKP.MTD_HIGHEST_CUR_ACCT_BAL")), 
           description = ""
         ),          SColumnExpression(
           _row_id = "1045653457", 
           target = "YTD_HIGHEST_CUR_ACCT_BAL", 
           expression = SColumn(col("ABCDDH_LKP.YTD_HIGHEST_CUR_ACCT_BAL")), 
           description = ""
         ),          SColumnExpression(
           _row_id = "1683013454", 
           target = "GL_MTD_AGGR_BANK_SHR_BAL", 
           expression = SColumn(col("ABCDDH_LKP.GL_MTD_AGGR_BANK_SHR_BAL")), 
           description = ""
         ),          SColumnExpression(
           _row_id = "1518850794", 
           target = "GL_MTD_AGGR_INT_AMT", 
           expression = SColumn(col("ABCDDH_LKP.GL_MTD_AGGR_INT_AMT")), 
           description = ""
         ),          SColumnExpression(
           _row_id = "1317414573", 
           target = "GL_MTD_AVG_BANK_BAL", 
           expression = SColumn(col("ABCDDH_LKP.GL_MTD_AVG_BANK_BAL")), 
           description = ""
         ),          SColumnExpression(
           _row_id = "76413279", 
           target = "GL_YTD_AGGR_BANK_SHR_BAL", 
           expression = SColumn(col("ABCDDH_LKP.GL_YTD_AGGR_BANK_SHR_BAL")), 
           description = ""
         ),          SColumnExpression(
           _row_id = "955755391", 
           target = "GL_YTD_AGGR_INT_AMT", 
           expression = SColumn(col("ABCDDH_LKP.GL_YTD_AGGR_INT_AMT")), 
           description = ""
         ),          SColumnExpression(
           _row_id = "1939586540", 
           target = "GL_YTD_AVG_BANK_SHR_BAL", 
           expression = SColumn(col("ABCDDH_LKP.GL_YTD_AVG_BANK_SHR_BAL")), 
           description = ""
         ),          SColumnExpression(
           _row_id = "1858892947", 
           target = "BANK_SHR_BAL", 
           expression = SColumn(col("ABCDDH_LKP.BANK_SHR_BAL")), 
           description = ""
         ),          SColumnExpression(
           _row_id = "255422903", 
           target = "BANK_SHR_INT_PERDIEM_AMT", 
           expression = SColumn(col("ABCDDH_LKP.BANK_SHR_INT_PERDIEM_AMT")), 
           description = ""
         ),          SColumnExpression(
           _row_id = "1606343958", 
           target = "ENTRY_DT", 
           expression = SColumn(col("ABCDDH_LKP.ENTRY_DT")), 
           description = ""
         ),          SColumnExpression(
           _row_id = "729876670", 
           target = "GL_QTD_AGGR_BANK_SHR_BAL", 
           expression = SColumn(col("ABCDDH_LKP.GL_QTD_AGGR_BANK_SHR_BAL")), 
           description = ""
         ),          SColumnExpression(
           _row_id = "1502917694", 
           target = "GL_QTD_AGGR_CUR_BAL", 
           expression = SColumn(col("ABCDDH_LKP.GL_QTD_AGGR_CUR_BAL")), 
           description = ""
         )], 
        headAlias = "ACCT_BAL_OUT", 
        whereClause = None, 
        allIn0 = None, 
        allIn1 = None, 
        hints = []
    )
    in0 = ACCT_BAL_OUT
    in1 = ABCDDH_LKP
    inDFs = []
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
