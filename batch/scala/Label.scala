package insight.engineer

import org.apache.spark.sql.Column
import org.apache.spark.sql.catalyst.expressions.{AggregateWindowFunction, AttributeReference, Expression, If, EqualTo, Literal, Or}
import org.apache.spark.sql.types._


/**
  * Created by sulei on 1/22/18.
  *
  * User defined window function to label record
  */
object Label {

  case class LabelUDWF(curPage:Expression) extends AggregateWindowFunction {
    self: Product =>

    override def children: Seq[Expression] = Seq(curPage)
    override def dataType: DataType = IntegerType


    protected val nullString = Literal(null:String)
    protected val payment = Literal("payment")
    protected val success = Literal(1)
    protected val unsuccess = Literal(0)

    protected val last = AttributeReference("last", StringType, nullable = true)()
    protected val cur =    AttributeReference("cur", IntegerType, nullable = false)()

    override val aggBufferAttributes: Seq[AttributeReference] =  last  :: cur :: Nil

    protected val assignLabel =  If(Or((EqualTo(payment, last)),(EqualTo(payment, curPage))),success,unsuccess)

    override val initialValues: Seq[Expression] =  nullString :: unsuccess :: Nil
    override val updateExpressions: Seq[Expression] = curPage :: assignLabel :: Nil

    override val evaluateExpression: Expression = aggBufferAttributes(1)
    override def prettyName: String = "getLabel"
  }

  def getLabel(page:Column): Column = withExpr { LabelUDWF(page.expr) }

  private def withExpr(expr: Expression): Column = new Column(expr)
}