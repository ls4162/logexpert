package insight.engineer

import java.util.UUID

import org.apache.spark.sql.Column
import org.apache.spark.sql.catalyst.expressions.{AggregateWindowFunction, AttributeReference, Expression, If, LessThanOrEqual, Literal, ScalaUDF, Subtract}
import org.apache.spark.sql.types._


/**
  * Created by sulei on 1/15/18.
  *
  * User defined window function to build session based on 10min window
  */
object Sessionization {
  val defaultMaxSessionLengthms = 600 * 1000
  case class SessionUDWF(timestamp:Expression, sessionWindow:Expression = Literal(defaultMaxSessionLengthms)) extends AggregateWindowFunction {
    self: Product =>

    override def children: Seq[Expression] = Seq(timestamp)
    override def dataType: DataType = StringType

    protected val zero = Literal( 0L )
    protected val nullString = Literal(null:String)
    protected val currentSession = AttributeReference("currentSession", StringType, nullable = true)()
    protected val previousTs =    AttributeReference("lastTs", LongType, nullable = false)()

    override val aggBufferAttributes: Seq[AttributeReference] =  currentSession  :: previousTs :: Nil

    protected val assignSession =  If(LessThanOrEqual(Subtract(timestamp, aggBufferAttributes(1)), sessionWindow),
      aggBufferAttributes(0),
      ScalaUDF( createNewSession, StringType, children = Nil))

    override val initialValues: Seq[Expression] =  nullString :: zero :: Nil
    override val updateExpressions: Seq[Expression] = assignSession :: timestamp :: Nil

    override val evaluateExpression: Expression = aggBufferAttributes(0)
    override def prettyName: String = "makeSession"
  }

  protected val  createNewSession = () => org.apache.spark.unsafe.types.UTF8String.fromString(UUID.randomUUID().toString)

  def calculateSession(ts:Column): Column = withExpr { SessionUDWF(ts.expr, Literal(defaultMaxSessionLengthms))}

  private def withExpr(expr: Expression): Column = new Column(expr)
}