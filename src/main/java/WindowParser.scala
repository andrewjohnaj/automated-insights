import java.text.SimpleDateFormat

import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.udf

class WindowParser extends Serializable {
  val inputFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")

  val day = new SimpleDateFormat("D")
  def parseDate: UserDefinedFunction = udf((s: String) => day.format(inputFormat.parse(s)))

  val week = new SimpleDateFormat("w")
  def parseWeek: UserDefinedFunction = udf((s: String) => week.format(inputFormat.parse(s)))

  val month = new SimpleDateFormat("MM")
  def parseMonth: UserDefinedFunction = udf((s: String) => month.format(inputFormat.parse(s)))

  val year = new SimpleDateFormat("yyyy")
  def parseYear: UserDefinedFunction = udf((s: String) => year.format(inputFormat.parse(s)))

}
