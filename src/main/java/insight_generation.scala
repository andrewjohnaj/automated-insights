import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Column, DataFrame, Row, SparkSession}

object insight_generation {
  val w = new WindowParser
  val ATTRIBUTE_VALUE = "attribute_value"

  def diffLast(curr: Double, prev: Double, window: String, aggType: String): Unit = {
    if (1.0 * (curr - prev) / prev > 0.01) {
      // Change this to sql write
      println(s"The $aggType in the last 2 $window has increased by ${1.0 * (curr - prev) / prev * 100}" +
        s" %")
    } else if (1.0 * (prev - curr) / prev > 0.01) {
      println(s"The $aggType in the last 2 $window has decreased by ${1.0 * (prev - curr) / prev * 100}" +
        s" %")
    }
  }

  def diffLast(c: Seq[Any], p: Seq[Any], window: String, aggType: String): Unit = {
    val curr = c(2).toString.toDouble
    val prev = p(2).toString.toDouble
    diffLast(curr, prev, window, aggType)
  }

  def diffLast2(c: Seq[Any], p: Seq[Any], window: String, aggType: String): Boolean = {
    if (c.head.toString.toLong - p.head.toString.toLong == 1) {
      diffLast(c, p, window, aggType)
      return true
    }
    false
  }

  def calculateWindowAgg(df: DataFrame, function: (Column) => Column, name: String, aggType: String): (Array[Row], Array[Row], Array[Row]) = {
    // this flag is used to check if we have 2 simulataneous days or weeks or months
    var window = false

    // Day based analysis
    val last2days = df
      .withColumn("day", w.parseDate(df.col("updated_timestamp")))
      .groupBy("day", "year")
      .agg(function(col(name)))
      .orderBy(desc("year"), desc("day"))
      .take(2)

    println(s"The $aggType in the last 1 day has been ${last2days(0)(2)}")

    if (last2days.length == 2) {
      window = diffLast2(last2days(0).toSeq, last2days(1).toSeq, "day", aggType)
    } else {
      return (last2days, null, null)
    }

    // Week based analysis
    val last2weeks = df
      .withColumn("week", w.parseWeek(df.col("updated_timestamp")))
      .groupBy("week", "year")
      .agg(function(col(name)))
      .orderBy(desc("year"), desc("week"))
      .take(2)

    println(s"The $aggType in the last 1 week has been ${last2weeks(0)(2)}")

    if (last2weeks.length == 2) {
      window = diffLast2(last2weeks(0).toSeq, last2weeks(1).toSeq, "week", aggType)
    }

    val last2months = df
      .withColumn("month", w.parseMonth(df.col("updated_timestamp")))
      .groupBy("month", "year")
      .agg(function(col(name)))
      .orderBy(desc("year"), desc("month"))
      .take(2)

    println(s"The $aggType in the last 1 month has been ${last2months(0)(2)}")
    if (last2months.length == 2) {
      window = diffLast2(last2months(0).toSeq, last2months(1).toSeq, "month", aggType)
    }

    if (!window) {
      diffLast(last2days(0).toSeq, last2days(1).toSeq, "loads", aggType)

      // TODO: gap between the last two loads
    }

    (last2days, last2weeks, last2months)
  }

  def checkCategorical(df: DataFrame): Boolean = {
    df.select(ATTRIBUTE_VALUE).distinct().count() <= 100
  }

  def calculateWindowFreq(df: DataFrame, name: String, attribute_name: String, counts: (Array[Row], Array[Row],
    Array[Row])): Unit = {

    var window = false
    val last2days = df
      .withColumn("day", w.parseDate(df.col("updated_timestamp")))
      .groupBy("day", "year", name)
      .count()
      .orderBy(desc("year"), desc("day"), desc("count"))

    println(s"In the last 1 day $attribute_name value has been ${last2days.take(1)(0)(2)} " +
      s"mostly with ${math.round(last2days.take(1)(0)(3).toString.toDouble / counts._1(0)(2).toString.toDouble * 100)}% " +
      s"representation")

    if (counts._1.length == 2) {
      last2days.as("ds1")
        .join(last2days.as("ds2"),
          col("ds1.day") =!= col("ds2.day") &&
            col("ds1." + name) <=> col("ds2." + name), "inner")
        .filter(col("ds1.day") <=> last2days.take(1)(0)(0))
        //        .select("ds1.day", "ds1.year", "ds1."+name, "ds1.count", "ds2."+name, "ds2.count")
        .withColumn("ds1.average_freq", col("ds1.count") / counts._1(0)(2))
        .withColumn("ds2.average_freq", col("ds2.count") / counts._1(1)(2))
        .foreach(row => diffLast(row.getAs[Double]("ds1.average_freq"),
          row.getAs[Double]("ds2.average_freq"),
          "day", s"The proportion of ${row.getAs(2)} $attribute_name"))
    }

    val last2weeks = df
      .withColumn("week", w.parseWeek(df.col("updated_timestamp")))
      .groupBy("week", "year", name)
      .count()
      .orderBy(desc("year"), desc("week"), desc("count"))

    if(counts._2!=null){
      println(s"In the last 1 week $attribute_name value has been ${last2weeks.take(1)(0)(2)} " +
        s"mostly with ${math.round(last2weeks.take(1)(0)(3).toString.toDouble / counts._2(0)(2).toString.toDouble *
          100)}% " +
        s"representation")
    }


    val last2months = df
      .withColumn("month", w.parseMonth(df.col("updated_timestamp")))
      .groupBy("month", "year", name)
      .count()
      .orderBy(desc("year"), desc("month"), desc("count"))

    if(counts._3!=null){
      println(s"In the last 1 week $attribute_name value has been ${last2months.take(1)(0)(2)} " +
        s"mostly with ${math.round(last2months.take(1)(0)(3).toString.toDouble / counts._3(0)(2).toString.toDouble *
          100)}% " +
        s"representation")
    }
  }

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName("automated-insights")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")


    // Create Hive Connection and fetch attribute
    val path = "datasets/d_individual_attribute_1000000025.csv"
    println(path)

    // irrelavant for the actual implementation
    // fetch the dataset and add year column to the source dataset as it is common for all the below operations
    val df = spark.read.format("csv")
      .option("header", value = true)
      .option("delimiter", ",")
      .option("mode", "DROPMALFORMED")
      .option("inferSchema", "true")
      .load(path)
      .withColumn("year", w.parseYear(col("updated_timestamp")))
      .cache()


    df.printSchema()

    val attribute_name = "blog_pages_visited" // vaibhav's change, make it dynamic


    val counts = calculateWindowAgg(df, count, "*", "number of records ingested")
    //    calculateWindowAgg(df, approx_count_distinct, "master_digital_profile_id", "distinct users")

    if (checkCategorical(df)) {
      calculateWindowFreq(df, ATTRIBUTE_VALUE, attribute_name, counts)
    }

//    println(df.select(ATTRIBUTE_VALUE).dtypes(0)._2)

    if(df.select(ATTRIBUTE_VALUE).dtypes(0)._2.equals("IntegerType") ||
      df.select(ATTRIBUTE_VALUE).dtypes(0)._2.equals("LongType") ||
      df.select(ATTRIBUTE_VALUE).dtypes(0)._2.equals("DoubleType")) {
      calculateWindowAgg(df, mean, ATTRIBUTE_VALUE, s"mean of $attribute_name")
      calculateWindowAgg(df, kurtosis, ATTRIBUTE_VALUE, s"kurtosis of $attribute_name")
      calculateWindowAgg(df, skewness, ATTRIBUTE_VALUE, s"skewness of $attribute_name")
      calculateWindowAgg(df, max, ATTRIBUTE_VALUE, s"max of $attribute_name")
      calculateWindowAgg(df, stddev, ATTRIBUTE_VALUE, s"standard deviation of $attribute_name")
      calculateWindowAgg(df, sum, ATTRIBUTE_VALUE, s"sum of $attribute_name")
    }

    // TODO: varchar (done)
    // TODO: add frequency distribution to the analysis (for factor variables) (done)
    // Example: 80% of the users have 0 page views
    // Example2: find quartiles for users and define metrics for each quartile
    // TODO: test with 040, 039, 042

    // TODO: forecasting and comparing trend
    // TODO: make insights more usable, more use friendly insights
    // TODO: make use of user feedback (figure out how)
    // TODO: bivariate analysis
  }
}
