package cse512

import java.util.StringTokenizer

import org.apache.spark.sql.SparkSession

object SpatialQuery extends App{
  def runRangeQuery(spark: SparkSession, arg1: String, arg2: String): Long = {
    println(arg2)
    val pointDf = spark.read.format("com.databricks.spark.csv").option("delimiter","\t").option("header","false").load(arg1);
    pointDf.createOrReplaceTempView("point")

    // YOU NEED TO FILL IN THIS USER DEFINED FUNCTION
    spark.udf.register("ST_Contains",(queryRectangle:String, pointString:String)=>((st_contains(queryRectangle,pointString)
    )))
    val resultDf = spark.sql("select * from point where ST_Contains('"+arg2+"',point._c0)")
    resultDf.show()

    return resultDf.count()
  }

  def runRangeJoinQuery(spark: SparkSession, arg1: String, arg2: String): Long = {

    val pointDf = spark.read.format("com.databricks.spark.csv").option("delimiter","\t").option("header","false").load(arg1);
    pointDf.createOrReplaceTempView("point")

    val rectangleDf = spark.read.format("com.databricks.spark.csv").option("delimiter","\t").option("header","false").load(arg2);
    rectangleDf.createOrReplaceTempView("rectangle")

    // YOU NEED TO FILL IN THIS USER DEFINED FUNCTION
    spark.udf.register("ST_Contains",(queryRectangle:String, pointString:String)=>((st_contains(queryRectangle,pointString))))

    val resultDf = spark.sql("select * from rectangle,point where ST_Contains(rectangle._c0,point._c0)")
    resultDf.show()

    return resultDf.count()
  }

  def runDistanceQuery(spark: SparkSession, arg1: String, arg2: String, arg3: String): Long = {

    val pointDf = spark.read.format("com.databricks.spark.csv").option("delimiter","\t").option("header","false").load(arg1);
    pointDf.createOrReplaceTempView("point")

    // YOU NEED TO FILL IN THIS USER DEFINED FUNCTION
    spark.udf.register("ST_Within",(pointString1:String, pointString2:String, distance:Double)=>((st_Within(pointString1,pointString2,distance))))

    val resultDf = spark.sql("select * from point where ST_Within(point._c0,'"+arg2+"',"+arg3+")")
    resultDf.show()

    return resultDf.count()
  }

  def runDistanceJoinQuery(spark: SparkSession, arg1: String, arg2: String, arg3: String): Long = {

    val pointDf = spark.read.format("com.databricks.spark.csv").option("delimiter","\t").option("header","false").load(arg1);
    pointDf.createOrReplaceTempView("point1")

    val pointDf2 = spark.read.format("com.databricks.spark.csv").option("delimiter","\t").option("header","false").load(arg2);
    pointDf2.createOrReplaceTempView("point2")

    // YOU NEED TO FILL IN THIS USER DEFINED FUNCTION
    spark.udf.register("ST_Within",(pointString1:String, pointString2:String, distance:Double)=>((st_Within(pointString1,pointString2,distance))))
    val resultDf = spark.sql("select * from point1 p1, point2 p2 where ST_Within(p1._c0, p2._c0, "+arg3+")")
    resultDf.show()

    return resultDf.count()
  }

  def st_contains(queryRectangle:String, pointString : String):Boolean = {
    val tokenizer = new StringTokenizer(queryRectangle,",",false);
    val rectX1 = tokenizer.nextToken().toDouble;
    val rectY1 = tokenizer.nextToken().toDouble;
    val rectX2 = tokenizer.nextToken().toDouble;
    val rectY2 = tokenizer.nextToken().toDouble;
    val tokenizer2 = new StringTokenizer(pointString,",",false);
    val x = tokenizer2.nextToken().toDouble;
    val y = tokenizer2.nextToken().toDouble;
    //println("( " + rectX1 +" , "+ rectY1 + " ) And ( "+ rectX2 +" , "+ rectY2 +  " ) ");
    //println("( " + x + " , " + y + " ) ");
    return (x>=rectX1)&&(x<=rectX2)&&(y>=rectY1)&&(y<=rectY2);
  }

  def st_Within(pointString1:String, pointString2:String, distance:Double):Boolean = {
    val tokenizer = new StringTokenizer(pointString1,",",false);
    val x1 = tokenizer.nextToken().toDouble;
    val y1= tokenizer.nextToken().toDouble;
    val tokenizer2 = new StringTokenizer(pointString2,",",false);
    val x2 = tokenizer2.nextToken().toDouble;
    val y2 = tokenizer2.nextToken().toDouble;
    return Math.sqrt(Math.pow(x1-x2,2) + Math.pow(y1-y2,2)) <= distance;
  }
}
