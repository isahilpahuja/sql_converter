package controllers

import javax.inject._
import play.api._
import play.api.mvc._
import play.twirl.api.StringInterpolation
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import java.sql.{Connection,DriverManager}
import org.apache.spark.sql.SparkSession
import play.api.libs.json._
import play.api.data.Form
import play.api.data.Forms.tuple
import play.api.data.Forms.text
import java.io._
import java.util.regex.Pattern
import java.util.Calendar



/**
  * This controller creates an `Action` to handle HTTP requests to the
  * application's home page.
  */
@Singleton
class SQLController @Inject()(cc: ControllerComponents) extends AbstractController(cc) {
  def readFile1() = Action { implicit request: Request[AnyContent] => {

    //val sc = new SparkContext("local", "Application", "/path/to/spark-0.9.0-incubating", List("target/scala-2.10/playsparkapp_2.10-1.0-SNAPSHOT.jar"))
    val conf = new SparkConf().setAppName("SOME APP NAME").setMaster("local[2]").set("spark.executor.memory", "1g")

    val sc = new SparkContext(conf)
    val logData = sc.textFile("file:///C:/Users/pushukla/Documents/project/FI/SQL_CONVERTER_FRAMEWORK/poc/play-scala-seed/app/controllers/test1.txt")
    //"C:\\Users\\pushukla\\Documents\\project\\FI\\SQL_CONVERTER_FRAMEWORK\\poc\\play-scala-seed\\app\\controllers\\test.txt")
    //val numSparks = logData.filter(line => line.contains("Spark")).count()

    println("first:" + logData.count())
    logData.foreach(println)
    val Sample = "select * from test101.test1;\nselect * from test202.test2;\nselect * from test303.test3;"

    val nameValuePairs = Sample.split(";")
    var j = "";
    var k = 0
    for (i <- nameValuePairs) {

      println("array: " + k + "-> " + i)
      k = k + 1
      j += i + ";"
    }

    sc.stop()
    Ok(j)
  }
  }

  def storedProcedureToSparkConverter() = Action { implicit request: Request[AnyContent] => {

    var finalOutput = ""
    var exceptionOutput = ""

    System.setProperty("hadoop.home.dir", "C:/winutils")
    val conf = new SparkConf().setAppName("SOME APP NAME").setMaster("local[2]").
      set("spark.executor.memory", "1g").set("spark.driver.allowMultipleContexts", "true")

    val sc = new SparkContext(conf)
    var spark = SparkSession
      .builder
      .appName("Spark Examples").config("spark.sql.warehouse.dir", "file:///C:/Users/pushukla/Documents/project/FI/SQL_CONVERTER_FRAMEWORK/poc/play-scala-seed/spark-warehouse/").config("spark.driver.allowMultipleContexts", "true")
      .config("hive.metastore.uris", "thrift://ussltccsl2285.solutions.glbsnet.com:9083") //"file:///C:/Users/pushukla/Documents/project/FI/SQL_CONVERTER_FRAMEWORK/poc/play-scala-seed")//"jdbc:hive2://ussltccsl2285.solutions.glbsnet.com:10000") //"thrift://ussltccsl2285.solutions.glbsnet.com:9083") //
      .config("spark.some.config.option", "some-value").enableHiveSupport
      .getOrCreate()
    spark.sqlContext.sql("use test")
    var tempQuery = ""
    try{

    val id = "'TEST1'"
    val form = Form(
      tuple(
        "databaseName" -> text,
        "sqlQuery" -> text

      )
    )
    def values = form.bindFromRequest.data
    def databaseName = values("databaseName")
    def sqlQuery = values("sqlQuery")
    tempQuery = sqlQuery


    println("databaseName: "+databaseName)
    println("sqlQuery-> "+sqlQuery)



    /*val pw = new PrintWriter(new File("spSql.txt" ))
    pw.write(sqlQuery)
    pw.close
*/

    /* val conf = new SparkConf().setAppName("SOME APP NAME").setMaster("local[2]").set("spark.executor.memory", "1g").set("spark.driver.allowMultipleContexts", "true")

     val sc = new SparkContext(conf)*/

   /* val conf = new SparkConf().setAppName("SOME APP NAME").setMaster("local[2]").
      set("spark.executor.memory", "1g").set("spark.driver.allowMultipleContexts", "true")

    val sc = new SparkContext(conf)*/

    /*var spark = SparkSession
      .builder
      .appName("Spark Examples").config("spark.sql.warehouse.dir", "file:///C:/Users/pushukla/Documents/project/FI/SQL_CONVERTER_FRAMEWORK/poc/play-scala-seed/spark-warehouse/").config("spark.driver.allowMultipleContexts", "true")
      .config("hive.metastore.uris", "thrift://ussltccsl2285.solutions.glbsnet.com:9083") //"file:///C:/Users/pushukla/Documents/project/FI/SQL_CONVERTER_FRAMEWORK/poc/play-scala-seed")//"jdbc:hive2://ussltccsl2285.solutions.glbsnet.com:10000") //"thrift://ussltccsl2285.solutions.glbsnet.com:9083") //
      .config("spark.some.config.option", "some-value").enableHiveSupport
      .getOrCreate()
    spark.sqlContext.sql("use test")*/

    /*val storedProcFile = sc.textFile("file:///C:/Users/pushukla/Documents/project/FI/SQL_CONVERTER_FRAMEWORK/poc/play-scala-seed/spSql.txt")*/
    val storedProcFile = sc.textFile("file:///C:/Users/pushukla/Documents/project/FI/SQL_CONVERTER_FRAMEWORK/poc/play-scala-seed/app/controllers/test3.txt")

    //val convertFileToArray = storedProcFile.collect().toArray
    val convertFileToArray = tempQuery.split("\n")


    var sqlQueryTemp = ""
    var selectQueryTemp = ""
    var messageOutput = ""
    var selectQuery = ""
    var finalSQL = ""
    var ifQuery = ""
    var printMessage = ""
    var messageCounter = 0
    var whereClause = ""
    var selectPart = ""
    var ifConditionTable = ""

    /* val spark = SparkSession
       .builder
       .appName("Spark Examples").config("spark.sql.warehouse.dir", "file:///C:/Users/pushukla/Documents/project/FI/SQL_CONVERTER_FRAMEWORK/poc/play-scala-seed/spark-warehouse/")
       .config("spark.some.config.option", "some-value")
       .getOrCreate()*/
    for (eachLine <- convertFileToArray) {
      var line = eachLine
      if (line != null) {
        line = line.trim
      }

      println("NEW TESTING line->:" + line)
      println("NEW TESTING printMessage->:" + printMessage)
      if (line.toUpperCase.contains("RAISE NOTICE") || printMessage.toUpperCase.contains("RAISE NOTICE")) {


        if (messageCounter == 0) {
          if (line.contains(";")) {
            printMessage += line.substring(line.indexOf("RAISE NOTICE"), line.indexOf(";"))
            printMessage = printMessage.replace("RAISE NOTICE", "RAISE NOTIE")
            messageOutput = "println(\"" + printMessage + ";\") \n"
            println("CHECK TESTING-> " + messageOutput)
            finalOutput += messageOutput
            println("TESTING-> " + finalOutput)
            messageCounter = 0
            printMessage = ""
          }
          else {
            printMessage += line.substring(line.indexOf("RAISE NOTICE"))
            messageCounter = messageCounter + 1
          }
        }
        else {
          if (line.contains(";")) {
            printMessage += line.substring(0, line.indexOf(";"))
            messageOutput = "println(\"" + printMessage + ";\") \n"
            println("CHECK TESTING else-> " + messageOutput)
            finalOutput += messageOutput
            println("TESTING else-> " + finalOutput)
            messageCounter = 0
            printMessage = ""
          }
          else {
            printMessage += line
            messageCounter = messageCounter + 1
          }
        }


      }


      else if (!(line.toUpperCase.trim.startsWith("EXCEPTION") || line.contains("$") ||
        line.toUpperCase.trim.startsWith("RETURN") || line.toUpperCase.trim.startsWith("ELSE IF NOT FOUND THEN") ||
        line.toUpperCase.trim.startsWith("WHEN OTHERS THEN") || line.toUpperCase.trim.startsWith("IF FOUND THEN") ||
        line.toUpperCase.trim.startsWith("CREATE") || line.toUpperCase.trim.startsWith("DECLARE") ||
        line.toUpperCase.trim.startsWith("RETURNS") || line.toUpperCase.trim.startsWith("BEGIN") ||
        line.toUpperCase.trim.startsWith("LANGUAGE") || line.toUpperCase.trim.startsWith("END") ||
        line.toUpperCase.trim.startsWith("--") || line.toUpperCase.trim.startsWith("EXECUTE IMMEDIATE"))) {


        /*println("LINE:->"+line)
        println("finalOutput:->"+finalOutput)*/
        //IF SELECT FOUND IN A ROW, THEN WILL CHECK SEMI-COLON EXISTS
        sqlQueryTemp += line + "\n"
        if ((sqlQueryTemp.contains(":=") && sqlQueryTemp.contains("'")) &&
          !((exceptionOutput.toUpperCase.contains("IF FOUND THEN") ||
            exceptionOutput.toUpperCase.contains("WHEN OTHERS THEN") ||
            exceptionOutput.toUpperCase.contains("ELSE IF NOT FOUND THEN")))) {
          //sqlQueryTemp += line + "\n"
          println("=>test2. Inside :=@@" + sqlQueryTemp)
          if (line.contains(";")) {
            println("=>test3. Inside line=> " + line)
            println("=>test3. Inside sqlQueryTemp=> " + sqlQueryTemp)
            selectQuery = sqlQueryTemp.substring(sqlQueryTemp.indexOf(":=") + 2, (sqlQueryTemp.indexOf(";")))


            //CHECKING "SELECT * INTO " QUERY AND CONVERT IT TO ACCORDING SPARK SQL

            if (selectQuery != null) {
              val regex = "^(SELECT|select)[\\s\\S]+?(INTO|into)\\s*?$"

              val p = Pattern.compile(regex, Pattern.MULTILINE)
              //val stringVal = " SELECT jkjf,jhdfj INTO "
              val matcher = p.matcher(selectQuery.trim);
              //println("matcher:" + matcher.find())

              var tempIntoStr = "into"
              if (matcher.find()) {
                var selectIntoCompleteQuery = selectQuery.trim

                if (selectIntoCompleteQuery.contains("INTO")) {
                  tempIntoStr = "INTO"
                }

                selectPart = selectIntoCompleteQuery.substring(0, selectIntoCompleteQuery.indexOf(tempIntoStr))

                selectIntoCompleteQuery = selectIntoCompleteQuery.substring(selectIntoCompleteQuery.indexOf(tempIntoStr))
                selectIntoCompleteQuery = selectIntoCompleteQuery.replace(tempIntoStr, "INSERT " + tempIntoStr)
                if (selectIntoCompleteQuery.indexOf("FROM") >= 0) {
                  selectIntoCompleteQuery = selectIntoCompleteQuery.replaceFirst("FROM", selectPart + " FROM")
                }
                else {
                  selectIntoCompleteQuery = selectIntoCompleteQuery.replaceFirst("from", selectPart + " from")
                }
                selectQuery = selectIntoCompleteQuery
              }
              if (selectQuery.indexOf(tempIntoStr) > 0) {
                var tableIndex = selectQuery.indexOf(tempIntoStr) + 2
                ifConditionTable = selectQuery.substring(tableIndex)
                ifConditionTable = ifConditionTable.substring(0, ifConditionTable.indexOf(" "))
                println("ifConditionTable:" + ifConditionTable)
              }
            }


            println("=>LINE:->" + selectQuery)
            /* spark.read
               .format("jdbc")
               .option("url", "jdbc:mysql://localhost:3306/mysql")
               .option("dbtable", "mysql.v_table")
               .option("user", "root")
               .option("password", "root")
               .load()
               .createOrReplaceTempView("my_spark_table")
             spark.read
               .format("jdbc")
               .option("url", "jdbc:mysql://localhost:3306/mysql")
               .option("dbtable", "mysql.drop_table")
               .option("user", "root")
               .option("password", "root")
               .load()
               .createOrReplaceTempView("my_spark_drop_table")*/
            selectQuery.replace("TNAME", "TEST1")
            if (selectQuery != null) {
              selectQuery = selectQuery.trim
            }
            finalSQL = "spark.sql(\"" + selectQuery + "\")" + "\n"
            finalSQL = finalSQL.replace("TNAME", "'\"+TNAME+\"'")
            selectQuery = selectQuery.replace("TNAME", "TEST1")
            finalSQL = finalSQL.replace("||", "")
            finalSQL = finalSQL.replace("'", "")
            /*selectQuery = selectQuery.replace("DROP_TABLE", "my_spark_drop_table")
            selectQuery = selectQuery.replace("V_TABLE", "my_spark_table")*/
            selectQuery = selectQuery.replace("||", "")
            selectQuery = selectQuery.replace("t3", "test.V_table_1")
            selectQuery = selectQuery.replace("col_name1", "id")
            selectQuery = selectQuery.replace("t2", "test.V_table")
            selectQuery = selectQuery.replace("'", "")
            try {

              //PUT A FLAG TO EXECUTE
              println("query to execute-> " + selectQuery)

              spark.sql(selectQuery)

            } catch {
              case ex: Exception => {
                println("Exception: " + ex.getMessage)
              }
              /* case e if (e.getMessage == null) => println ("Unknown exception with no message")
               case e => println ("An unknown error has been caught" + e.getMessage)*/
            }

            if (selectQuery.lastIndexOf("FROM") > 0) {
              whereClause = selectQuery.substring(selectQuery.lastIndexOf("FROM"))
            }
            else if (selectQuery.lastIndexOf("from") > 0) {
              whereClause = selectQuery.substring(selectQuery.lastIndexOf("from"))
            }


            finalOutput += finalSQL
            println("final output 1->" + finalOutput)
            //Making this replace so that the line should not be executed again.
            finalOutput = finalOutput.replace(":=", " ")
            sqlQueryTemp = ""
          }

          println("->colon whereClause:" + whereClause)

        }

        else if ((sqlQueryTemp.toUpperCase.contains("SELECT"))) {
          println("test2. Inside Select.")
          //sqlQueryTemp += line + "\n"
          if (line.contains(";")) {
            println("test3. Inside ;. " + line)
            selectQuery = sqlQueryTemp.substring(0, (sqlQueryTemp.indexOf(";")))

            if (selectQuery != null) {
              //val regex = "^(SELECT|select)[\\s\\S]+?(INTO|into)\\s*?$"
              val regex = "^(SELECT|select)[\\s\\S]+?(INTO|into) *"

              val p = Pattern.compile(regex, Pattern.MULTILINE)
              //val stringVal = " SELECT jkjf,jhdfj INTO "
              val matcher = p.matcher(selectQuery.trim);
              //println("matcher:" + matcher.find())
              //println("again matcher:" + matcher.find())

              var tempIntoStr = "into"
              if (matcher.find()) {

                var selectIntoCompleteQuery = selectQuery.trim

                if (selectIntoCompleteQuery.contains("INTO")) {
                  tempIntoStr = "INTO"
                }

                println("inside find->" + selectIntoCompleteQuery + " =tempIntoStr=" + tempIntoStr)
                selectPart = selectIntoCompleteQuery.substring(0, selectIntoCompleteQuery.indexOf(tempIntoStr))
                println("selectPart:" + selectPart)

                selectIntoCompleteQuery = selectIntoCompleteQuery.substring(selectIntoCompleteQuery.indexOf(tempIntoStr))
                selectIntoCompleteQuery = selectIntoCompleteQuery.replace(tempIntoStr, "INSERT " + tempIntoStr)
                if (selectIntoCompleteQuery.indexOf("FROM") >= 0) {
                  selectIntoCompleteQuery = selectIntoCompleteQuery.replaceFirst("FROM", selectPart + " FROM")
                }
                else {
                  selectIntoCompleteQuery = selectIntoCompleteQuery.replaceFirst("from", selectPart + " from")
                }
                println("selectIntoCompleteQuery:" + selectIntoCompleteQuery)
                selectQuery = selectIntoCompleteQuery
              }

              var tableIndex = selectQuery.indexOf(tempIntoStr) + 5
              ifConditionTable = selectQuery.substring(tableIndex)
              ifConditionTable = ifConditionTable.substring(0, ifConditionTable.indexOf(" "))
              println("ifConditionTable:" + ifConditionTable)
            }

            println("LINE:->" + selectQuery)
            /*  spark.read
                .format("jdbc")
                .option("url", "jdbc:mysql://localhost:3306/mysql")
                .option("dbtable", "mysql.v_table")
                .option("user", "root")
                .option("password", "root")
                .load()
                .createOrReplaceTempView("my_spark_table")
              spark.read
                .format("jdbc")
                .option("url", "jdbc:mysql://localhost:3306/mysql")
                .option("dbtable", "mysql.drop_table")
                .option("user", "root")
                .option("password", "root")
                .load()
                .createOrReplaceTempView("my_spark_drop_table")*/
            selectQuery.replace("TNAME", id)
            if (selectQuery != null) {
              selectQuery = selectQuery.trim
            }
            finalSQL = "spark.sql(\"" + selectQuery + "\")" + "\n"
            finalSQL = finalSQL.replace("TNAME", "'\"+TNAME+\"'")
            selectQuery = selectQuery.replace("TNAME", id)
            if (selectQuery.lastIndexOf("WHERE") > 0) {
              whereClause = selectQuery.substring(selectQuery.lastIndexOf("WHERE"))
            }
            else if (selectQuery.lastIndexOf("where") > 0) {
              whereClause = selectQuery.substring(selectQuery.lastIndexOf("where"))
            }
            //selectQuery = selectQuery.replace("DROP_TABLE", "my_spark_drop_table")
            //selectQuery = selectQuery.replace("V_TABLE", "my_spark_table")
            selectQuery = selectQuery.replace("||", "\"+\"")
            //PUT A FLAG TO EXECUTE
            spark.sql(selectQuery)

            println("->select whereClause:" + whereClause)

            finalOutput += finalSQL
            println("final output 2->" + finalOutput)
            //Making this replace so that the line should not be executed again.
            finalOutput = finalOutput.replace("SELECT", "SELCT")
            sqlQueryTemp = ""

          }

        }
        /*else{
          exceptionOutput += line + "\n"
        }
  */
        //CHECKS FOR "IF FOUND THEN " WAS FOUND IN PREVIOUS QUERY THEN THIS WILL BE IF LOGIC, WE TRY TO EXTRACT IT
        else if ((exceptionOutput.toUpperCase.contains("IF FOUND THEN") || exceptionOutput.toUpperCase.contains("WHEN OTHERS THEN") || exceptionOutput.toUpperCase.contains("ELSE IF NOT FOUND THEN")) && (line.contains("'")) && (line.contains(":="))) {
          println("t value:" + line)
          var temp = line.substring((line.indexOf("'")), (line.indexOf("'")))

          var temp1 = line.substring((line.indexOf("'")), (line.indexOf(";")))
          ifQuery = temp.concat(temp1.replace("||", ""))
          ifQuery = ifQuery.replace("'", "")

          println("ifQuery value:" + ifQuery)

          //Making this replace so that the line should not be executed again.
          exceptionOutput = exceptionOutput.replace("IF FOUND THEN", "IF FOUND THN")
          exceptionOutput = exceptionOutput.replace("WHEN OTHERS THEN", "WHEN OTHERS THN")

          selectQuery = selectPart + " from " + ifConditionTable + " " + whereClause //"select * from test.v_drop WHERE UPPER(AME) =UPPER(" + id + ")"
          println("selectQuery value ->" + selectQuery)

          finalSQL = "var selectDF = spark.sql(\" select * from V_DROP WHERE UPPER(AME) =UPPER('\"+" +
            "TNAME + \"')\")" + "\n"

          finalOutput += finalSQL
          val selectDF = spark.sql(selectQuery)

          //selectDF.collect.foreach(println)


          var selectOutput: String = ""
          for (df1 <- selectDF.collect) {
            var temp3 = df1.toSeq
            temp3.foreach(selectOutput += _ + " ")

          }
          var tempCode = "var selectOutput: String = \"\"\n        for (df1 <- selectDF.collect) {\n          var temp3 = df1.toSeq\n          temp3.foreach(selectOutput += _ + \" \")\n\n        }"

          finalOutput += tempCode + "\n"

          println("contents of  temp2 table:" + selectOutput)
          println("contents of drop table." + selectOutput.contains(id) + ":id:" + id)
          if (selectOutput.contains(id.replace("'", ""))) {
            println("Going inside contents of drop table.")
            finalSQL = "spark.sql(\"" + ifQuery + "\")"
            finalSQL = finalSQL.replace("TNAME", "\"+TNAME+\"")
            //finalOutput +=finalSQL + "\n"
            tempCode = " if (selectOutput.contains(id.replace(\"'\", \"\"))) {\n          println(\"Going inside contents of drop table.\")\n "
            tempCode += finalSQL + "} \n"
            finalOutput += tempCode + "\n"
          }


          println("testDF:" + selectDF.collect.length)
          println("testDF count:" + selectDF.count)
          println("new sum:" + selectOutput)
          selectDF.show

        }

        else {
          exceptionOutput += line.trim + "\n"
          sqlQueryTemp = ""

        }

      }

      else {
        exceptionOutput += line.trim + "\n"
      }

    }



    println("spark.sql(" + selectQuery + ")")
    println("spark.sql(" + ifQuery + ")")
    println("finalArr->" + finalOutput)
    exceptionOutput = exceptionOutput.replace("IF FOUND THN", "IF FOUND THEN")
    exceptionOutput = exceptionOutput.replace("WHEN OTHERS THN", "WHEN OTHERS THEN")
    finalOutput = finalOutput.replace("SELCT", "SELECT")
    finalOutput = finalOutput.replace("RAISE NOTIE", "")
    finalOutput = finalOutput.replace("||", "")
    println("Exception Messages:" + exceptionOutput)







  }
    catch {
      case ex: Exception => {
        println("Final Exception: "+ex.getMessage + ":"+ex)
      }

      /* case e if (e.getMessage == null) => println ("Unknown exception with no message")
       case e => println ("An unknown error has been caught" + e.getMessage)*/
    }

    //var saveData=finalOutput + "\n EXCEPTIONS: \n" + exceptionOutput

    //var tempQuery = sqlQuery
    /*tempQuery = tempQuery.replace("'","""\'""")
    tempQuery = tempQuery.replace(""";""","""\;""")
    saveData = saveData.replace("'","""\'""")
    saveData = saveData.replace(""";""","""\;""")

    println("sqlQuerysqlQuery=> "+tempQuery)
    println("saveDatasaveData=> "+saveData)

    import java.text.SimpleDateFormat

    val cal = Calendar.getInstance
    val format1 = new SimpleDateFormat("yyyy-MM-dd HH-MM-SS")
    val formatted = format1.format(cal.getTime)
    println("TESTINGNOWformatted------->"+formatted)
    val now = Calendar.getInstance().getTime
    println("TESTINGNOW------->"+now)
    spark.sql("INSERT INTO TABLE test.SQLCONVERTER VALUES('"+tempQuery+"','"+saveData+"',null,null)")*/
    finally {
      spark.stop()
      sc.stop()
    }



    Ok(finalOutput + "\n EXCEPTIONS: \n" + exceptionOutput)

  }
  }




  def hiveConnection() = Action { implicit request: Request[AnyContent] => {
    System.setProperty("hadoop.home.dir", "C:\\hadoop")

    val conf = new SparkConf().setAppName("SOME APP NAME").setMaster("local[2]").
      set("spark.executor.memory", "1g").set("spark.driver.allowMultipleContexts", "true")

    val sc = new SparkContext(conf)

    val spark = SparkSession
      .builder
      .appName("Spark Examples").config("spark.sql.warehouse.dir", "file:///C:/Users/pushukla/Documents/project/FI/SQL_CONVERTER_FRAMEWORK/poc/play-scala-seed/spark-warehouse/")
      .config("hive.metastore.uris","thrift://ussltccsl2285.solutions.glbsnet.com:9083") //"file:///C:/Users/pushukla/Documents/project/FI/SQL_CONVERTER_FRAMEWORK/poc/play-scala-seed")//"jdbc:hive2://ussltccsl2285.solutions.glbsnet.com:10000") //"thrift://ussltccsl2285.solutions.glbsnet.com:9083") //
      .config("spark.some.config.option", "some-value").enableHiveSupport
      .getOrCreate()

    //val sqlContext = new org.apache.spark.sql.hive.HiveContext(sc)


    /*val l1 = spark.sqlContext.sql("show databases").show
    val l2 = spark.sqlContext.sql("use test")
    val l = spark.sqlContext.sql("select * from v_table").show(10)
    val x = spark.sqlContext.sql("select * from emp where deptno not in (select deptno from dept)").show*/
    val test1 = spark.sqlContext.sql("select mgr from test.emp group by mgr having sum(mgr) > 1000").show

    var TNAME = "TEST1"
    spark.sql("INSERT into test.V_DROP SELECT ID,AME FROM test.V_TABLE WHERE UPPER(AME) =UPPER('"+TNAME+"')")
    var selectDF = spark.sql(" select * from test.V_DROP WHERE UPPER(AME) =UPPER('"+TNAME + "')")
    var selectOutput: String = ""
    for (df1 <- selectDF.collect) {
      var temp3 = df1.toSeq
      temp3.foreach(selectOutput += _ + " ")

    }
    if (selectOutput.contains(TNAME)) {
      println("Going inside contents of drop table.")
      spark.sql("DROP TABLE  test." + TNAME )

    }
    spark.stop()
    sc.stop()
    Ok("test")
  }
  }


  def sqlToDataFrameConverter() = Action { implicit request: Request[AnyContent] => {

    var finalOutput = ""
    var exceptionOutput = ""

    System.setProperty("HADOOP_USER_NAME","arkumar")
    System.setProperty("hadoop.home.dir", "C:/winutils")
    val conf = new SparkConf().setAppName("SOME APP NAME").setMaster("local[2]").
      set("spark.executor.memory", "1g").set("spark.driver.allowMultipleContexts", "true")

    val sc = new SparkContext(conf)
    var spark = SparkSession
      .builder
      .appName("Spark Examples").config("spark.sql.warehouse.dir", "file:///C:/Users/pushukla/Documents/project/FI/SQL_CONVERTER_FRAMEWORK/poc/play-scala-seed/spark-warehouse/").config("spark.driver.allowMultipleContexts", "true")
      .config("hive.metastore.uris", "thrift://ussltccsl2285.solutions.glbsnet.com:9083") //"file:///C:/Users/pushukla/Documents/project/FI/SQL_CONVERTER_FRAMEWORK/poc/play-scala-seed")//"jdbc:hive2://ussltccsl2285.solutions.glbsnet.com:10000") //"thrift://ussltccsl2285.solutions.glbsnet.com:9083") //
      .config("spark.some.config.option", "some-value").enableHiveSupport
      .getOrCreate()
    spark.sqlContext.sql("use temp")
    var tempQuery = ""
    try{

      val id = "'TEST2'"
      val form = Form(
        tuple(
          "databaseName" -> text,
          "sqlQuery" -> text

        )
      )
      def values = form.bindFromRequest.data
      def databaseName = values("databaseName")
      def sqlQuery = values("sqlQuery")
      tempQuery = sqlQuery


      println("databaseName: "+databaseName)
      println("sqlQuery-> "+sqlQuery)



      /*val pw = new PrintWriter(new File("spSql.txt" ))
      pw.write(sqlQuery)
      pw.close
  */

      /* val conf = new SparkConf().setAppName("SOME APP NAME").setMaster("local[2]").set("spark.executor.memory", "1g").set("spark.driver.allowMultipleContexts", "true")

       val sc = new SparkContext(conf)*/

      /* val conf = new SparkConf().setAppName("SOME APP NAME").setMaster("local[2]").
         set("spark.executor.memory", "1g").set("spark.driver.allowMultipleContexts", "true")

       val sc = new SparkContext(conf)*/

      /*var spark = SparkSession
        .builder
        .appName("Spark Examples").config("spark.sql.warehouse.dir", "file:///C:/Users/pushukla/Documents/project/FI/SQL_CONVERTER_FRAMEWORK/poc/play-scala-seed/spark-warehouse/").config("spark.driver.allowMultipleContexts", "true")
        .config("hive.metastore.uris", "thrift://ussltccsl2285.solutions.glbsnet.com:9083") //"file:///C:/Users/pushukla/Documents/project/FI/SQL_CONVERTER_FRAMEWORK/poc/play-scala-seed")//"jdbc:hive2://ussltccsl2285.solutions.glbsnet.com:10000") //"thrift://ussltccsl2285.solutions.glbsnet.com:9083") //
        .config("spark.some.config.option", "some-value").enableHiveSupport
        .getOrCreate()
      spark.sqlContext.sql("use test")*/

      /*val storedProcFile = sc.textFile("file:///C:/Users/pushukla/Documents/project/FI/SQL_CONVERTER_FRAMEWORK/poc/play-scala-seed/spSql.txt")*/
      val storedProcFile = sc.textFile("file:///C:/Users/pushukla/Documents/project/FI/SQL_CONVERTER_FRAMEWORK/poc/play-scala-seed/app/controllers/test3.txt")

      //val convertFileToArray = storedProcFile.collect().toArray
      val convertFileToArray = tempQuery.split("\n")

      var sqlQueryTemp = ""
      var selectQueryTemp = ""
      var messageOutput = ""
      var selectQuery = ""
      var finalSQL = ""
      var ifQuery = ""
      var printMessage = ""
      var messageCounter = 0
      var whereClause = ""
      var selectPart = ""
      var ifConditionTable = ""
      var tableNameWithWhereClause = ""

      /* val spark = SparkSession
         .builder
         .appName("Spark Examples").config("spark.sql.warehouse.dir", "file:///C:/Users/pushukla/Documents/project/FI/SQL_CONVERTER_FRAMEWORK/poc/play-scala-seed/spark-warehouse/")
         .config("spark.some.config.option", "some-value")
         .getOrCreate()*/
      for (eachLine <- convertFileToArray) {
        var line = eachLine
        if (line != null) {
          line = line.trim
        }

        println("NEW TESTING line->:" + line)
        println("NEW TESTING printMessage->:" + printMessage)
        if (line.toUpperCase.contains("RAISE NOTICE") || printMessage.toUpperCase.contains("RAISE NOTICE")) {


          if (messageCounter == 0) {
            if (line.contains(";")) {
              printMessage += line.substring(line.indexOf("RAISE NOTICE"), line.indexOf(";"))
              printMessage = printMessage.replace("RAISE NOTICE", "RAISE NOTIE")
              messageOutput = "println(\"" + printMessage + ";\") \n"
              println("CHECK TESTING-> " + messageOutput)
              finalOutput += messageOutput
              println("TESTING-> " + finalOutput)
              messageCounter = 0
              printMessage = ""
            }
            else {
              printMessage += line.substring(line.indexOf("RAISE NOTICE"))
              messageCounter = messageCounter + 1
            }
          }
          else {
            if (line.contains(";")) {
              printMessage += line.substring(0, line.indexOf(";"))
              messageOutput = "println(\"" + printMessage + ";\") \n"
              println("CHECK TESTING else-> " + messageOutput)
              finalOutput += messageOutput
              println("TESTING else-> " + finalOutput)
              messageCounter = 0
              printMessage = ""
            }
            else {
              printMessage += line
              messageCounter = messageCounter + 1
            }
          }


        }


        else if (!(line.toUpperCase.trim.startsWith("EXCEPTION") || line.contains("$") ||
          line.toUpperCase.trim.startsWith("RETURN") || line.toUpperCase.trim.startsWith("ELSE IF NOT FOUND THEN") ||
          line.toUpperCase.trim.startsWith("WHEN OTHERS THEN") || line.toUpperCase.trim.startsWith("IF FOUND THEN") ||
          line.toUpperCase.trim.startsWith("CREATE") || line.toUpperCase.trim.startsWith("DECLARE") ||
          line.toUpperCase.trim.startsWith("RETURNS") || line.toUpperCase.trim.startsWith("BEGIN") ||
          line.toUpperCase.trim.startsWith("LANGUAGE") || line.toUpperCase.trim.startsWith("END") ||
          line.toUpperCase.trim.startsWith("--") || line.toUpperCase.trim.startsWith("EXECUTE IMMEDIATE"))) {


          /*println("LINE:->"+line)
          println("finalOutput:->"+finalOutput)*/
          //IF SELECT FOUND IN A ROW, THEN WILL CHECK SEMI-COLON EXISTS
          sqlQueryTemp += line + "\n"
          if ((sqlQueryTemp.contains(":=") && sqlQueryTemp.contains("'")) &&
            !((exceptionOutput.toUpperCase.contains("IF FOUND THEN") ||
              exceptionOutput.toUpperCase.contains("WHEN OTHERS THEN") ||
              exceptionOutput.toUpperCase.contains("ELSE IF NOT FOUND THEN")))) {
            //sqlQueryTemp += line + "\n"
            println("=>test2. Inside :=@@" + sqlQueryTemp)
            if (line.contains(";")) {
              println("=>test3. Inside line=> " + line)
              println("=>test3. Inside sqlQueryTemp=> " + sqlQueryTemp)
              selectQuery = sqlQueryTemp.substring(sqlQueryTemp.indexOf(":=") + 2, (sqlQueryTemp.indexOf(";")))


              //CHECKING "SELECT * INTO " QUERY AND CONVERT IT TO ACCORDING SPARK SQL

              if (selectQuery != null) {
                val regex = "^(SELECT|select)[\\s\\S]+?(INTO|into)\\s*?$"

                val p = Pattern.compile(regex, Pattern.MULTILINE)
                //val stringVal = " SELECT jkjf,jhdfj INTO "
                val matcher = p.matcher(selectQuery.trim);
                //println("matcher:" + matcher.find())

                var tempIntoStr = "into"
                if (matcher.find()) {
                  var selectIntoCompleteQuery = selectQuery.trim

                  if (selectIntoCompleteQuery.contains("INTO")) {
                    tempIntoStr = "INTO"
                  }

                  selectPart = selectIntoCompleteQuery.substring(0, selectIntoCompleteQuery.indexOf(tempIntoStr))

                  selectIntoCompleteQuery = selectIntoCompleteQuery.substring(selectIntoCompleteQuery.indexOf(tempIntoStr))
                  selectIntoCompleteQuery = selectIntoCompleteQuery.replace(tempIntoStr, "INSERT " + tempIntoStr)
                  if (selectIntoCompleteQuery.indexOf("FROM") >= 0) {
                    selectIntoCompleteQuery = selectIntoCompleteQuery.replaceFirst("FROM", selectPart + " FROM")
                  }
                  else {
                    selectIntoCompleteQuery = selectIntoCompleteQuery.replaceFirst("from", selectPart + " from")
                  }
                  selectQuery = selectIntoCompleteQuery
                }
                if (selectQuery.indexOf(tempIntoStr) > 0) {
                  var tableIndex = selectQuery.indexOf(tempIntoStr) + 2
                  ifConditionTable = selectQuery.substring(tableIndex)
                  ifConditionTable = ifConditionTable.substring(0, ifConditionTable.indexOf(" "))
                  println("ifConditionTable:" + ifConditionTable)
                }
              }


              println("=>LINE:->" + selectQuery)
              /* spark.read
                 .format("jdbc")
                 .option("url", "jdbc:mysql://localhost:3306/mysql")
                 .option("dbtable", "mysql.v_table")
                 .option("user", "root")
                 .option("password", "root")
                 .load()
                 .createOrReplaceTempView("my_spark_table")
               spark.read
                 .format("jdbc")
                 .option("url", "jdbc:mysql://localhost:3306/mysql")
                 .option("dbtable", "mysql.drop_table")
                 .option("user", "root")
                 .option("password", "root")
                 .load()
                 .createOrReplaceTempView("my_spark_drop_table")*/
              selectQuery.replace("TNAME", "TEST1")
              if (selectQuery != null) {
                selectQuery = selectQuery.trim
              }
              finalSQL = "spark.sql(\"" + selectQuery + "\")" + "\n"
              finalSQL = finalSQL.replace("TNAME", "'\"+TNAME+\"'")
              selectQuery = selectQuery.replace("TNAME", "TEST1")
              finalSQL = finalSQL.replace("||", "")
              finalSQL = finalSQL.replace("'", "")
              /*selectQuery = selectQuery.replace("DROP_TABLE", "my_spark_drop_table")
              selectQuery = selectQuery.replace("V_TABLE", "my_spark_table")*/
              selectQuery = selectQuery.replace("||", "")
              selectQuery = selectQuery.replace("t3", "test.V_table_1")
              selectQuery = selectQuery.replace("col_name1", "id")
              selectQuery = selectQuery.replace("t2", "test.V_table")
              selectQuery = selectQuery.replace("'", "")
              try {

                //PUT A FLAG TO EXECUTE
                println("query to execute-> " + selectQuery)

                spark.sql(selectQuery)

              } catch {
                case ex: Exception => {
                  println("Exception: " + ex.getMessage)
                }
                /* case e if (e.getMessage == null) => println ("Unknown exception with no message")
                 case e => println ("An unknown error has been caught" + e.getMessage)*/
              }

              if (selectQuery.lastIndexOf("FROM") > 0) {
                whereClause = selectQuery.substring(selectQuery.lastIndexOf("FROM"))
              }
              else if (selectQuery.lastIndexOf("from") > 0) {
                whereClause = selectQuery.substring(selectQuery.lastIndexOf("from"))
              }


              finalOutput += finalSQL
              println("final output 1->" + finalOutput)
              //Making this replace so that the line should not be executed again.
              finalOutput = finalOutput.replace(":=", " ")
              sqlQueryTemp = ""
            }

            println("->colon whereClause:" + whereClause)

          }

          else if ((sqlQueryTemp.toUpperCase.contains("SELECT"))) {
            println("test2. Inside Select.")
            //sqlQueryTemp += line + "\n"
            if (line.contains(";")) {
              println("test3. Inside ;. " + line)
              selectQuery = sqlQueryTemp.substring(0, (sqlQueryTemp.indexOf(";")))

              if (selectQuery != null) {
                //val regex = "^(SELECT|select)[\\s\\S]+?(INTO|into)\\s*?$"
                val regex = "^(SELECT|select)[\\s\\S]+?(INTO|into) *"

                val p = Pattern.compile(regex, Pattern.MULTILINE)
                //val stringVal = " SELECT jkjf,jhdfj INTO "
                val matcher = p.matcher(selectQuery.trim);
                //println("matcher:" + matcher.find())
                //println("again matcher:" + matcher.find())

                var tempIntoStr = "into"
                if (matcher.find()) {

                  var selectIntoCompleteQuery = selectQuery.trim

                  if (selectIntoCompleteQuery.contains("INTO")) {
                    tempIntoStr = "INTO"
                  }

                  println("inside find->" + selectIntoCompleteQuery + " =tempIntoStr=" + tempIntoStr)
                  selectPart = selectIntoCompleteQuery.substring(0, selectIntoCompleteQuery.indexOf(tempIntoStr))
                  println("selectPart:" + selectPart)

                  selectIntoCompleteQuery = selectIntoCompleteQuery.substring(selectIntoCompleteQuery.indexOf(tempIntoStr))
                  selectIntoCompleteQuery = selectIntoCompleteQuery.replace(tempIntoStr, "INSERT " + tempIntoStr)
                  if (selectIntoCompleteQuery.indexOf("FROM") >= 0) {
                    selectIntoCompleteQuery = selectIntoCompleteQuery.replaceFirst("FROM", selectPart + " FROM")
                  }
                  else {
                    selectIntoCompleteQuery = selectIntoCompleteQuery.replaceFirst("from", selectPart + " from")
                  }
                  println("selectIntoCompleteQuery:" + selectIntoCompleteQuery)
                  selectQuery = selectIntoCompleteQuery

                  var tableIndex = selectQuery.indexOf(tempIntoStr) + 5
                  ifConditionTable = selectQuery.substring(tableIndex)
                  ifConditionTable = ifConditionTable.substring(0, ifConditionTable.indexOf(" "))
                  println("ifConditionTable:" + ifConditionTable)

                  tableNameWithWhereClause = selectQuery.substring(selectQuery.toUpperCase.lastIndexOf("FROM")).trim

                  tableNameWithWhereClause = tableNameWithWhereClause.replace("TNAME", id)
                  println("============> TESTING DATAFRAME")
                  val selectDf = spark.sql(selectPart +" "+tableNameWithWhereClause)
                  finalSQL = "spark.sql(\"" + selectPart + " " + tableNameWithWhereClause  + "\")" + "\n"
                  selectDf.show

                  finalOutput += finalSQL
                  finalOutput += "selectDf.write.mode(\"overwrite\").saveAsTable(\""+ifConditionTable+"\")" + "\n"
                  selectDf.write.mode("overwrite").saveAsTable(ifConditionTable)

                  if (selectQuery.toUpperCase.lastIndexOf("WHERE") > 0) {
                    whereClause = selectQuery.substring(selectQuery.toUpperCase.lastIndexOf("WHERE"))
                  }

                }



                //val regexForTableName = "(FROM|from)[\\s\\S]+?(WHERE|where) *"

                //val pattern = Pattern.compile(regex, Pattern.MULTILINE)

                //val patternMatcher = pattern.matcher(selectQuery.trim);

                //println("============> TESTING DATAFRAME======>"+patternMatcher.find())
                //if(patternMatcher.find()){




                //}

              }

              println("LINE:->" + selectQuery)
              println("final output 2->" + finalOutput)
              //Making this replace so that the line should not be executed again.
              finalOutput = finalOutput.replace("SELECT", "SELCT")
              /*  spark.read
                  .format("jdbc")
                  .option("url", "jdbc:mysql://localhost:3306/mysql")
                  .option("dbtable", "mysql.v_table")
                  .option("user", "root")
                  .option("password", "root")
                  .load()
                  .createOrReplaceTempView("my_spark_table")
                spark.read
                  .format("jdbc")
                  .option("url", "jdbc:mysql://localhost:3306/mysql")
                  .option("dbtable", "mysql.drop_table")
                  .option("user", "root")
                  .option("password", "root")
                  .load()
                  .createOrReplaceTempView("my_spark_drop_table")*/
             /* selectQuery.replace("TNAME", id)
              if (selectQuery != null) {
                selectQuery = selectQuery.trim
              }
              finalSQL = "spark.sql(\"" + selectQuery + "\")" + "\n"
              finalSQL = finalSQL.replace("TNAME", "'\"+TNAME+\"'")
              selectQuery = selectQuery.replace("TNAME", id)
              if (selectQuery.lastIndexOf("WHERE") > 0) {
                whereClause = selectQuery.substring(selectQuery.lastIndexOf("WHERE"))
              }
              else if (selectQuery.lastIndexOf("where") > 0) {
                whereClause = selectQuery.substring(selectQuery.lastIndexOf("where"))
              }
              //selectQuery = selectQuery.replace("DROP_TABLE", "my_spark_drop_table")
              //selectQuery = selectQuery.replace("V_TABLE", "my_spark_table")
              selectQuery = selectQuery.replace("||", "\"+\"")
              //PUT A FLAG TO EXECUTE
              spark.sql(selectQuery)

              println("->select whereClause:" + whereClause)

              finalOutput += finalSQL
              println("final output 2->" + finalOutput)
              //Making this replace so that the line should not be executed again.
              finalOutput = finalOutput.replace("SELECT", "SELCT")*/
              sqlQueryTemp = ""

            }

          }
          /*else{
            exceptionOutput += line + "\n"
          }
    */
          //CHECKS FOR "IF FOUND THEN " WAS FOUND IN PREVIOUS QUERY THEN THIS WILL BE IF LOGIC, WE TRY TO EXTRACT IT
          else if ((exceptionOutput.toUpperCase.contains("IF FOUND THEN") || exceptionOutput.toUpperCase.contains("WHEN OTHERS THEN") || exceptionOutput.toUpperCase.contains("ELSE IF NOT FOUND THEN")) && (line.contains("'")) && (line.contains(":="))) {
            println("t value:" + line)
            var temp = line.substring((line.indexOf("'")), (line.indexOf("'")))

            var temp1 = line.substring((line.indexOf("'")), (line.indexOf(";")))
            ifQuery = temp.concat(temp1.replace("||", ""))
            ifQuery = ifQuery.replace("'", "")

            println("ifQuery value:" + ifQuery)

            //Making this replace so that the line should not be executed again.
            exceptionOutput = exceptionOutput.replace("IF FOUND THEN", "IF FOUND THN")
            exceptionOutput = exceptionOutput.replace("WHEN OTHERS THEN", "WHEN OTHERS THN")

            selectQuery = selectPart + " from " + ifConditionTable + " " + whereClause //"select * from test.v_drop WHERE UPPER(AME) =UPPER(" + id + ")"
            selectQuery = selectQuery.replace("TNAME",id)
            println("selectQuery value ->" + selectQuery)

            finalSQL = "var selectDF = spark.sql(\"" +selectQuery+"\") \n"
            finalSQL = finalSQL.replace(id,"TNAME")
            //select * from V_DROP WHERE UPPER(AME) =UPPER('\"+" + "TNAME + \"')\")" + "\n"

            finalOutput += finalSQL
            val selectDF = spark.sql(selectQuery)

            //selectDF.collect.foreach(println)


            var selectOutput: String = ""
            for (df1 <- selectDF.collect) {
              var temp3 = df1.toSeq
              temp3.foreach(selectOutput += _ + " ")

            }
            var tempCode = "var selectOutput: String = \"\"\n        for (df1 <- selectDF.collect) {\n  " +
              "        var temp3 = df1.toSeq\n          temp3.foreach(selectOutput += _ + \" \")\n\n        }"

            finalOutput += tempCode + "\n"

            println("contents of  temp2 table:" + selectOutput)
            println("contents of drop table." + selectOutput.contains(id) + ":id:" + id)
            if (selectOutput.contains(id.replace("'", ""))) {
              println("Going inside contents of drop table.")
              finalSQL = "spark.sql(\"" + ifQuery + "\")"
              finalSQL = finalSQL.replace("TNAME", "\"+TNAME+\"")
              //finalOutput +=finalSQL + "\n"
              tempCode = " if (selectOutput.contains(id.replace(\"'\", \"\"))) {\n          println(\"Going inside contents of drop table.\")\n "
              tempCode += finalSQL + "} \n"
              finalOutput += tempCode + "\n"
            }


            println("testDF:" + selectDF.collect.length)
            println("testDF count:" + selectDF.count)
            println("new sum:" + selectOutput)
            selectDF.show

          }

          else {
            exceptionOutput += line.trim + "\n"
            sqlQueryTemp = ""

          }

        }

        else {
          exceptionOutput += line.trim + "\n"
        }

      }



      println("spark.sql(" + selectQuery + ")")
      println("spark.sql(" + ifQuery + ")")
      println("finalArr->" + finalOutput)
      exceptionOutput = exceptionOutput.replace("IF FOUND THN", "IF FOUND THEN")
      exceptionOutput = exceptionOutput.replace("WHEN OTHERS THN", "WHEN OTHERS THEN")
      finalOutput = finalOutput.replace("SELCT", "SELECT")
      finalOutput = finalOutput.replace("RAISE NOTIE", "")
      finalOutput = finalOutput.replace("||", "")
      println("Exception Messages:" + exceptionOutput)







    }
    catch {
      case ex: Exception => {
        println("Final Exception: "+ex.getMessage + ":"+ex)
      }

      /* case e if (e.getMessage == null) => println ("Unknown exception with no message")
       case e => println ("An unknown error has been caught" + e.getMessage)*/
    }

    //var saveData=finalOutput + "\n EXCEPTIONS: \n" + exceptionOutput

    //var tempQuery = sqlQuery
    /*tempQuery = tempQuery.replace("'","""\'""")
    tempQuery = tempQuery.replace(""";""","""\;""")
    saveData = saveData.replace("'","""\'""")
    saveData = saveData.replace(""";""","""\;""")

    println("sqlQuerysqlQuery=> "+tempQuery)
    println("saveDatasaveData=> "+saveData)

    import java.text.SimpleDateFormat

    val cal = Calendar.getInstance
    val format1 = new SimpleDateFormat("yyyy-MM-dd HH-MM-SS")
    val formatted = format1.format(cal.getTime)
    println("TESTINGNOWformatted------->"+formatted)
    val now = Calendar.getInstance().getTime
    println("TESTINGNOW------->"+now)
    spark.sql("INSERT INTO TABLE test.SQLCONVERTER VALUES('"+tempQuery+"','"+saveData+"',null,null)")*/
    finally {
      spark.stop()
      sc.stop()
    }




    finalOutput = finalOutput.replace("#","<hash>")
    Ok(finalOutput + "\n # EXCEPTIONS: \n" + exceptionOutput)

  }
  }
  def sqlTest() = Action { implicit request: Request[AnyContent] => {
    var queryText =
      """SELECT TB1.COL1, TB2.COL2, TB3.COL3, TRIM(TB3.COL4) AS COL4, TB1.COL2, TB1.COL9 FROM TABLE9 TB9 JOIN TABLE_12 TB12
         ON (TB1.COL = TB2.COL1 AND TB2.COL2 =TB1.COL1 ) WHERE A = 1;
        """;
    queryText = queryText.replaceAll("\n", " ");
    var test= getTables(queryText);
    var output = ""
    var temp = test.get("Text").toString();
    temp = temp.replace("Some(","");
    temp = temp.substring(0, temp.length-1)
    output = output + temp +"\n"
    val toPass = test.filterKeys(_ != "Text")
    output = output + getJoin(queryText, toPass)+"\n";
    output = output + getSelect(queryText)+".\n";
    output = output + getWhere(queryText)+"\n";
    Ok(output)
  }
  }
  //METHOD TO GET THE JOINS
  def getWhere (query: String):String = {
    var myVar: Int = 0;
    var st = "";
    var queryText = query;
    var tempWhere = queryText;
    var st2 = "";
    var alias ="";
    var len: Int = 0;
    var len1: Int = 0;
    var whereMap = scala.collection.mutable.LinkedHashMap[String,String]();
    myVar = queryText.indexOf("WHERE");
    queryText = queryText.substring(myVar, queryText.indexOf(";"));
    queryText = queryText.replaceFirst("WHERE ", "");
    while (queryText.contains("AND")|| queryText.contains("OR") ) {
      len = queryText.indexOf("AND")
      len1 = queryText.indexOf("OR")
      if(len1 < len){
        st = "AND"
      }else{
        len = len1
        st = "OR"
      }
      tempWhere = queryText.substring(0,len)
      len = tempWhere.indexOf("=")
      if(len == -1){
        len = tempWhere.indexOf("<")
      }
      if(len == -1){
        len = tempWhere.indexOf(">")
      }
      st2 = tempWhere.substring(0, len);
      st2.trim();
      tempWhere = tempWhere.substring(len+2, tempWhere.length);
      tempWhere.trim();
      whereMap += (st2 -> tempWhere);
      queryText = queryText.substring(len+2, queryText.length)
      queryText = queryText.replace(tempWhere,"")
      queryText = queryText.replaceFirst(st,"")
      queryText.trim();
    }
    tempWhere = queryText
    len = tempWhere.indexOf("=")
    if(len == -1){
      len = tempWhere.indexOf("<")
    }
    if(len == -1){
      len = tempWhere.indexOf(">")
    }
    st2 = tempWhere.substring(0, len);
    st2.trim();
    tempWhere = tempWhere.substring(len+2, tempWhere.length);
    whereMap += (st2 -> tempWhere);
    var output ="where( "
    for ((k,v) <- whereMap) {
      output =output+ ",";
      output =output+ " && "+"col(\""+k+"\" === \""+v+"\")";
      output = output.replaceFirst(",","")
    }
    output = output.replaceFirst(" &&","")
    output = output+")"
    output
  }

  //METHOD TO GET THE JOINS
  def getSelect(query: String):String = {
    var myVar: Int = 0;
    var queryText = query;
    var tempSelect = queryText;
    var st2 = "";
    var alias ="";
    var len: Int = 0;
    var selectMap = scala.collection.mutable.Map[String,String]();
    myVar = queryText.indexOf("SELECT ");
    queryText = queryText.substring(myVar, queryText.length);
    queryText = queryText.replaceFirst("SELECT ", "");
    len = queryText.indexOf("FROM ");
    if(len == -1){
      len = queryText.indexOf(";")
    }
    tempSelect = queryText.substring(0,len);

    queryText = queryText.replaceFirst(tempSelect,"");
    queryText.trim();
    tempSelect = tempSelect.trim();

    while (tempSelect.contains(",")) {
      len = tempSelect.indexOf(",");
      var st2 = tempSelect.substring(0,len);
      if(tempSelect.contains("SUM")|| tempSelect.contains("TRIM")) {
        tempSelect = tempSelect.replace(st2, "");
      }else{
        tempSelect =tempSelect.replaceFirst(st2, "")
      }
      tempSelect = tempSelect.replaceFirst(",", "");

      st2.trim();
      alias = st2;
      tempSelect.trim();
      selectMap += (st2 -> alias);

    }
    selectMap += (tempSelect -> tempSelect);
    var output = "select( ";
    var temp ="";
    for ((k,v) <- selectMap) {
      output =output+ ",";
      var col = k;
      if(col.contains("AS")){
        if(col.contains("SUM")){
          output =output+ "sum(";
          col = col.replace("SUM(","")
          col = col.replace(")","")
        }
        if(col.contains("TRIM")){
          output =output+ "trim(";
          col = col.replace("TRIM(","")
          col = col.replace(")","")
        }
        len = col.indexOf("AS")
        temp = col.substring(len,col.length)
        col = col.substring(0,len)
        temp =temp.trim()
        col = col.trim()
      }
      col.trim()
      output =output+ "col(\""+col+"\")";
      if(temp.contains("AS")){
        temp = temp.replace("AS","")
        temp = temp.trim()

        output =output + ").alias("+"\""+temp+"\")"
      }
    }
    output = output + ")"
    output = output.replaceFirst(",", "");
    output
  }

  //METHOD TO GET THE JOINS
  def getJoin(query: String, map: scala.collection.Map[String, String]) :String = {
    var myVar: Int = 0;
    var myVar1: Int = 0;
    var st = "";
    var queryText = query;
    var tempJoin = queryText;
    var st2 = "";
    var alias ="";
    var len: Int = 0;
    var joinMap = scala.collection.mutable.Map[String,String]();
    queryText = queryText.substring(0, queryText.indexOf("WHERE"))
    while (queryText.contains("ON")) {
      myVar = queryText.indexOf("ON");
      queryText = queryText.substring(myVar, queryText.length);
      queryText = queryText.replaceFirst("ON ", "");
      len = queryText.indexOf("JOIN")
      if(len == -1){
        len = queryText.indexOf(";")
      }
      if(len == -1){
        len = queryText.length()
      }
      tempJoin = queryText.substring(0,len);
      queryText = queryText.replaceFirst(tempJoin,"");
      queryText.trim();
      tempJoin = tempJoin.trim();

      while (tempJoin.contains("AND")) {
        myVar1 = tempJoin.indexOf("=");
        st2 = tempJoin.substring(0, myVar1);
        st2 = st2.trim();
        tempJoin = tempJoin.replace(st2, "");
        tempJoin = tempJoin.replaceFirst("=", "");
        tempJoin = tempJoin.trim();
        myVar = tempJoin.indexOf(" ");
        alias = tempJoin.substring(0 ,myVar);
        alias = alias.trim();
        tempJoin = tempJoin.replaceFirst(alias, "");
        tempJoin = tempJoin.replaceFirst("AND", "");
        tempJoin = tempJoin.trim();
        if (st.matches("[a-zA-Z0-9]*")) {
          joinMap += (st2 -> alias);
        }
      }
      myVar1 = tempJoin.indexOf("=");
      st2 = tempJoin.substring(0, myVar1);
      st2 = st2.trim();

      tempJoin = tempJoin.replace(st2,"")
      tempJoin = tempJoin.replaceFirst("=","");
      tempJoin = tempJoin.trim();
      alias = tempJoin.substring(0 ,tempJoin.length());
      alias = alias.trim();
      tempJoin = tempJoin.replace(alias, "");
      if (st.matches("[a-zA-Z0-9]*")) {
        joinMap += (st2 -> alias);
      }
    }
    var output = "";
    output= "val final_query = "
    var count : Int = 0
    for ((k,v) <- map) {
      if(count == 0){
        output = output +"df_"+k+".alias("+v+").join("
        count = count+1
      }else{
        output = output +"df_"+k+".alias("+v+"),"
      }
    }
    for ((k,v) <- joinMap) {
      var tempON= k
      var tempV = v
      if(tempON.contains("(")){
        tempON = tempON.replace("(","")
      }
      if(tempV.contains(")")){
        tempV = tempV.replace(")","")
      }
      output =output+ " && "+"$\""+tempON+"\" === $\""+tempV+"\"";
    }
    output = output.replaceFirst(" && ", "")
    output = output+").";
    output
  }

  //METHOD TO GET THE TABLE NAMES
  def getTables(query: String) : scala.collection.mutable.Map[String, String] ={
    var myVar: Int = 0;
    var myVar1: Int = 0;
    var st = "";
    var queryText = query;
    var tempJoin = queryText;
    var st2 = "";
    var alias ="";
    var tableMap = scala.collection.mutable.Map[String,String]()
    while (queryText.contains("FROM")) {
      myVar = queryText.indexOf("FROM");
      queryText = queryText.substring(myVar, queryText.length);
      queryText = queryText.replaceFirst("FROM ", "");
      queryText = queryText.trim();
      myVar1 = queryText.indexOf(" ");
      st = queryText.substring(0, myVar1);
      queryText = queryText.substring(myVar1, queryText.length);
      queryText = queryText.trim();
      myVar1 = queryText.indexOf(" ");
      alias = queryText.substring(0, myVar1);
      if (st.matches("[a-zA-Z0-9]*")) {
        tableMap += (st -> alias);
      }
      queryText = queryText.replaceFirst(alias, "");
    }
    while (tempJoin.contains("JOIN")) {
      myVar = tempJoin.indexOf("JOIN");
      tempJoin = tempJoin.substring(myVar, tempJoin.length);
      tempJoin = tempJoin.replaceFirst("JOIN ", "");
      tempJoin.trim();
      myVar1 = tempJoin.indexOf(" ");
      st = tempJoin.substring(0, myVar1);
      tempJoin = tempJoin.substring(myVar1, tempJoin.length);
      tempJoin = tempJoin.trim();
      myVar1 = tempJoin.indexOf(" ");
      alias = tempJoin.substring(0, myVar1);
      if (st.matches("[a-zA-Z0-9_]*")) {
        tableMap += (st -> alias);
      }
      tempJoin = tempJoin.replaceFirst(alias, "");
      tempJoin = tempJoin.replaceFirst("st", "");
    }
    var output = "";
    for ((k,v) <- tableMap) {
      output =output+ "\n"+"val df_"+k+"= spark.sql(\"\"\"Select * from "+k+"\"\"\")";
    }
    Ok("done")
    tableMap += ("Text" -> output);
    tableMap
  }
  def testSqlPost() = Action { implicit request: Request[AnyContent] => {
    val id ="'TEST2'"
    val form = Form(
      tuple(
        "databaseName"-> text,
        "sqlQuery"-> text

      )
    )
    def values = form.bindFromRequest.data
    def databaseName = values("databaseName")
    def sqlQuery = values("sqlQuery")
    var tempQuery = sqlQuery



    var queryText = tempQuery;
    /*"""SELECT TB1.COL1, TB2.COL2, TB3.COL3, TRIM(TB3.COL4) AS COL4, TB1.COL2, TB1.COL9 FROM TABLE9 TB9 JOIN TABLE_12 TB12
       ON (TB1.COL = TB2.COL1 AND TB2.COL2 =TB1.COL1 ) WHERE A = 1;
      """;*/
    queryText = queryText.replaceAll("\n", " ");
    var test= getTables(queryText);
    var output = ""
    var temp = test.get("Text").toString();
    temp = temp.replace("Some(","");
    temp = temp.substring(0, temp.length-1)
    output = output + temp +"\n"
    val toPass = test.filterKeys(_ != "Text")
    output = output + getJoin(queryText, toPass)+"\n";
    output = output + getSelect(queryText)+".\n";
    output = output + getWhere(queryText)+"\n";
    Ok(output+"#")
  }
}


}






