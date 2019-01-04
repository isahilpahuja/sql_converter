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



/**
 * This controller creates an `Action` to handle HTTP requests to the
 * application's home page.
 */
@Singleton
class SQLConversionController @Inject()(cc: ControllerComponents) extends AbstractController(cc) {


  /**
    * Create an Action to render an HTML page.
    *
    * The configuration in the `routes` file means that this method
    * will be called when the application receives a `GET` request with
    * a path of `/`.
    */
  def readFile() = Action { implicit request: Request[AnyContent] => {

    //val sc = new SparkContext("local", "Application", "/path/to/spark-0.9.0-incubating", List("target/scala-2.10/playsparkapp_2.10-1.0-SNAPSHOT.jar"))
    val conf = new SparkConf().setAppName("SOME APP NAME").setMaster("local[2]").set("spark.executor.memory", "1g")

    val sc = new SparkContext(conf)
    val logData = sc.textFile("file:///C:/Users/pushukla/Documents/project/FI/SQL_CONVERTER_FRAMEWORK/poc/play-scala-seed/app/controllers/test.txt")
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

  /*=>
    widgetRepo.select(BSONDocument(Id -> BSONObjectID(id))).map(widget => Ok(Json.toJson(widget)))*/

  def readText(id: String) = Action { implicit request: Request[AnyContent] => {


    val conf = new SparkConf().setAppName("SOME APP NAME").setMaster("local[2]").set("spark.executor.memory", "1g").set("spark.driver.allowMultipleContexts", "true")

    val sc = new SparkContext(conf)
    val storedProcFile = sc.textFile("file:///C:/Users/pushukla/Documents/project/FI/SQL_CONVERTER_FRAMEWORK/poc/play-scala-seed/app/controllers/test1.txt")

    val convertFileToArray = storedProcFile.collect().toArray
    var finalOutput = ""
    var sqlQueryTemp = ""
    var selectQueryTemp = ""
    var messageOutput = ""
    var exceptionOutput = ""
    var selectQuery = ""
    var finalSQL = ""
    var ifQuery = ""
    var printMessage = ""
    var messageCounter = 0

    val spark = SparkSession
      .builder
      .appName("Spark Examples").config("spark.sql.warehouse.dir", "file:///C:/Users/pushukla/Documents/project/FI/SQL_CONVERTER_FRAMEWORK/poc/play-scala-seed/spark-warehouse/")
      .config("spark.some.config.option", "some-value")
      .getOrCreate()
    for (line <- convertFileToArray) {
      println("NEW TESTING line->:"+line)
      println("NEW TESTING printMessage->:"+printMessage)
      if(line.toUpperCase.contains("RAISE NOTICE") || printMessage.toUpperCase.contains("RAISE NOTICE") ){


        if(messageCounter == 0 ) {
          if (line.contains(";")) {
          printMessage += line.substring(line.indexOf("RAISE NOTICE"),line.indexOf(";"))
            printMessage = printMessage.replace("RAISE NOTICE","RAISE NOTIE")
            messageOutput = "println(\"" + printMessage +";\") \n"
            println("CHECK TESTING-> "+messageOutput)
            finalOutput += messageOutput
            println("TESTING-> "+finalOutput)
            messageCounter = 0
            printMessage = ""
        }
          else{
            printMessage += line.substring(line.indexOf("RAISE NOTICE"))
            messageCounter = messageCounter +1
          }
        }
        else{
          if (line.contains(";")) {
            printMessage += line.substring(0,line.indexOf(";"))
            messageOutput = "println(\"" + printMessage +";\") \n"
            println("CHECK TESTING else-> "+messageOutput)
            finalOutput += messageOutput
            println("TESTING else-> "+finalOutput)
            messageCounter = 0
            printMessage = ""
          }
          else{
            printMessage += line
            messageCounter = messageCounter +1
          }
        }


      }



      else if (!(line.toUpperCase.trim.startsWith("EXCEPTION") || line.contains("$") || line.toUpperCase.trim.startsWith("RETURN") || line.toUpperCase.trim.startsWith("ELSE IF NOT FOUND THEN") || line.toUpperCase.trim.startsWith("WHEN OTHERS THEN") || line.toUpperCase.trim.startsWith("IF FOUND THEN") || line.toUpperCase.trim.startsWith("CREATE") || line.toUpperCase.trim.startsWith("DECLARE") || line.toUpperCase.trim.startsWith("RETURNS") || line.toUpperCase.trim.startsWith("BEGIN") || line.toUpperCase.trim.startsWith("LANGUAGE") || line.toUpperCase.trim.startsWith("END") || line.toUpperCase.trim.startsWith("--") || line.toUpperCase.trim.startsWith("EXECUTE IMMEDIATE"))) {




        /*println("LINE:->"+line)
        println("finalOutput:->"+finalOutput)*/
        //IF SELECT FOUND IN A ROW, THEN WILL CHECK SEMI-COLON EXISTS
        sqlQueryTemp += line + "\n"
        if ((sqlQueryTemp.contains(":=") && sqlQueryTemp.contains("'")) && !((exceptionOutput.toUpperCase.contains("IF FOUND THEN") || exceptionOutput.toUpperCase.contains("WHEN OTHERS THEN") || exceptionOutput.toUpperCase.contains("ELSE IF NOT FOUND THEN")))) {
          //sqlQueryTemp += line + "\n"
          println("=>test2. Inside :=@@"+sqlQueryTemp)
          if (line.contains(";")) {
            println("=>test3. Inside line=> " + line)
            println("=>test3. Inside sqlQueryTemp=> " + sqlQueryTemp)
            selectQuery = sqlQueryTemp.substring(sqlQueryTemp.indexOf(":=")+2, (sqlQueryTemp.indexOf(";")))

            println("=>LINE:->" + selectQuery)
            spark.read
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
              .createOrReplaceTempView("my_spark_drop_table")
            selectQuery.replace("TNAME", id)
            if (selectQuery != null) {
              selectQuery = selectQuery.trim
            }
            finalSQL = "spark.sql(\"" + selectQuery + "\")" + "\n"
            selectQuery = selectQuery.replace("TNAME", id)
            selectQuery = selectQuery.replace("DROP_TABLE", "my_spark_drop_table")
            selectQuery = selectQuery.replace("V_TABLE", "my_spark_table")
            //PUT A FLAG TO EXECUTE
            //spark.sql(selectQuery)


            finalOutput += finalSQL
            println("final output 1->"+finalOutput)
            //Making this replace so that the line should not be executed again.
            finalOutput = finalOutput.replace(":=", " ")
            sqlQueryTemp = ""
          }


        }

        else if ((sqlQueryTemp.toUpperCase.contains("SELECT")) ) {
          println("test2. Inside Select.")
          //sqlQueryTemp += line + "\n"
          if (line.contains(";")) {
            println("test3. Inside ;. " + line)
            selectQuery = sqlQueryTemp.substring(0, (sqlQueryTemp.indexOf(";")))

            println("LINE:->" + selectQuery)
            spark.read
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
              .createOrReplaceTempView("my_spark_drop_table")
            selectQuery.replace("TNAME", id)
            if (selectQuery != null) {
              selectQuery = selectQuery.trim
            }
            finalSQL = "spark.sql(\"" + selectQuery + "\")" + "\n"
            selectQuery = selectQuery.replace("TNAME", id)
            selectQuery = selectQuery.replace("DROP_TABLE", "my_spark_drop_table")
            selectQuery = selectQuery.replace("V_TABLE", "my_spark_table")
            //PUT A FLAG TO EXECUTE
            //spark.sql(selectQuery)


            finalOutput += finalSQL
            println("final output 2->"+finalOutput)
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

          selectQuery = "select * from my_spark_drop_table WHERE UPPER(TABLENAME) =UPPER(" + id + ")"
          println("selectQuery value ->" + selectQuery)

          val testDF = spark.sql(selectQuery)
          testDF.collect.foreach(println)


          var selectOutput: String = ""
          for (df1 <- testDF.collect) {
            var temp3 = df1.toSeq
            temp3.foreach(selectOutput += _ + " ")

          }


          println("contents of  temp2 table:" + selectOutput)
          println("contents of drop table." + selectOutput.contains(id) + ":id:" + id)
          if (selectOutput.contains(id.replace("'", ""))) {
            println("Going inside contents of drop table.")
            finalSQL = "spark.sql(\"" + ifQuery + "\")"
            finalOutput +=finalSQL
          }

          println("testDF:" + testDF.collect.length)
          println("testDF count:" + testDF.count)
          println("new sum:" + selectOutput)
          testDF.show

        }

        else {
          exceptionOutput += line + "\n"
          sqlQueryTemp = ""

        }

      }

      else {
        exceptionOutput += line + "\n"
      }

    }


    spark.stop()
    println("spark.sql(" + selectQuery + ")")
    println("spark.sql(" + ifQuery + ")")
    println("finalArr->" + finalOutput)
    exceptionOutput = exceptionOutput.replace("IF FOUND THN", "IF FOUND THEN")
    exceptionOutput = exceptionOutput.replace("WHEN OTHERS THN", "WHEN OTHERS THEN")
    finalOutput = finalOutput.replace("SELCT", "SELECT")
    finalOutput = finalOutput.replace("RAISE NOTIE", "")
    finalOutput = finalOutput.replace("||", "")
    println("Exception Messages:" + exceptionOutput)
    sc.stop()

    Ok(finalOutput + "\n EXCEPTIONS: \n" + exceptionOutput)

  }
  }

  def ifFoundThenComputation(tableName: String, whereClause: String): String = {

    var selectQueryForIf = "SELECT * FROM " + tableName + " WHERE " + whereClause;

    selectQueryForIf
  }

  /*def convertSQL = Action.async { request =>
    val user = request.body.asFormUrlEncoded.get.get("user").head
    Future(Ok())
  }*/

  /*def addBook = Action() {  request =>

    val newBook = request.body.as[Book]

    println("new Book value:-> "+newBook)
    val user1 = request.queryString.get("isbn")
    val user = request.queryString.get("isbn").flatMap(_.headOption)
    println("user values 1: "+user1)
    println("user values: "+user)
    /*val JsValue_json_string: JsValue = Json.parse(json_string)
    val parse_json_return = (JsValue_json_string \ "children" \ 0 \ "href").get*/
    val books = newBook

    println("testing->" + books)
    println("testing json->" + Json.stringify(Json.toJson(books)))
    val name = request.queryString.get("isbn")

    val map : Map[String,Seq[String]] = request.body
    val seq1 : Seq[String] = map.getOrElse("isbn", Seq[String]())
    val seq2 : Seq[String] = map.getOrElse("id", Seq[String]())
    val socketId = seq1.head
    val channelName = seq2.head

    val newuser = request.body("isbn").head
    println("new user socketId::="+socketId+"-channelName-"+channelName)
    Ok(Json.stringify(Json.toJson(books)))

  }*/


  def addBook = Action { implicit request =>

    val form = Form(
      tuple(
        "databaseName" -> text,
        "sqlQuery" -> text

      )
    )
    def values = form.bindFromRequest.data
    def databaseName = values("databaseName")
    def sqlQuery = values("sqlQuery")

    println("databaseName: "+databaseName)
    println("sqlQuery-> "+sqlQuery)


    val pw = new PrintWriter(new File("hello.txt" ))
    pw.write(sqlQuery)
    pw.close

   // val newBook = request.body.as[Book]
    Ok(databaseName+" , "+sqlQuery)
  }


  def readMySQL() = Action { implicit request: Request[AnyContent] => {
    val url = "jdbc:mysql://localhost:3306/mysql"
    val driver = "com.mysql.jdbc.Driver"
    val username = "root"
    val password = "root"
    var connection: Connection = null
    try {
      println("Go inside try.")
      Class.forName(driver)
      connection = DriverManager.getConnection(url, username, password)
      val statement = connection.createStatement
      val rs = statement.executeQuery("SELECT host, user FROM user")
      while (rs.next) {
        val host = rs.getString("host")
        val user = rs.getString("user")
        println("host = %s, user = %s".format(host, user))
      }
    } catch {
      case e: Exception => e.printStackTrace
    }
    connection.close
    Ok("JDBC Testing")

  }
  }

  def readSQLQueries() = Action { implicit request: Request[AnyContent] => {
    System.setProperty("hadoop.home.dir", "C:/winutils")
    val conf = new SparkConf().setAppName("SOME APP NAME").setMaster("local[2]").set("spark.executor.memory", "1g").set("spark.driver.allowMultipleContexts", "true").set("spark.sql.crossJoin.enabled","true")
    val sc = new SparkContext(conf)
    val data_file = sc.textFile("file:///C:/Users/pushukla/Documents/project/FI/SQL_CONVERTER_FRAMEWORK/poc/play-scala-seed/app/controllers/sql1.txt")
    val spark = SparkSession
      .builder
      .appName("Spark Examples").config("spark.sql.warehouse.dir", "file:///C:/Users/jaypanda/Desktop/My Softs/new_poc/playframework/new_poc/spark-warehouse/spark-warehouse/")
      .config("spark.some.config.option", "some-value")
      .getOrCreate()
    val testArr = data_file.collect().toArray

    var finalArr = ""
    var exceptionArr = ""

    var selectQuery = ""
    for (t <- testArr) {
      if (!(t.trim.startsWith("CREATE") || t.trim.startsWith("--"))) {

        if ((t.contains("select")) && (t.contains(";"))) {
          spark.read
            .format("jdbc")
            .option("url", "jdbc:mysql://localhost:3306/mysql")
            .option("dbtable", "mysql.emp")
            .option("user", "root")
            .option("password", "root")
            .load()
            .createOrReplaceTempView("my_spark_emp_table")
          spark.read
            .format("jdbc")
            .option("url", "jdbc:mysql://localhost:3306/mysql")
            .option("dbtable", "mysql.dept")
            .option("user", "root")
            .option("password", "root")
            .load()
            .createOrReplaceTempView("my_spark_dept_table")
          spark.read
            .format("jdbc")
            .option("url", "jdbc:mysql://localhost:3306/mysql")
            .option("dbtable", "mysql.salgrade")
            .option("user", "root")
            .option("password", "root")
            .load()
            .createOrReplaceTempView("my_spark_salgrade_table")

          var temp = t.substring(t.indexOf("select"), t.indexOf(";"))
          //temp = "spark.sql(\"" + temp + "\")"
          selectQuery = selectQuery + temp + "\n"
          temp = temp.replace("emp ", "my_spark_emp_table ")
          temp = temp.replace("dept ", "my_spark_dept_table ")
          temp = temp.replace("salgrade ", "my_spark_salgrade_table ")
          println("selectQuery-> "+selectQuery)
          println("temp-> "+temp)
          finalArr  += "spark.sql(\"" + temp + "\") \n"
          val sqlOutput = spark.sql(temp).show


        }

      }
      else {
        exceptionArr += t + "\n"
      }
    }
    println("Query:" + finalArr)
    //println("spark.sql("+c+")")
    // println(finalArr)
    println("exception message->" + exceptionArr)
    Ok(finalArr+"\n  EXCEPTIONS: "+exceptionArr)
  }
  }
}
