package service

import java.io.{BufferedInputStream, File, FileInputStream}
import java.text.SimpleDateFormat
import java.util.{Date, Properties}

import Util.HdfsUtil
import org.apache.hadoop.fs.Path
import org.apache.spark.SparkConf
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SaveMode, SparkSession}

import scala.collection.mutable.{ArrayBuffer, ListBuffer}


object CollectHiveTableSize {
  def main(args: Array[String]): Unit = {
    if (args.length < 1) {
      println("--class classname  path for hdfs-audit.log ")
    }

    System.setProperty("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    val sparkConf = new SparkConf().setAppName("CollectHiveTableSize4day").setMaster("local")
    val spark = SparkSession.builder
      .config(sparkConf).getOrCreate()
    val sc = spark.sparkContext
    //设置日志级别
    sc.setLogLevel("warn")
    import java.sql.DriverManager
    val sqlContext = spark.sqlContext

    //读取mproperties的相关配置
    val properties = new Properties
    //val path = Thread.currentThread().getContextClassLoader.getResource("db.properties").getPath
    val path = args(1).toString
    properties.load(new FileInputStream(path))
    val url_in = properties.getProperty("url_in")
    val url_out = properties.getProperty("url_out")
    val user_in = properties.getProperty("user_in")
    val user_out = properties.getProperty("user_out")
    val password_in = properties.getProperty("password_in")
    val password_out = properties.getProperty("password_out")
    val begin_with = properties.getProperty("begin_with")


    //val dbc = "jdbc:mysql://192.168.184.61:3306/hive?user=hive&password=hive"
    val dbc = url_in + "?user=" + user_in + "&password=" + password_in
    try {
      classOf[com.mysql.jdbc.Driver]
      val connection = DriverManager.getConnection(dbc)
      val statement = connection.createStatement()

      //从 TBLS A, DBS B, SDS  C,SERDES D表中查询出DB TABLE_NAME LOCATION 等表的相关信息
      val resultSet = statement.executeQuery("SELECT B.NAME AS DB, A.TBL_NAME AS TABLE_NAME, " +
        "A.SD_ID, A.TBL_TYPE,A.CREATE_TIME  , C.LOCATION,D.SLIB " +
        "FROM TBLS A, DBS B, SDS  C,SERDES D " +
        "WHERE A.`DB_ID` = B.`DB_ID` " +
        "AND C.`SD_ID` = A.`SD_ID` " +
        "AND C.`SERDE_ID`=D.`SERDE_ID` " +
        "AND D.SLIB  not like  '%hbase%' ")


      //获取今日的时间
      val now: Date = new Date()
      val dateFormat: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd")
      val date = dateFormat.format(now)

      val statement2 = connection.createStatement()


      //初始化list为可变list
      var list = new ListBuffer[ArrayBuffer[String]]

      while (resultSet.next()) {
        try {

          val DB = resultSet.getString(1)
          val TABLE_NAME = resultSet.getString(2)
          val SD_ID = resultSet.getString(3)
          val TBL_TYPE = resultSet.getString(4)
          val CREATE_TIME = resultSet.getString(5)
          val LOCATION = resultSet.getString(6)
          val SLIB = resultSet.getString(7)
          var WRITE_COUNT = 0
          var READ_COUNT = 0

          //对LOCAION字段做处理，将字符串替换为和日志统一格式

          val src = LOCATION.replace(begin_with, "src=")

          //获取文件系统的类型
          val fileSystem = HdfsUtil.getFileSystem()
          if (fileSystem.exists(new Path(LOCATION))) {

            //调用getFileSize方法 得到文件的大小FILESIZE
            val FILESIZE = HdfsUtil.getFileSize(fileSystem, LOCATION).toString()

            //将字段写入到mysql中
            val result = statement2.executeUpdate("INSERT into TABLEINFO values('" + date + "','" + DB + "','" + TABLE_NAME + "','" + SD_ID + "','" + TBL_TYPE + "','" + CREATE_TIME + "','" + LOCATION + "','" + SLIB + "','" + FILESIZE + "','" + WRITE_COUNT + "','" + READ_COUNT + "','" + src + "')" +
              "ON DUPLICATE KEY UPDATE date='" + date + "',DB='" + DB + "',TABLE_NAME='" + TABLE_NAME + "'")
            //用list收集每行的（LOCATION,FILESIZE)
            val arr = scala.collection.mutable.ArrayBuffer[String]()
            arr += (LOCATION, FILESIZE)
            list += arr

          } else {
            val FILESIZE = "null"
            val result = statement2.executeUpdate("INSERT into TABLEINFO values('" + date + "','" + DB + "','" + TABLE_NAME + "','" + SD_ID + "','" + TBL_TYPE + "','" + CREATE_TIME + "','" + LOCATION + "','" + SLIB + "','" + FILESIZE + "','" + WRITE_COUNT + "','" + READ_COUNT + "','" + src + "') " +
              "ON DUPLICATE KEY UPDATE date='" + date + "',DB='" + DB + "',TABLE_NAME='" + TABLE_NAME + "'")

            // map +=(LOCATION->FILESIZE)
            val arr = scala.collection.mutable.ArrayBuffer[String]()
            arr += (LOCATION, FILESIZE)
            list += arr

          }

          /* val FILESIZE = "null"
           val result = statement2.executeUpdate("INSERT into TABLEINFO values('" + date + "','" + DB + "','" + TABLE_NAME + "','" + SD_ID + "','" + TBL_TYPE + "','" + CREATE_TIME + "','" + LOCATION + "','" + SLIB + "','" + FILESIZE + "','" + WRITE_COUNT + "','" + READ_COUNT + "','" + src + "') " +
             "ON DUPLICATE KEY UPDATE date='" + date + "',DB='" + DB + "',TABLE_NAME='" + TABLE_NAME + "'")

           // map +=(LOCATION->FILESIZE)
           val arr = scala.collection.mutable.ArrayBuffer[String]()
           arr += (LOCATION, FILESIZE)
           list += arr
 */

        } catch {
          case e: Exception => e.printStackTrace()
        }
      }

      //将list注册为rdd
      val rdd1 = sc.makeRDD(list)
      //对rdd1进行处理 变成rdd[Row]格式的rdd2
      val rdd2 = rdd1.map(x =>
        Row(x(0), x(1))
      )

      //设置schema的格式
      val schema = StructType(
        Seq(
          StructField("LOCATION", StringType, true),
          StructField("FILESIZE", StringType, true)
        )
      )

      //将rdd2转换为datafream
      val rdd_df = spark.createDataFrame(rdd2, schema)
      //将rdd_df注册为临时表FILESIZE_TABLE
      rdd_df.createOrReplaceTempView("FILESIZE_TABLE")


      //从日志文件中获取datafream
      import sqlContext.implicits._

      // val file_df = sc.textFile("d://hdfs-audit.log").map(line => {
      val file_df = sc.textFile(args(0).toString).map(line => {
        val fields = line.replaceAll("\\t", " ").split(" ")

        val length = fields.size
        var date = ""
        var cmd = ""
        var src = ""
        //这里有两种情况，日志中的字段可能是16也可能是13
        if (length == 16) {
          date = fields(0)
          cmd = fields(11)
          src = fields(12)
        } else if (length == 13) {
          date = fields(0)
          cmd = fields(8)
          src = fields(9)
        }


        (date, cmd, src)


      }).toDF().withColumnRenamed("_1", "DATE").withColumnRenamed("_2", "cmd").withColumnRenamed("_3", "src")


      //从数据库中获取datafream
      val db_df = sqlContext.read.format("jdbc").options(
        Map("url" -> dbc,
          "dbtable" -> "TABLEINFO")).load()

      //从file_df，db_df两个datafream注册的临时表中查询出相同DATE和匹配的src对应的数据
      file_df.createOrReplaceTempView("file_table")
      db_df.createOrReplaceTempView("db_table")

      val result_df1 = spark.sql("select b.DATE,b.DB,b.TABLE_NAME,b.LOCATION as LOCATION,a.cmd as cmd ,b.WRITE_COUNT as WRITE,b.READ_COUNT as READ from file_table a,db_table b where a.DATE=b.DATE  and a.src like concat('%',b.src,'%')").toDF()

      result_df1.createOrReplaceTempView("result_df1_table")

      //对result_df1_table用DATE,DB,TABLE_NAME,LOCATION,cmd做groupBy 得出每个cmd对应的count
      val result_df3 = spark.sql("select DATE,DB,TABLE_NAME,LOCATION," +
        " substring(cmd, 5) as cmd,count(cmd) as cmdcount from result_df1_table group by DATE,DB,TABLE_NAME,LOCATION,cmd ").toDF()

      //对cmd字段进行过滤
      val result_df4 = result_df3.where($"cmd".isin("open", "create", "append", "detele", "mkdirs", "rename"))

      result_df4.createOrReplaceTempView("result_df4_table")

      //FILESIZE_TABLE和result_df4_table做join
      val result_df5 = spark.sql("SELECT a.DATE,a.DB,a.TABLE_NAME,a.LOCATION,a.cmd,a.cmdcount,b.FILESIZE FROM result_df4_table a join FILESIZE_TABLE b ON (a.LOCATION = b.LOCATION)").toDF()


      //将结果写到mysql中
      val properties = new Properties()
      properties.setProperty("user", user_out)
      properties.setProperty("password", password_out)
      //result_df5.write.mode(SaveMode.Append).jdbc("jdbc:mysql://192.168.184.61:3306/hive", "RESULT_DF", properties)

      result_df5.write.mode(SaveMode.Append).jdbc(url_out, "RESULT_DF", properties)


    } catch {
      case e: Exception => e.printStackTrace()
    } finally {}

  }


}
