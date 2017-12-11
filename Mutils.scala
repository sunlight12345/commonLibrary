package com.tcl.common.utils

import java.{io, util}
import java.io.FileWriter
import java.net.URI
import java.text.SimpleDateFormat
import java.util.{Calendar, Date, Properties}

import com.tcl.common.utils.MySQLPropertyUtils.getInsertSql
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, FileUtil, Path}
import org.apache.hadoop.io.compress.GzipCodec
import org.apache.log4j.Logger
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SQLContext}

import scala.collection.mutable.ArrayBuffer
import scala.reflect.io.File

object Mutils {
  val logger: Logger = Logger.getLogger(getClass.getSimpleName)

  /**
    * 求时间差(Hour),返回保留4位小数的字符串
    *
    * @param startTime startTime
    * @param endTime   endTime
    * @return ###.0000
    */
  def getStampDiff(startTime: String, endTime: String): String = {
    //判断属性是否符合时间格式 yyyy-MM-dd HH:mm:ss
    val regexS =
      """\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}"""
    if (!startTime.matches(regexS) || !endTime.matches(regexS)) return ""
    val format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val st1 = format.parse(startTime).getTime
    val st2 = format.parse(endTime).getTime
    val interval = st2 - st1
    if (interval >= 0) {
      val diff = (st2 - st1) / (1000 * 60 * 60 * 1.0)
      import java.text.DecimalFormat
      val df = new DecimalFormat("###.0000")
      val s = df.format(diff)
      if (s.indexOf(".") != 0) s else "0" + s
    } else {
      ""
    }
  }

  /**
    * 判断clientType字段是否合法，大小写字母开头即合法，否则非法
    */
  def isClientTypeValid(clientTypeStr: String): Boolean = {
    val cliPattern = """[a-zA-Z]""".r
    val clientType = cliPattern.findPrefixOf(clientTypeStr)
    null != clientType.orNull
  }

  def isDnumTypeValid(dnum: String): Boolean = {
    !"".equals(dnum) && !"null".equalsIgnoreCase(dnum) && !"000".equals(dnum) && dnum.matches("""^\d+$""")
  }

  def getIp(ipAddress: String): String = {
    val ipPattern = """(\d{1,3}\.){3}\d{1,3}""".r
    val ip = ipPattern.findAllIn(ipAddress).toList
    if (0 == ip.length) null else ip.head
  }

  //根据ip地址获取地址
  def getAddressByIp(ip: String, res: IPLoadResult): Seq[String] = {
    if (ip != null && !"".equals(ip)) {
      IPAnalysisUtil.find(ip, res).toSeq
    } else {
      Seq[String]()
    }
  }

  def saveDFToMySQL(df: DataFrame, tableName: String): Unit = {
    val resultWriter = df.write.mode("append")
    val jdbcURL = "jdbc:mysql://192.168.10.40:3306/datacenter"
    val userName = "datacenter"
    val passWord = "Bds6NGda3lVaBNps"
    val prop = new Properties()
    prop.put("user", userName)
    prop.put("password", passWord)
    resultWriter.jdbc(jdbcURL, tableName, prop)
  }

  /**
    * 获取指定日期往后回溯n天(包括该天)的日志路径(路径中日期格式:yyyy/MM/dd)
    *
    * @param date       指定日期
    * @param range      日期范围{1->day,7->week,30->month,other->other day}
    * @param pathPrefix 路径前缀
    * @param pathSuffix 路径后缀
    * @return (范围标志,范围路径)
    */
  def getDayRangeInputFilePath(date: String, range: Int, pathPrefix: String, pathSuffix: String,
                               dateFormat: String = "yyyy/MM/dd"): (String, String) = {
    val dateFlag = range match {
      case 1 => "day"
      case 7 => "week"
      case 30 => "month"
      case other => other + "day"
    }

    val sdf = new SimpleDateFormat("yyyy-MM-dd") //recordDate的日期格式
    val pathDateFormat = new SimpleDateFormat(dateFormat) //路径中的日期格式
    val arrayBuffer = ArrayBuffer[String]()
    val calendar = Calendar.getInstance()

    for (i <- 0 until range) {
      calendar.setTime(sdf.parse(date))
      calendar.add(Calendar.DATE, -i)
      val dateT = pathDateFormat.format(calendar.getTime)
      val fileSystem = FileSystem.get(new Configuration())

      val path = if (pathPrefix.endsWith("/")) pathPrefix + dateT else pathPrefix + "/" + dateT

      if (fileSystem.exists(new Path(path))) {
        arrayBuffer += dateT
      }
    }
    val inputPaths = pathPrefix + "{" + arrayBuffer.toList.mkString(",") + "}" + pathSuffix
    if (arrayBuffer.nonEmpty) (dateFlag, inputPaths) else null
  }

  /**
    * 将RDD以gz方式压缩保存到本地文件系统
    *
    * @param rdd        RDD
    * @param pathS      保存路径
    * @param sc         SparkContext
    * @param num        RDD分区数量
    * @param isMergeOne 是否将文件合并成一个保存
    * @return
    */
  def saveRddToHdfs(rdd: RDD[String], pathS: String, sc: SparkContext, num: Int, isMergeOne: Boolean): AnyVal = {
    val srcPath = new Path(pathS)
    val dstPath = new Path(pathS + ".log.gz")
    val hadoopConf = sc.hadoopConfiguration
    val hdfs = FileSystem.get(hadoopConf)
    if (hdfs.exists(srcPath)) hdfs.delete(srcPath, true)
    rdd.repartition(num).saveAsTextFile(pathS, classOf[GzipCodec])
    //是否将结果保存为一个文件
    if (isMergeOne) {
      val srcFileSystem = FileSystem.get(new URI(pathS), hadoopConf)
      val dstFileSystem = FileSystem.get(new URI(pathS + ".log"), hadoopConf)
      if (dstFileSystem.exists(dstPath)) dstFileSystem.delete(dstPath, true)
      FileUtil.copyMerge(
        srcFileSystem,
        srcPath,
        dstFileSystem,
        dstPath,
        true,
        hadoopConf,
        null)
    }
  }

  /**
    * 获取指定日期的时间,当时间出现跨天情况,截取指定天的时间
    *
    * @param startTime 开始时间
    * @param endTime   结束时间
    * @return
    */
  def getSpecifiedDayTime(calcDate: String, startTime: String, endTime: String): Array[String] = {
    val regexS ="""\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}"""
    if (!startTime.matches(regexS) || !endTime.matches(regexS)) return Array()
    val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val day = calcDate.substring(0, 10) //yyyy-MM-dd
    val dayMinStamp = sdf.parse(day + " 00:00:00").getTime
    val dayMaxStamp = sdf.parse(day + " 23:59:59").getTime

    val startTimeStamp = sdf.parse(startTime).getTime
    val endTimeStamp = sdf.parse(endTime).getTime

    if (startTimeStamp > endTimeStamp) return Array()
    if (endTimeStamp < dayMinStamp) return Array()
    if (startTimeStamp > dayMaxStamp) return Array()

    val startStamp = if (startTimeStamp < dayMinStamp) dayMinStamp else startTimeStamp
    val endStamp = if (endTimeStamp > dayMaxStamp) dayMaxStamp else endTimeStamp
    Array(sdf.format(new Date(startStamp)), sdf.format(new Date(endStamp)))
  }

  /**
    * 将同一天开始,结束时间按小时分割到多个区间
    *
    * @param startTime "yyyy-MM-dd HH:mm:ss"
    * @param endTime   "yyyy-MM-dd HH:mm:ss"
    * @return List(小时,时长)
    */
  def splitTimeToHourSection(startTime: String, endTime: String): List[(Int, Int)] = {
    //获取时间的时,分,秒的数字
    val ss = Array[Char]('-', ':', ' ')
    val startTimeSp = startTime.split(ss)
    val endTimeSp = endTime.split(ss)

    val sHour = startTimeSp(3).toInt
    val eHour = endTimeSp(3).toInt
    val sMinute = startTimeSp(4).toInt
    val eMinute = endTimeSp(4).toInt
    val sSecond = startTimeSp(5).toInt
    val eSecond = endTimeSp(5).toInt

    if (eHour < sHour) {
      Nil
    } else if (sHour == eHour) {
      val uT = (eMinute * 60 + eSecond) - (sMinute * 60 + sSecond)
      List[Int](sHour) zip List[Int](uT)
    } else {
      val sut = 3600 - (sMinute * 60 + sSecond)
      val eut = eMinute * 60 + eSecond

      import Array._
      val uTarray = fill[Int](eHour - sHour - 1)(3600)
      val hList = range(sHour, eHour + 1).toList
      val useList = (sut +: uTarray :+ eut).toList
      hList zip useList
    }
  }

  /**
    * 从MySql数据库中获取DateFrame
    *
    * @param sqlContext     sqlContext
    * @param mysqlTableName 表名
    * @param queryCondition 查询条件(可选)
    * @return DateFrame
    */
  def getDFFromMysql(sqlContext: SQLContext, mysqlTableName: String, queryCondition: String): DataFrame = {
    val (jdbcURL, userName, passWord) = MyUtils.getMySQLInfo
    val prop = new Properties()
    prop.put("user", userName)
    prop.put("password", passWord)

    if (null == queryCondition)
      sqlContext.read.jdbc(jdbcURL, mysqlTableName, prop)
    else
      sqlContext.read.jdbc(jdbcURL, mysqlTableName, prop).where(queryCondition)
  }


  /**
    * 将DataFrame所有类型(除id外)转换为String后,通过c3p0的连接池方法，向mysql写入数据(存储引擎为myisam时，写入效率较低)
    * Mysql数据表除了id(整型,自增)外,所有字段类型需为字符串类型(varchar)
    *
    * @param tableName       表名
    * @param resultDateFrame DataFrame
    */
  def saveDFtoDBUsePoolByCastTypeToString(tableName: String, resultDateFrame: DataFrame) {
    val colNumbers = resultDateFrame.columns.length
    val sql = getInsertSql(tableName, colNumbers)
    resultDateFrame.foreachPartition(partitionRecords => {
      val conn = MysqlManager.getMysqlManager.getConnection //从连接池中获取一个连接
      val preparedStatement = conn.prepareStatement(sql)
      val metaDate = conn.getMetaData.getColumns(null, "%", tableName, "%") //通过连接获取表名对应数据表的元数据
      try {
        conn.setAutoCommit(false)
        partitionRecords.foreach(record => {
          //注意:setString方法从1开始，record.getString()方法从0开始
          for (i <- 1 to colNumbers) {
            val value = record.get(i - 1)
            if (value != null) { //如何值不为空,将类型转换为String
              preparedStatement.setString(i, value.toString)
            } else { //如果值为空,将值设为对应类型的空值
              metaDate.absolute(i)
              preparedStatement.setNull(i, metaDate.getInt("DATA_TYPE"))
            }
          }
          preparedStatement.addBatch()
        })
        preparedStatement.executeBatch()
        conn.commit()
      } catch {
        case e: Exception => println(e.getMessage)
        // do some log
      } finally {
        preparedStatement.close()
        conn.close()
      }
    })
  }

  def saveDFtoDBCreateTableIfNotExist(tableName: String, resultDateFrame: DataFrame) {
    //如果没有表,根据DataFrame建表
    createTableIfNotExist(tableName, resultDateFrame)
    //验证数据表字段和dataFrame字段个数和名称,顺序是否一致
    verifyFieldConsistency(tableName, resultDateFrame)
    //保存df
    saveDFtoDBUsePoolByCastTypeToString(tableName, resultDateFrame)
  }


  /**
    * 如果数据表不存在,根据DataFrame的字段创建数据表,数据表字段顺序和dataFrame对应
    * 若DateFrame出现名为id的字段,将其设为数据库主键(int,自增,主键),其他字段全部设置为字符串类型(varchar)
    *
    * @param tableName 表名
    * @param df        dataFrame
    * @return
    */
  def createTableIfNotExist(tableName: String, df: DataFrame): AnyVal = {
    val con = MysqlManager.getMysqlManager.getConnection
    val metaDate = con.getMetaData
    val colResultSet = metaDate.getColumns(null, "%", tableName, "%")
    //如果没有该表,创建数据表
    if (!colResultSet.next()) {
      //构建建表字符串
      val sb = new StringBuilder(s"CREATE TABLE `$tableName` (")
      df.columns.foreach { x =>
        if (x.equalsIgnoreCase("id")) {
          sb.append(s"`$x` int(255) NOT NULL AUTO_INCREMENT PRIMARY KEY,") //如果是字段名为id,设置主键,整形,自增
        } else {
          sb.append(s"`$x` varchar(100) DEFAULT NULL,")
        }
      }
      sb.append(") ENGINE=InnoDB DEFAULT CHARSET=utf8")
      val sql_createTable = sb.deleteCharAt(sb.lastIndexOf(',')).toString()
      val statement = con.createStatement()
      statement.execute(sql_createTable)
    }
  }

  /**
    * 验证数据表和dataFrame字段个数,名称,顺序是否一致
    *
    * @param tableName 表名
    * @param df        dataFrame
    */
  def verifyFieldConsistency(tableName: String, df: DataFrame): Unit = {
    val con = MysqlManager.getMysqlManager.getConnection
    val metaDate = con.getMetaData
    val colResultSet = metaDate.getColumns(null, "%", tableName, "%")
    colResultSet.last()
    val tableFiledNum = colResultSet.getRow
    val dfFiledNum = df.columns.length
    if (tableFiledNum != dfFiledNum) {
      throw new Exception(s"数据表和DataFrame字段个数不一致!!table--$tableFiledNum but dataFrame--$dfFiledNum")
    }
    for (i <- 1 to tableFiledNum) {
      colResultSet.absolute(i)
      val tableFileName = colResultSet.getString("COLUMN_NAME")
      val dfFiledName = df.columns.apply(i - 1)
      if (!tableFileName.equals(dfFiledName)) {
        throw new Exception(s"数据表和DataFrame字段名不一致!!table--'$tableFileName' but dataFrame--'$dfFiledName'")
      }
    }
    colResultSet.beforeFirst()
  }

  /**
    * 根据数据表名向数据库中插入一条除指定字段外所有字段都为对应类型空值的记录
    *
    * @param tableName     表名
    * @param fieldValueMap 指定不为空的字段(Map(字段->值))
    */
  def insertAllNullButSpecifiedFieldRecord(tableName: String, fieldValueMap: util.Map[String, String]): Unit = {
    val con = MysqlManager.getMysqlManager.getConnection
    val colResultSet = con.getMetaData.getColumns(null, "%", tableName, "%")

    if (!colResultSet.next()) {
      throw new Exception(s"${con.getMetaData.getURL}不存在表名为:$tableName 的数据表!!!")
    }

    colResultSet.last()
    val tableFieldNum = colResultSet.getRow //获取数据表的字段个数
    val sql = getInsertSql(tableName, tableFieldNum)
    val preparedStatement = con.prepareStatement(sql)
    con.setAutoCommit(false) //关闭自动提交

    try {
      for (i <- 1 to tableFieldNum) {
        colResultSet.absolute(i)
        val fieldName = colResultSet.getString("COLUMN_NAME")
        if (fieldValueMap.containsKey(fieldName)) {
          preparedStatement.setString(i, fieldValueMap.get(fieldName))
        } else {
          preparedStatement.setNull(i, colResultSet.getInt("DATA_TYPE"))
        }
      }
      preparedStatement.addBatch()
      preparedStatement.executeBatch()
      con.commit()
    } catch {
      case e: Exception => e.printStackTrace()
    } finally {
      preparedStatement.close()
      con.close()
    }
  }

  def insertNullButSpeFieldRecord(tableName: String, mapList: List[(String, String)]): Unit = {
    val fieldValueMap = new util.HashMap[String, String]()
    for ((fieldName, value) <- mapList) {
      fieldValueMap.put(fieldName, value)
    }
    insertAllNullButSpecifiedFieldRecord(tableName, fieldValueMap)
  }

  /**
    * 判断某天是否为一周的最后一天(星期天)
    * Calendar中以星期六为一周最后一天,星期天为一周第一天
    *
    * @param date 日期(yyyy-MM-dd)
    * @return
    */
  def isDayOfWeekEnd(date: String): Boolean = {
    val format = new SimpleDateFormat("yyyy-MM-dd")
    val calendar = Calendar.getInstance()
    calendar.setTime(format.parse(date))
    calendar.get(Calendar.DAY_OF_WEEK) == calendar.getFirstDayOfWeek
  }

  /**
    * 判断某天是否为一个月的最后一天
    *
    * @param date 日期(yyyy-MM-dd)
    * @return
    */
  def isDayOfMonthEnd(date: String): Boolean = {
    val format = new SimpleDateFormat("yyyy-MM-dd")
    val calendar = Calendar.getInstance()
    calendar.setTime(format.parse(date))
    calendar.get(Calendar.DAY_OF_MONTH) == calendar.getActualMaximum(Calendar.DAY_OF_MONTH)
  }
}