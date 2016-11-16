package com.kexion.wireless

import java.text.SimpleDateFormat

import com.stratio.datasource.mongodb._
import com.stratio.datasource.mongodb.config.MongodbConfig._
import com.stratio.datasource.mongodb.config._
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}


/**
  * Created by Administrator on 2016/7/14.
  */

trait MongoConecner {
  val hostPort = "192.168.10.151:30000"
  val dataBase = "hbwifi"
  val tableName = "WA_SOURCE_FJ_0001"
  val mongoProvider = "com.stratio.datasource.mongodb"
}

sealed abstract class UserQuery

case class QueryA(userMac: String, startTime: String, endTime: String) extends UserQuery

case class QueryB(authAccount: String, startTime: String, endTime: String) extends UserQuery

case class QueryC(userMac: String) extends UserQuery

case class QueryD(authAccount: String) extends UserQuery

case class QueryE(startTime: String, endTime: String) extends UserQuery

object CollisionAnalysis extends UserQuery with MongoConecner {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("CollisionAnalysis").setMaster("local")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    val userVisit = new CollisionAnalysis("192.168.10.151:30000", "hbwifi", sqlContext, "WA_SOURCE_FJ_0001")
    val users = userVisit.getUserList(new QueryE("1467781266","1468398424"))
    //    val users = userVisit.getUserList(new QueryD(""))
    for (i <- users) {
      i.show
//      userVisit.saveDF(i)
      println(i.count)
    }

    /*for (i <- userVisit.getApUserList()) {
      i.show()
      userVisit.saveDF(i)
    }*/
  }
}

class CollisionAnalysis(override val hostPort: String, override val dataBase: String, val sqlContext: SQLContext, override val tableName: String) extends UserQuery with MongoConecner {
  val builder = MongodbConfigBuilder(Map(Host -> List(hostPort), Database -> dataBase, Collection -> tableName, SamplingRatio -> 1.0, WriteConcern -> "normal")).build
  val mongoRDD = sqlContext.fromMongoDB(builder)
  mongoRDD.registerTempTable("UserCollisionList")
  val sql = "SELECT AP_MAC,MAC,AUTH_ACCOUNT,START_TIME,END_TIME FROM UserCollisionList "
  val df = sqlContext.sql(sql)
  val apMacList = df.select(df("AP_MAC")).toJSON.distinct.map(ap => ap.substring(11, ap.length - 2)).collect
  val apDF = collection.mutable.Set.empty[DataFrame]

  for (tag <- apMacList) {
    apDF += df.filter(df("AP_MAC") === tag).sort(df("START_TIME").desc)
  }
  val userList = collection.mutable.Set.empty[DataFrame]

  def getApUserList(): List[DataFrame] = {
    val apDFList = apDF.toList
    for (i <- 0 until apDFList.length - 1) {
      for (j <- i + 1 until apDFList.length) {
        val apUserA = apDFList(i).withColumnRenamed("AP_MAC", "AP_MAC_L").withColumnRenamed("MAC", "MAC_L").withColumnRenamed("START_TIME", "START_TIME_L").withColumnRenamed("END_TIME", "END_TIME_L")
        val apUserB = apDFList(j)
        val ap = apUserA.join(apUserB, Seq("AUTH_ACCOUNT"))
        userList += ap
      }
    }
    userList.toList
  }

  def getUserList(queryUser: UserQuery): List[DataFrame] = (queryUser: @unchecked) match {
    case QueryA(userMac, startTime, endTime) => {
      val apDFList = apDF.toList
      for (i <- 0 until apDFList.length - 1) {
        for (j <- i + 1 until apDFList.length) {
          val apUserA = apDFList(i).filter(apDFList(i)("START_TIME") >= startTime.toInt).filter(apDFList(i)("START_TIME") <= endTime.toInt).filter(apDFList(i)("MAC") === userMac)
          val apUserB = apDFList(j).filter(apDFList(j)("START_TIME") >= startTime.toInt).filter(apDFList(j)("START_TIME") <= endTime.toInt).filter(apDFList(j)("MAC") === userMac)
          val setUser = apUserA.join(apUserB, Seq("MAC"))
          userList += setUser
        }
      }
      userList.toList
    }
    case QueryB(authAccount, startTime, endTime) => {
      val apDFList = apDF.toList
      for (i <- 0 until apDFList.length - 1) {
        for (j <- i + 1 until apDFList.length) {
          val apUserA = apDFList(i).filter(apDFList(i)("START_TIME") >= startTime.toInt).filter(apDFList(i)("START_TIME") <= endTime.toInt).filter(apDFList(i)("AUTH_ACCOUNT") === authAccount)
          val apUserB = apDFList(j).filter(apDFList(j)("START_TIME") >= startTime.toInt).filter(apDFList(j)("START_TIME") <= endTime.toInt).filter(apDFList(j)("AUTH_ACCOUNT") === authAccount)
          val setUser = apUserA.join(apUserB, Seq("AUTH_ACCOUNT"))
          userList += setUser
        }
      }
      userList.toList
    }
    case QueryC(userMac) => {
      val apDFList = apDF.toList
      for (i <- 0 until apDFList.length - 1) {
        for (j <- i + 1 until apDFList.length) {
          val apUserA = apDFList(i).filter(apDFList(i)("MAC") === userMac)
          val apUserB = apDFList(j).filter(apDFList(j)("MAC") === userMac)
          val setUser = apUserA.join(apUserB)
          userList += setUser
        }
      }
      userList.toList
    }
    case QueryD(authAccount) => {
      val apDFList = apDF.toList
      for (i <- 0 until apDFList.length - 1) {
        for (j <- i + 1 until apDFList.length) {
          val apUserA = apDFList(i).filter(apDFList(i)("AUTH_ACCOUNT") === authAccount)
          val apUserB = apDFList(j).filter(apDFList(j)("AUTH_ACCOUNT") === authAccount)
          val setUser = apUserA.join(apUserB, Seq("AUTH_ACCOUNT"))
          userList += setUser
        }
      }
      userList.toList
    }
    case QueryE(startTime, endTime) => {
      val apDFList = apDF.toList
      for (i <- 0 until apDFList.length - 1) {
        for (j <- i + 1 until apDFList.length) {
          val apUserA = apDFList(i).filter(apDFList(i)("START_TIME") >= startTime.toInt).filter(apDFList(i)("START_TIME") <= endTime.toInt)
          val apUserB = apDFList(j).filter(apDFList(i)("START_TIME") >= startTime.toInt).filter(apDFList(i)("START_TIME") <= endTime.toInt)
          val setUser = apUserA.join(apUserB, Seq("AUTH_ACCOUNT"))
          userList += setUser
        }
      }
      userList.toList
    }
  }

  def strToDate(str: String): Long = {
    val date = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse(str).getTime
    date / 1000
  }

  def saveDF(dataFrame: DataFrame): Unit = {
    val saveConfig = MongodbConfigBuilder(Map(Host -> List(hostPort), Database -> dataBase, Collection -> "CollisionData", SamplingRatio -> 1.0, WriteConcern -> "normal", SplitSize -> 10, SplitKey -> "_id"))
    dataFrame.saveToMongodb(saveConfig.build)
  }
}
