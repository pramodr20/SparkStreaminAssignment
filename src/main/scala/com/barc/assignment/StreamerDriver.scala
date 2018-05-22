package com.barc.assignment


import com.google.common.base.Charsets
import com.typesafe.config.Config
import kafka.common.TopicAndPartition
import kafka.message.MessageAndMetadata
import kafka.serializer.StringDecoder
import org.apache.curator.CuratorConnectionLossException
import org.apache.curator.framework.{CuratorFramework, CuratorFrameworkFactory}
import org.apache.curator.retry.ExponentialBackoffRetry
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka.{HasOffsetRanges, KafkaUtils}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.HashMap
import scala.util.Try

/**
  * Created by Pramod Raju on 5/22/18.
  */





object StreamerDriver {



  def main(args: Array[String]): Unit = {


    val config = com.typesafe.config.ConfigFactory.load()
    val batchInterval =  config.getInt("com.barc.assignment.batchInterval")
    val zkpath = config.getString("com.barc.assignment.zkpath")
    val zookeeperhosts = config.getString("com.barc.assignment.zookeeperhosts")

    val conf = new SparkConf().setAppName("Kafka Streaming")
    .setMaster("local")
    val sc = new SparkContext(conf)
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    val streamcontext = new StreamingContext(sc, Seconds(batchInterval.toLong))
    val retryPolicy = new ExponentialBackoffRetry(1000, 3);


    val curatorClient = CuratorFrameworkFactory.newClient(zookeeperhosts, retryPolicy)  // Using Curator for connecting to Zookeeper
    curatorClient.start()

    val kafkaTopicSet = Set[String](config.getString("com.barc.assignment.kafkatopic"))
    val directStream = createCustomDirectKafkaStream(streamcontext, buildKafkaConfig(config).toMap, curatorClient, zkpath, kafkaTopicSet)



    directStream.foreachRDD(rdd => {

      startStreamProcess(rdd.map(x=>x._2), sqlContext)
      saveOffsets(curatorClient, zkpath, rdd)

    }
    )



    streamcontext.start()
    streamcontext.awaitTermination()


  }



  def buildKafkaConfig(config: Config):HashMap[String, String] = {


    val kafkaBroker =  config.getString("com.barc.assignment.broker")
    val zookeeperhosts = config.getString("com.barc.assignment.zookeeperhosts")
    val serialiserclass = config.getString("com.barc.assignment.kafkaserializer")


    var kafkaconfig = new HashMap[String, String]()
    kafkaconfig += ("zookeeper.connect" -> zookeeperhosts);
    kafkaconfig += ("bootstrap.servers" -> kafkaBroker)
    kafkaconfig += ("serializer.class" -> serialiserclass)
    kafkaconfig += ("group.id" -> "default")

    kafkaconfig
  }


  def createCustomDirectKafkaStream(ssc: StreamingContext, kafkaParams: Map[String, String], curatorClient: CuratorFramework, zkPath: String, topics: Set[String]): InputDStream[(String,String)] = {

    val topic = topics.last
    val storedOffsets = readOffsets(curatorClient, zkPath, topic)
    val kafkaStream = storedOffsets match {
      case None => // start from the latest offsets
        KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topics)
      case Some(fromOffsets) => // start from previously saved offsets
        val messageHandler = (mmd: MessageAndMetadata[String, String]) => (mmd.key, mmd.message)
        KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder
          , (String, String)](ssc, kafkaParams, fromOffsets, messageHandler)
    }

    kafkaStream
  }




  def readOffsets(curatorClient: CuratorFramework, zkPath: String, topic: String):
  Option[Map[TopicAndPartition, Long]] = {

   println("Reading offsets from Zookeeper")

    val pathCheck = Try(curatorClient.getData.forPath(zkPath))

    val offsetsRangesStrOpt = Option(if (pathCheck.isSuccess) {

      val map = new String(pathCheck.get, Charsets.UTF_8);
      map
    } else

      null)

    offsetsRangesStrOpt match {
      case Some(offsetsRangesStr) =>
       println(s"Read offset ranges: ${offsetsRangesStr}")
        val offsets = offsetsRangesStr.split(",")
          .map(s => s.split(":"))
          .map { case Array(partitionStr, offsetStr) => (TopicAndPartition(topic, partitionStr.toInt) -> (offsetStr.toLong)) }
          .toMap
        Some(offsets)
      case None =>
        None
    }
  }



  def saveOffsets(curatorClient: CuratorFramework, zkPath: String, rdd: RDD[_]): Unit = {
   println("Saving offsets to Zookeeper")

    val offsetsRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
    offsetsRanges.foreach(offsetRange => println(s"Using ${offsetRange}"))
    val offsetsRangesStr = offsetsRanges.map(offsetRange => s"${offsetRange.partition}:${offsetRange.fromOffset}")
      .mkString(",")
   println(" Writing offsets to Zookeeper zkPath=" + zkPath + "  offsetsRangesStr:" + offsetsRangesStr)

    Try(curatorClient.delete().forPath(zkPath))

    val createTry = Try(curatorClient.create().creatingParentsIfNeeded().forPath(zkPath, offsetsRangesStr.getBytes()))


    if (createTry.isFailure) {
      throw new CuratorConnectionLossException
    }


  }


  def startStreamProcess(rdd: RDD[String],sqlContext: SQLContext) = {


    if(!rdd.isEmpty()) {
      val emp = sqlContext.read.json(rdd)
      emp.registerTempTable("Employee")
      sqlContext.sql("select age,name from Employee ").filter(emp("age") === 30).show()
    }


  }




}
