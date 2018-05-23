# SparkStreaminAssignment

The Driver to the Spark Streaming Project is StreamerDriver under the package com.barc.assignment
The Configurations are set in the application.conf file located in the resources directory .
The default configurations for the Kafka and Zookeeper are in the Configurations file and are as follows .
  
  
  SparkStreaming Batch Interval - batchInterval = "10"
  Kafka Broker - broker = "localhost:9092"
  Zookeeper Path to save the Offsets - zkpath = "/tmp/zoookeepercur"
  Zookeeper Host - zookeeperhosts = "localhost:2182"
  Kafka topic to read the data from - kafkatopic = "test"
  Default serializer - kafkaserializer = "kafka.serializer.DefaultEncoder"
  Hive Table Name - hivetablename = "employee"
  Hive external path - hiveexternalpath = "/tmp/testemployee"
  
  
  Sample input to the kafka topic 
  
  { "name":"John", "age":30 }
 
