# DBpedia recommendation engine

### Setting up Apache Spark cluster on EC2
Setup an Apache Spark cluster using following instructions,
http://spark.apache.org/docs/latest/ec2-scripts.html

### Initializing Dev environment
* Ensure that the JDK (Java Development Kit) is installed.
* Download and install SBT (Simple Build Tool)  
http://www.scala-sbt.org/download.html
* Execute [init-dataset] script to download & install necessary DBpedia datasets for testing purpose
* Clone [dbpedia-ml] project from github & build it using "sbt clean package" command

### Preprocessing the DBpedia dataset

* Log into Spark shell as follows,  
  spark-shell --jars target/scala-2.10/dbpedia-listnet_2.10-1.0.jar
* Import the dbpedia-preprocessing script using following command,  
  :load src/main/scala/preprocessing.txt
* Execute following commands to save the preprocessed data to HDFS  
  * vertexRDD.saveAsObjectFile(vertices_dir_path)  
  * pageLinks.map({case (a,b,c) => s"$a $b $c"}).saveAsTextFile(edges_dir_path)
  * features.saveAsObjectFile(features_dir_path)  

### Training the model

### Ranking

[dbpedia-ml]: https://github.com/kulkarnism/CS298-Project
[init-dataset]: https://github.com/kulkarnism/CS298-Project/blob/master/init-dataset