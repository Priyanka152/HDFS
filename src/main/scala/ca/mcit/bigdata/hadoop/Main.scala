package ca.mcit.bigdata.hadoop

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, FileUtil, Path}

import scala.io.Source

object Main extends App {

    var conf = new Configuration()

    conf.addResource(new  Path("/home/bd-user/opt/hadoop-2.7.3/etc/cloudera/core-site.xml"))
    conf.addResource(new  Path("/home/bd-user/opt/hadoop-2.7.3/etc/cloudera/hdfs-site.xml"))

    var fs: FileSystem = FileSystem.get(conf)
    /*print URI*/
    println(fs.getUri)

    /*print  current working directory*/
    println(fs.getWorkingDirectory)

    val path = new Path("/user/fall2019/priyanka")
    /*2. List the content of /user/2019*/
        fs.listStatus(new Path("/user/fall2019/"))
          .foreach(println)
       /*4.a check folder is exists or not */
     if(fs.exists(path)) {
          println("i found my folder")
        }
     else {
         println("i failed in the previous practice")
        }
    /* Delete Folder */
      if (fs.exists(path)){
        println("deleting file : " + path)
        fs.delete(path,true)
      }
      else {
        println("File/Directory" + path + " does not exist")

      }
      /*create Folder priyanka */
        fs.mkdirs(new Path("/user/fall2019/priyanka"))
          println("sucssesfully created folder priyanka" )

      /*list folder */
      fs.listStatus(new Path("/user/fall2019/priyanka"))
        .foreach(println)
      /* subfolder STM */
      fs.mkdirs(new Path("/user/fall2019/priyanka/STM"))
      println(" created subfolder STM")

     /*copy Stops.txt */
      val srcPath = new Path("/home/bd-user/Downloads/stops.txt")
      val destPath = new Path("/user/fall2019/priyanka/STM")
      fs.copyFromLocalFile(srcPath, destPath)
      println("stops.txt file uploaded")

      /* make copy  stop2.txt */
     val src= new Path("/user/fall2019/priyanka/STM/stops.txt")
     val dest= new Path("/user/fall2019/priyanka/STM/stops2.txt")
     FileUtil.copy(src.getFileSystem(conf),src,dest.getFileSystem(conf),dest,false,conf)
      println("sucessfully created copy of stops.txt")

      /* rename Stop2.txt to stop.csv*/
     fs.rename(dest, new Path("/user/fall2019/priyanka/STM/stops.csv"))
       println("file rename to stops.csv")
      /*print 5 lines*/
     val lines: Unit = Source.fromInputStream( fs.open( new Path( "/user/fall2019/priyanka/STM/stops.csv") ) ).getLines().slice(0,6).foreach( println )
}