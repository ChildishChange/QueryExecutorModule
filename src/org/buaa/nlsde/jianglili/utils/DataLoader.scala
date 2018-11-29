package org.buaa.nlsde.jianglili.utils

import java.io.FileNotFoundException

import de.tf.uni.freiburg.sparkrdf.model.graph.GraphLoader
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.log4j._
import de.tf.uni.freiburg.sparkrdf.constants.Const
import org.apache.spark.SparkContext

object DataLoader {

    val logger: Logger = Logger.getLogger(DataLoader.this.getClass);

    /**
     * Loads the graph into the spark context.
     */
//    def loadGraph(rootdir: String, filename: String, context: SparkContext): Unit = {
//        val filepath = rootdir + "/" + filename;
//        return loadGraph(filepath, context)
//    }

    def loadMyGraph(filepath: String, context: SparkContext): Unit = {
        val conf: Configuration = new Configuration;
        val fs: FileSystem = FileSystem.get(conf);
        println("Input file: " + filepath + ", " + fs.exists(new Path(filepath)));
        if (fs.exists(new Path(filepath))) {
            if (Const.countBasedLoading) {
                GraphLoader.loadGraphCountBased(filepath, context)
            } else {
                GraphLoader.loadGraphHashBased(filepath, context)
            }
        } else {
            GraphLoader.loadGraphHashBased(filepath, context)
            // throw new FileNotFoundException("Input file " + filepath + " doesn't exist")
        }
    }
}
