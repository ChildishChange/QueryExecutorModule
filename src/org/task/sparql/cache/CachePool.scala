package org.task.sparql.cache

import de.tf.uni.freiburg.sparkrdf.model.rdf.executionresults.RDDVariablePair
import de.tf.uni.freiburg.sparkrdf.sparql.operator.result.util.SolutionMapping
import org.apache.spark.rdd.RDD
import org.apache.jena.sparql.algebra.Op
import org.task.sparql.rewriter.OptRewriter
import java.util

import scala.collection.mutable
import scala.collection.mutable.Map

/**
  * This is a modified version of de.tf.uni.freiburg.sparkrdf.model.rdf.executionresults.IntermediateResultsModel.
  *
  * The hashCode was replaced by op's serialize result as the key of result mapping
  * for better cache optimization.
  *
  * TODO more accurate and efficient cache control strategy should be implemented and tested.
  */
object CachePool {

    var totalSize: Long = 0

    var SIZE_LIMIT: Long = 0

    val pool: LRUCache[String, CacheEntry] = new LRUCache(32)

    def putResult(op: Op, result: RDD[SolutionMapping], vars: util.Set[String]) {

        if (this.totalSize > SIZE_LIMIT) {
            // TODO
        }
        if (result != null) {
            this.totalSize += result.count()
        }
        pool.put(OptRewriter.serialize(op), new CacheEntry(new RDDVariablePair(result, vars), false))
    }

    def contains(op: Op): Boolean = {
        pool.containsKey(OptRewriter.serialize(op))
    }

    def markAsActive(op: Op): Boolean = {
        val key: String = OptRewriter.serialize(op)
        if (pool.containsKey(key)) {
            pool.get(key).rddpair.getRdd
            println("hit the cache...")
            true
        } else {
            false
        }
    }

    def getResultPair(op: Op): RDDVariablePair = {
        pool.get(OptRewriter.serialize(op)).rddpair
    }

    def getResultRDD(op: Op): RDD[SolutionMapping] = {
        pool.get(OptRewriter.serialize(op)).rddpair.getRdd
    }

    def getResultVariables(op: Op): util.Set[String] = {
        pool.get(OptRewriter.serialize(op)).rddpair.getVariables
    }

    def removeResult(op: Op): Unit = {
        // TODO needs refactor and fine-tune.
        val key: String = OptRewriter.serialize(op)
        val r = pool.get(key).rddpair.getRdd
        if (r != null) {
            this.totalSize -= r.count()
        }
        pool.remove(key)
    }

    def clearResults(): Unit = {
        pool.clear()
    }

    def getFinalResult(op: Op): RDD[SolutionMapping] = {
        getFinalResult(OptRewriter.serialize(op))
    }

    def getFinalResult(key: String): RDD[SolutionMapping] = {
        if (!pool.containsKey(key)) {
            throw new RuntimeException(
                "Final result not found for query:\n" + key)
        }
        pool.get(key).rddpair.getRdd
    }
}

class CacheEntry(val rddpair: RDDVariablePair, var removed: Boolean) {
    def markAsDelete(): Unit = {
        this.removed = true
    }

    def markAsActive(): Unit = {
        this.removed = false
    }

    def getSize(): Long = {
        this.rddpair.getRdd.count()
    }
}
