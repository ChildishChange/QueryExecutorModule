//package org.task.sparql.executor
//
//import de.tf.uni.freiburg.sparkrdf.constants.Const
//import de.tf.uni.freiburg.sparkrdf.parser.query.AlgebraTranslator
//import de.tf.uni.freiburg.sparkrdf.parser.query.AlgebraWalker
//import de.tf.uni.freiburg.sparkrdf.run.ArgumentParser
//import de.tf.uni.freiburg.sparkrdf.sparql.SparkFacade
//import org.apache.jena.query.ARQ
//import org.apache.jena.query.Query
//import org.apache.jena.query.QueryFactory
//import org.apache.jena.shared.PrefixMapping
//import org.apache.jena.sparql.algebra.{Algebra, AlgebraGenerator, Op}
//import org.apache.jena.system.JenaSystem
//import org.apache.log4j.Level
//import org.apache.log4j.Logger
//import java.io.BufferedWriter
//import java.io.File
//import java.io.FileOutputStream
//import java.io.OutputStreamWriter
//import java.io.{InputStream, OutputStream}
//import java.net.InetSocketAddress
//
//import play.api.libs.json._
//import com.sun.net.httpserver.HttpHandler
//import com.sun.net.httpserver.{HttpExchange, HttpHandler, HttpServer}
//import de.tf.uni.freiburg.sparkrdf.model.rdf.executionresults.IntermediateResultsModel
//import de.tf.uni.freiburg.sparkrdf.sparql.operator.result.util.SolutionMapping
//import org.apache.commons.io.IOUtils
//import org.apache.spark.rdd.RDD
//import org.task.sparql.cache.CachePool
//import org.task.sparql.rewriter.OptRewriter
//
//import scala.collection.mutable
//import scala.collection.mutable._
//
///**
//  * Execute computing on spark.
//  * @author HE, Tao
//  */
//object QueryExecutor {
//
//	val logger: Logger = Logger.getLogger(QueryExecutor.this.getClass);
//
//    /**
//     * QueryExecutor class to start Spark and do query
//     *
//     * @param args
//     */
//	def main(args: Array[String]): Unit = {
//		ArgumentParser.parseInput(args)
//
//		// initialize jena and spark context
//		ARQ.init()
//		JenaSystem.init()
//		val context = SparkFacade.createSparkContext()
//
//		// load graph into context
//		logger.log(Level.INFO, "Started Graph loading")
//		val startLoading = System.currentTimeMillis()
//		DataLoader.loadGraph(Const.rootDir, Const.inputFile, context)
//		val stopLoading = System.currentTimeMillis()
//		logger.log(Level.INFO, "Finished Graph Loading in " + (stopLoading - startLoading) + " ms")
//
//		if (Const.serverMode) {
//			runServerMode()
//		} else {
//			runLocalMode()
//		}
//
//		SparkFacade.closeContext()
//	}
//
//	def runTheQuery(name: String, query: Query, optimize: Boolean): (Long, String, Long, String) = {
//
//		val prefixes: PrefixMapping = query.getPrefixMapping
//
//		ARQ.enableOptimizer(ARQ.getContext, optimize)
//	//	AlgebraGenerator.applySimplification = true
//
//		val opRoot: Op = if (optimize) {
//			// do optimization
//			OptRewriter.optimize(Algebra.compile(query))
//		} else {
//			Algebra.compile(query)
//		}
//		val translator = new AlgebraTranslator(prefixes)
//		opRoot.visit(new AlgebraWalker(translator))
//
//		// execute all operator
//		def executeFineGain(): Long = {
//			val queue = translator.getExecutionQueue
//			val startTime = System.currentTimeMillis()
//			while (!queue.isEmpty) {  // The execution queue is mutable, don't use foreach
//				val toExecute = queue.poll()
//				logger.log(Level.INFO, "start execute " + toExecute.getTag)
//				val startQuery = System.currentTimeMillis()
//
//				if (Const.cacheEnable && !Const.cacheEnableLegacy) {
//					toExecute.executeCached()
//				} else {
//					toExecute.execute()
//				}
//				val stopQuery = System.currentTimeMillis() - startQuery
//				logger.log(Level.INFO, "Finish " + toExecute.getTag + " in " + stopQuery + "ms")
//			}
//			val finishTime = System.currentTimeMillis()
//			logger.log(Level.INFO, "Finish execute query " + name + " within time " + (finishTime - startTime) + " ms")
//			(finishTime - startTime)
//		}
//
//		var tdiff = 0L
//
//		logger.log(Level.INFO, "Started query " + name)
//		if (Const.cacheEnableLegacy) {
//			if (CachePool.contains(opRoot)) {
//				CachePool.getFinalResult(opRoot)
//			} else {
//				tdiff = executeFineGain()
//			}
//		} else {
//			tdiff = executeFineGain()
//		}
//
//		// save result
//		var resultCount: Long = 0L
//		var res: RDD[SolutionMapping] = null
//
//		if (Const.cacheEnable || (Const.cacheEnableLegacy && CachePool.contains(opRoot))) {
//			res = CachePool.getFinalResult(opRoot)
//		} else {
//			res = IntermediateResultsModel.getInstance().getFinalResult
//			IntermediateResultsModel.getInstance().clearResults()
//		}
//
//		// count result
//		if (res != null) {
//			resultCount = res.count()
//		}
//		logger.log(Level.INFO, "Result count: " + resultCount)
//
//		// save result to file
//		if (Const.outputFilePath != null) {
//			SparkFacade.saveResultToFile(res)
//		}
//
//		// console output
//		if (Const.printToConsole) {
//			SparkFacade.printRDD(res)
//		}
//
//		val resString = if (res == null) {
//			""
//		} else {
//			val result: mutable.MutableList[String] = mutable.MutableList()
//			res.collect().foreach(x => {
//				result += x.toString
//			})
//			result.toList.toString()
//		}
//		(tdiff, opRoot.toString, resultCount, resString)
//	}
//
//	def runLocalMode(): Unit = {
//		// execute query one-by-one
//		Const.query.split(",").foreach(path => {
//			runTheQuery(path, QueryFactory.read("file:" + path), Const.optimize)
//		})
//	}
//
//	def runServerMode(): Unit = {
//		val server = HttpServer.create(new InetSocketAddress(8000), 0)
//		server.createContext("/api", new RootHandler())
//		server.setExecutor(null)
//		server.start()
//
//		println("Hit any key to exit...")
//		System.in.read()
//		server.stop(0)
//	}
//
//	class RootHandler extends HttpHandler {
//
//		def handle(httpExchange: HttpExchange) {
//			val tree = Json.parse(displayPayload(httpExchange.getRequestBody))
//			val name = (tree \ "name").as[String]
//			val query = (tree \ "query").as[String]
//
//			Const.cacheEnable = false
//			val r1 = runTheQuery(name, QueryFactory.create(query), false)
//
//			Const.cacheEnable = true
//			val r2 = runTheQuery(name, QueryFactory.create(query), true)
//
//			val r = Json.obj(
//				"tdiff1" -> r1._1,
//				"parsed1" -> r1._2,
//				"size1" -> r1._3,
//				"resultset1" -> r1._4,
//				"tdiff2" -> r2._1,
//				"parsed2" -> r2._2,
//				"size2" -> r2._3,
//				"resultset2" -> r2._4
//			).toString()
//
//			sendResponse(r, httpExchange)
//		}
//
//		private def displayPayload(body: InputStream): String ={
//			IOUtils.toString(body)
//		}
//
//		private def sendResponse(response: String, t: HttpExchange) {
//			t.sendResponseHeaders(200, response.length())
//			val os = t.getResponseBody
//			os.write(response.getBytes)
//			os.close()
//		}
//	}
//}
