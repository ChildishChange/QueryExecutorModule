package de.tf.uni.freiburg.sparkrdf.run;

import de.tf.uni.freiburg.sparkrdf.constants.Const;
import de.tf.uni.freiburg.sparkrdf.model.rdf.executionresults.IntermediateResultsModel;
import de.tf.uni.freiburg.sparkrdf.parser.query.AlgebraTranslator;
import de.tf.uni.freiburg.sparkrdf.parser.query.AlgebraWalker;
import de.tf.uni.freiburg.sparkrdf.parser.query.op.SparkOp;
import de.tf.uni.freiburg.sparkrdf.sparql.SparkFacade;
import de.tf.uni.freiburg.sparkrdf.sparql.operator.result.util.SolutionMapping;

import org.apache.jena.query.ARQ;
import org.apache.jena.query.Query;
import org.apache.jena.query.QueryFactory;
import org.apache.jena.shared.PrefixMapping;
import org.apache.jena.sparql.algebra.Algebra;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.system.JenaSystem;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.rdd.RDD;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.Map;
import java.util.Queue;
import java.util.TreeMap;

/**
 * Class to start the computation of SPARQL queries with Spark and GraphX
 *
 * @author Thorsten Berberich
 */
public class QueryExecutor {

    /**
     * QueryExecutor class to start Spark and GraphX
     */
    public static void main(String[] args) {
        ArgumentParser.parseInput(args);

        Logger log = Logger.getLogger(QueryExecutor.class);
        Map<String, Long> operationDuration = new TreeMap<>();
        long resCount = 0;

        JenaSystem.init();
        ARQ.init();
        SparkFacade.createSparkContext();

        // Load the graph
        log.log(Level.INFO, "Started Graph loading");
        long startLoading = System.currentTimeMillis();
        SparkFacade.loadGraph();
        long endLoading = System.currentTimeMillis() - startLoading;

        log.log(Level.INFO, "Finished Graph Loading in " + endLoading + " ms");
        operationDuration.put("GraphLoading", endLoading);

        if (Const.query() != null) {
        /*
	     * Get all queries that should be executed on this graph
	     */
            String[] queries = Const.query().split(",");
            int queryCount = queries.length;

            for (int itr = 0; itr < queryCount; itr++) {
                String queryFile = queries[itr];
                log.log(Level.INFO, "Started query file: " + queryFile);
                IntermediateResultsModel.getInstance().clearResults();

                // Parse the query
                Query query = QueryFactory.read("file:" + queryFile.trim());
                Const.parsedQuery_$eq(query.toString());

                PrefixMapping prefixes = query.getPrefixMapping();

                Op opRoot = Algebra.compile(query);
                AlgebraTranslator trans = new AlgebraTranslator(prefixes);
                opRoot.visit(new AlgebraWalker(trans));

                // Queue with all operators
                Queue<SparkOp> q = trans.getExecutionQueue();

                // Execute all operators from the queue
                while (!q.isEmpty()) {
                    SparkOp actual = q.poll();
                    String tag = actual.getTag();
                    log.log(Level.INFO, "Started " + tag);
                    long start = System.currentTimeMillis();
                    actual.execute();
                    long finished = System.currentTimeMillis() - start;
                    putTimeToMap(operationDuration, finished, tag);
                    log.log(Level.INFO, "Finished " + tag + " in " + finished
                                        + " ms");
                }

		/*
		 * Save the output to HDFS
		 */
                if (Const.outputFilePath() != null) {
                    SparkFacade.saveResultToFile(IntermediateResultsModel
                                                     .getInstance().getFinalResult());
                } else {
		    /*
		     * Count the result which are then saved into the map with
		     * the timings
		     */
                    RDD<SolutionMapping> res = IntermediateResultsModel
                        .getInstance().getFinalResult();
                    if (res != null) {
                        resCount = res.count();
                    } else {
                        resCount = 0;
                    }
                    log.log(Level.INFO, "Result count " + resCount);
                }

		/*
		 * Print the output to the console
		 */
                if (Const.printToConsole()) {
                    SparkFacade.printRDD(IntermediateResultsModel.getInstance()
                                             .getFinalResult());
                }

                // Write the durations and the result count to the given file
                if (Const.timeFilePath() != null) {
                    OutputStreamWriter writer;
                    try {
                        File f = new File(Const.timeFilePath());
                        Boolean exists = f.exists();

                        writer = new OutputStreamWriter(new FileOutputStream(f,
                                                                             true), "UTF-8");
                        BufferedWriter fbw = new BufferedWriter(writer);

                        if (!exists) {
                            f.mkdirs();
                            f.createNewFile();
                            fbw.write("Query File;");
                            for (String tag : operationDuration.keySet()) {
                                fbw.write(tag + ";");
                            }
                            fbw.write("Overall Duration;");
                            fbw.write("Result Count");
                            fbw.newLine();
                        }

                        long sum = 0;
                        fbw.write(queryFile + ";");
                        for (String tag : operationDuration.keySet()) {
                            fbw.write(operationDuration.get(tag) + ";");
                            sum += operationDuration.get(tag);
                        }
                        // Duration of all parts
                        fbw.write(String.valueOf(sum) + ";");
                        fbw.write(String.valueOf(resCount));

                        fbw.newLine();

                        if (itr + 1 == queryCount) {
                            fbw.newLine();
                        }

                        fbw.close();

                        // Clear the map for the next iteration
                        operationDuration.clear();
                        operationDuration.put("GraphLoading", 0l);
                    } catch (IOException e) {
                        log.log(Level.ERROR, "Couldn't write execution times",
                                e);
                    }
                }
            }
        }
        SparkFacade.closeContext();
    }

    /**
     * Put the duration of the operation into the given map. Times will be summed up.
     *
     * @param map  Map to put the durations
     * @param time Time needed for the operation
     * @param tag  Tag of the operation
     */
    private static void putTimeToMap(Map<String, Long> map, Long time,
                                     String tag) {
        if (map.get(tag) != null) {
            long newTime = map.get(tag) + time;
            map.put(tag, newTime);
        } else {
            map.put(tag, time);
        }
    }
}
