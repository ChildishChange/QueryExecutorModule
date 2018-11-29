package de.tf.uni.freiburg.sparkrdf.parser.query.op;

import de.tf.uni.freiburg.sparkrdf.model.rdf.executionresults.IntermediateResultsModel;
import de.tf.uni.freiburg.sparkrdf.model.rdf.executionresults.RDDVariablePair;
import de.tf.uni.freiburg.sparkrdf.sparql.*;
import de.tf.uni.freiburg.sparkrdf.sparql.operator.result.util.SolutionMapping;

import org.apache.jena.sparql.algebra.op.OpDistinct;
import org.apache.spark.rdd.RDD;
import org.task.sparql.cache.*;

import java.util.Set;

//import de.tf.uni.freiburg.sparkrdf.model.rdf.executionresults.IntermediateResultsModel;

/**
 * @author Thorsten Berberich
 */
public class SparkDistinct implements SparkOp {

    private final String TAG = "Distinct";
    private final OpDistinct op;

    public SparkDistinct(OpDistinct op) {
        this.op = op;
    }

    @Override
    public void execute() {
        if (op.getSubOp() != null) {
			RDD<SolutionMapping> oldResult = IntermediateResultsModel
				.getInstance().getResultRDD(op.getSubOp().hashCode());
			Set<String> vars = IntermediateResultsModel.getInstance()
				.getResultVariables(op.getSubOp().hashCode());

            RDD<SolutionMapping> result = SparkFacade.distinct(oldResult);

			IntermediateResultsModel.getInstance().removeResult(
				op.getSubOp().hashCode());
			IntermediateResultsModel.getInstance().putResult(op.hashCode(),
															 result, vars);
        }
    }

    @Override
    public void executeCached() {
        if (CachePool.markAsActive(op)) {
            return;
        }
        if (op.getSubOp() != null) {
            RDDVariablePair pair = CachePool.getResultPair(op.getSubOp());
            RDD<SolutionMapping> oldResult = pair.getRdd();
            Set<String> vars = pair.getVariables();

            RDD<SolutionMapping> result = SparkFacade.distinct(oldResult);

            IntermediateResultsModel.getInstance().removeResult(
                    op.getSubOp().hashCode());
            IntermediateResultsModel.getInstance().putResult(op.hashCode(),
                    result, vars);

            CachePool.putResult(op, result, vars);
        }
    }

    @Override
    public String getTag() {
        return TAG;
    }

}
