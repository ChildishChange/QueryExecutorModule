package de.tf.uni.freiburg.sparkrdf.parser.query.op;

import de.tf.uni.freiburg.sparkrdf.model.rdf.executionresults.IntermediateResultsModel;
import de.tf.uni.freiburg.sparkrdf.model.rdf.executionresults.RDDVariablePair;
import de.tf.uni.freiburg.sparkrdf.sparql.SparkFacade;
import de.tf.uni.freiburg.sparkrdf.sparql.operator.result.util.SolutionMapping;

import org.apache.jena.sparql.algebra.op.OpUnion;
import org.apache.spark.rdd.RDD;
import org.task.sparql.cache.CachePool;

import java.util.Set;

//import de.tf.uni.freiburg.sparkrdf.model.rdf.executionresults.IntermediateResultsModel;

/**
 * @author Thorsten Berberich
 */
public class SparkUnion implements SparkOp {

    private final String TAG = "Union";
    private final OpUnion op;

    public SparkUnion(OpUnion op) {
        this.op = op;
    }

    @Override
    public void execute() {
        if (op.getLeft() != null && op.getRight() != null) {
			RDD<SolutionMapping> leftResult = IntermediateResultsModel
				.getInstance().getResultRDD(op.getLeft().hashCode());
			Set<String> leftVars = IntermediateResultsModel.getInstance()
				.getResultVariables(op.getLeft().hashCode());

			RDD<SolutionMapping> rightResult = IntermediateResultsModel
				.getInstance().getResultRDD(op.getRight().hashCode());
			Set<String> rightVars = IntermediateResultsModel.getInstance()
				.getResultVariables(op.getRight().hashCode());

            RDD<SolutionMapping> result = SparkFacade.union(leftResult,
                                                            rightResult);
            leftVars.addAll(rightVars);
			IntermediateResultsModel.getInstance().putResult(op.hashCode(),
															 result, leftVars);

			IntermediateResultsModel.getInstance().removeResult(
				op.getLeft().hashCode());
			IntermediateResultsModel.getInstance().removeResult(
				op.getRight().hashCode());
        }
    }

    @Override
    public void executeCached() {
        if (CachePool.markAsActive(this.op)) {
            return;
        }
        if (op.getLeft() != null && op.getRight() != null) {
            RDDVariablePair leftPair;
            RDD<SolutionMapping> leftResult;
            Set<String> leftVars;
            if(op.getLeft()!=null){
                leftPair = CachePool.getResultPair(op.getLeft());
                leftResult = leftPair.getRdd();
                leftVars = leftPair.getVariables();
            }

            else {
                leftPair=null;
                leftResult=null;
                leftVars=null;
            }


//

            RDDVariablePair rightPair = CachePool.getResultPair(op.getRight());
            RDD<SolutionMapping> rightResult = rightPair.getRdd();
            Set<String> rightVars = rightPair.getVariables();

            RDD<SolutionMapping> result = SparkFacade.union(leftResult,
                                                            rightResult);
            leftVars.addAll(rightVars);


            CachePool.putResult(op, result, leftVars);
        }
    }

    @Override
    public String getTag() {
        return TAG;
    }

}
