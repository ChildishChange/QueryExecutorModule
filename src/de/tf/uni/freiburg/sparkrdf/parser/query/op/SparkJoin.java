package de.tf.uni.freiburg.sparkrdf.parser.query.op;

import de.tf.uni.freiburg.sparkrdf.model.rdf.executionresults.IntermediateResultsModel;
import de.tf.uni.freiburg.sparkrdf.model.rdf.executionresults.RDDVariablePair;
import de.tf.uni.freiburg.sparkrdf.sparql.SparkFacade;
import de.tf.uni.freiburg.sparkrdf.sparql.operator.result.util.SolutionMapping;
import org.apache.jena.shared.PrefixMapping;
import org.apache.jena.sparql.algebra.op.OpJoin;
import org.apache.spark.rdd.RDD;
import org.task.sparql.cache.CachePool;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Set;

/**
 * Spark OpJoin
 */
public class SparkJoin implements SparkOp{
    private final OpJoin op;
    private final String TAG = "Join";
    public SparkJoin(OpJoin op) {
        this.op = op;
    }

    @Override
    public void execute() {
        if (op.getLeft() != null && op.getRight() != null) {
            int leftID = op.getLeft().hashCode();
            RDD<SolutionMapping> leftRes = IntermediateResultsModel
                .getInstance().getResultRDD(leftID);

            int rightID = op.getRight().hashCode();
            RDD<SolutionMapping> rightRes = IntermediateResultsModel
                .getInstance().getResultRDD(rightID);

            // Intersection between the variables, gives the join variables
            Set<String> joinVars = getJoinVars(
                IntermediateResultsModel.getInstance().getResultVariables(
                    op.getLeft().hashCode()),
                IntermediateResultsModel.getInstance().getResultVariables(
                    op.getRight().hashCode()));
            // Union of the variables are the resulting variables
            Set<String> resultVars = new HashSet<>(IntermediateResultsModel
                                                       .getInstance().getResultVariables(op.getLeft().hashCode()));
            resultVars.addAll(IntermediateResultsModel.getInstance()
                                  .getResultVariables(op.getRight().hashCode()));

            RDD<SolutionMapping> result =  SparkFacade.join(new ArrayList<String>(
                joinVars), leftRes, rightRes);

            IntermediateResultsModel.getInstance().removeResult(leftID);
            IntermediateResultsModel.getInstance().removeResult(rightID);
//            int resNum=IntermediateResultsModel.getInstance().resultNum();
  //          System.out.println("resultNum after Jion "+resNum);
            IntermediateResultsModel.getInstance().putResult(op.hashCode(),
                                                             result, resultVars);
  //          resNum=IntermediateResultsModel.getInstance().resultNum();
   //         System.out.println("resultNum after Jion "+resNum);
        }
    }

    @Override
    public void executeCached() {
        if (CachePool.markAsActive(op)) {
            return;
        }
        if (op.getLeft() != null && op.getRight() != null) {

//            int leftID = op.getLeft().hashCode();
//            int rightID = op.getRight().hashCode();
            RDDVariablePair leftPair = CachePool.getResultPair(op.getLeft());
            RDDVariablePair rightPair = CachePool.getResultPair(op.getRight());

            RDD<SolutionMapping> leftRes = leftPair.getRdd();
            RDD<SolutionMapping> rightRes = rightPair.getRdd();

            Set<String> joinVars = getJoinVars(
                leftPair.getVariables(),
                rightPair.getVariables());
            // Union of the variables are the resulting variables
            Set<String> resultVars = new HashSet<>(leftPair.getVariables());
            resultVars.addAll(rightPair.getVariables());

            RDD<SolutionMapping> result =  SparkFacade.join(new ArrayList<String>(
                joinVars),leftRes,rightRes);

            CachePool.putResult(op, result, resultVars);
        }
    }

    private Set<String> getJoinVars(Set<String> left, Set<String> right) {
        left.retainAll(right);
        return left;
    }

    @Override
    public String getTag() {
        return TAG;
    }
}