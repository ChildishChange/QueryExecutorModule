package de.tf.uni.freiburg.sparkrdf.parser.query.op;

import de.tf.uni.freiburg.sparkrdf.model.rdf.executionresults.IntermediateResultsModel;
import de.tf.uni.freiburg.sparkrdf.parser.query.expression.ExprCompiler;
import de.tf.uni.freiburg.sparkrdf.parser.query.expression.op.ExprVar;
import de.tf.uni.freiburg.sparkrdf.sparql.SparkFacade;
import de.tf.uni.freiburg.sparkrdf.sparql.operator.result.util.SolutionMapping;

import org.apache.jena.query.Query;
import org.apache.jena.query.SortCondition;
import org.apache.jena.shared.PrefixMapping;
import org.apache.jena.sparql.algebra.op.OpOrder;
import org.apache.jena.sparql.expr.Expr;
import org.apache.spark.rdd.RDD;
import org.task.sparql.cache.CachePool;

//import de.tf.uni.freiburg.sparkrdf.model.rdf.executionresults.IntermediateResultsModel;

/**
 * @author Thorsten Berberich
 */
public class SparkOrderBy implements SparkOp {

    private final String TAG = "Order By";
    private final OpOrder op;
    private final PrefixMapping prefixes;

    public SparkOrderBy(OpOrder op, PrefixMapping prefixes) {
        this.op = op;
        this.prefixes = prefixes;
    }

    @Override
    public void execute() {
        SortCondition condition = op.getConditions().iterator().next();
        Expr next = condition.getExpression();
        ExprCompiler translator = new ExprCompiler(prefixes);
        try {
            String var = ((ExprVar) translator.translate(next)).getVar();
            Boolean asc = false;
            switch (condition.getDirection()) {
                case Query.ORDER_ASCENDING: {
                    asc = true;
                    break;
                }
                case Query.ORDER_DESCENDING: {
                    asc = false;
                    break;
                }
                case Query.ORDER_DEFAULT: {
                    asc = true;
                    break;
                }
            }
			RDD<SolutionMapping> toOrder = IntermediateResultsModel
				.getInstance().getResultRDD(op.getSubOp().hashCode());

			RDD<SolutionMapping> ordered = SparkFacade.order(toOrder, var, asc);
			IntermediateResultsModel.getInstance().putResult(
				op.hashCode(),
				ordered,
				IntermediateResultsModel.getInstance().getResultVariables(
					op.getSubOp().hashCode()));

			IntermediateResultsModel.getInstance().removeResult(
				op.getSubOp().hashCode());
        } catch (ClassCastException e) {
        }
    }

    @Override
    public void executeCached() {
        if (CachePool.markAsActive(this.op)) {
            return;
        }
        SortCondition condition = op.getConditions().iterator().next();
        Expr next = condition.getExpression();
        ExprCompiler translator = new ExprCompiler(prefixes);
        try {
            String var = ((ExprVar) translator.translate(next)).getVar();
            Boolean asc = false;
            switch (condition.getDirection()) {
                case Query.ORDER_ASCENDING: {
                    asc = true;
                    break;
                }
                case Query.ORDER_DESCENDING: {
                    asc = false;
                    break;
                }
                case Query.ORDER_DEFAULT: {
                    asc = true;
                    break;
                }
            }
            RDD<SolutionMapping> toOrder = CachePool.getResultRDD(op.getSubOp());
            RDD<SolutionMapping> ordered = SparkFacade.order(toOrder, var, asc);

            IntermediateResultsModel.getInstance().putResult(
                    op.hashCode(),
                    ordered,
                    IntermediateResultsModel.getInstance().getResultVariables(
                            op.getSubOp().hashCode()));

            IntermediateResultsModel.getInstance().removeResult(
                    op.getSubOp().hashCode());

            CachePool.putResult(op, ordered, CachePool.getResultVariables(op.getSubOp()));
        } catch (ClassCastException e) {
        }
    }

    @Override
    public String getTag() {
        return TAG;
    }

}
