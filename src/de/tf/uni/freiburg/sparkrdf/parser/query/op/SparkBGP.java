package de.tf.uni.freiburg.sparkrdf.parser.query.op;

import de.tf.uni.freiburg.sparkrdf.constants.Const;
import de.tf.uni.freiburg.sparkrdf.model.graph.node.VertexInterface;
import de.tf.uni.freiburg.sparkrdf.model.rdf.executionresults.IntermediateResultsModel;
import de.tf.uni.freiburg.sparkrdf.model.rdf.executionresults.RDDVariablePair;
import de.tf.uni.freiburg.sparkrdf.model.rdf.triple.TriplePattern;
import de.tf.uni.freiburg.sparkrdf.model.rdf.triple.TriplePatternUtils;
import de.tf.uni.freiburg.sparkrdf.parser.PrefixUtil;
import de.tf.uni.freiburg.sparkrdf.parser.query.expression.ExprCompiler;
import de.tf.uni.freiburg.sparkrdf.sparql.SparkFacade;
import de.tf.uni.freiburg.sparkrdf.sparql.operator.result.util.SolutionMapping;

import org.apache.jena.graph.Node;
import org.apache.jena.graph.Triple;
import org.apache.jena.shared.PrefixMapping;
import org.apache.jena.sparql.algebra.op.OpBGP;
import org.apache.jena.sparql.core.Var;
import org.apache.jena.sparql.expr.Expr;
import org.apache.jena.sparql.expr.ExprList;
import org.apache.jena.sparql.util.FmtUtils;
import org.apache.spark.rdd.RDD;
import org.task.sparql.cache.CachePool;

import java.util.*;

import scala.Tuple2;

//import de.tf.uni.freiburg.sparkrdf.model.rdf.executionresults.IntermediateResultsModel;

/**
 * @author Thorsten Berberich
 */
public class SparkBGP implements SparkOp {

    private final int ID;
    private final OpBGP op;
    private final PrefixMapping prefixes;
    private List<TriplePattern> bgp;
    private RDD<Tuple2<Object, VertexInterface>> bgpMatched;
    private int execCount = 0;
    private ExprList expressions;

    public SparkBGP(OpBGP op, PrefixMapping prefixes) {
        this.prefixes = prefixes;
        ID = op.hashCode();
        this.op = op;
        buildBGP(op);
    }

    public void addExpressions(ExprList expressions) {
        this.expressions = expressions;
    }

    private void buildBGP(OpBGP op) {
        final List<Triple> triples = op.getPattern().getList();
        for (final Triple triple : triples) {
            final Node subjectNode = triple.getSubject();
            final Node predicateNode = triple.getPredicate();
            final Node objectNode = triple.getObject();

            final String subject = PrefixUtil.collapsePrefix(FmtUtils
                                                                 .stringForNode(subjectNode,
                                                                                prefixes));
            final String object = PrefixUtil.collapsePrefix(FmtUtils
                                                                .stringForNode(objectNode,
                                                                               prefixes));
            final String predicate = PrefixUtil.collapsePrefix(FmtUtils
                                                                   .stringForNode(predicateNode,
                                                                                  prefixes));

            if (bgp == null) {
                bgp = new ArrayList<TriplePattern>();
            }
            bgp.add(new TriplePattern(subject, predicate, object));
        }
    }

    @Override
    public void execute() {
        if (bgpMatched == null) {
            // Match the bgp
            addFiltersToBGP();
            bgpMatched = SparkFacade.executeBasicGraphPattern(bgp);
        } else {
            // Build the result
            final RDD<SolutionMapping> result = SparkFacade.buildResult(bgp,
                                                                            bgpMatched);
//            if(result==null)
//                System.out.println("RDD res in bgp is null");
			IntermediateResultsModel.getInstance().putResult(ID, result,
                                                             getVariables());
        //    int resNum=IntermediateResultsModel.getInstance().resultNum();
//            System.out.println("resultNum after BGP "+resNum);
        }
    }

    @Override
    public void executeCached() {
        if (CachePool.markAsActive(this.op)) {
            return;
        }
        if (bgpMatched == null) {
            // Match the bgp
            addFiltersToBGP();
            bgpMatched = SparkFacade.executeBasicGraphPattern(bgp);
        } else {
            // Build the result
            final RDD<SolutionMapping> result = SparkFacade.buildResult(bgp,
                                                                        bgpMatched);
            CachePool.putResult(this.op, result, getVariables());
        }
    }

    private void addFiltersToBGP() {
        if (expressions != null) {
            final Iterator<Expr> iterator = expressions.iterator();

            final ExprCompiler translator = new ExprCompiler(prefixes);

            while (iterator.hasNext()) {
                final Expr current = iterator.next();
                final int varCount = current.getVarsMentioned().size();

                for (final TriplePattern tp : bgp) {
                    int matching = 0;

                    if (TriplePatternUtils.isVariable(tp.getSubject())) {
                        for (final Var var : current.getVarsMentioned()) {
                            if (var.toString().equals(tp.getSubject())) {
                                matching++;
                            }
                        }
                    }

                    if (TriplePatternUtils.isVariable(tp.getObject())) {
                        for (final Var var : current.getVarsMentioned()) {
                            if (var.toString().equals(tp.getObject())) {
                                matching++;
                            }
                        }
                    }

                    if (TriplePatternUtils.isVariable(tp.getPredicate())) {
                        for (final Var var : current.getVarsMentioned()) {
                            if (var.toString().equals(tp.getPredicate())) {
                                matching++;
                            }
                        }
                    }

                    if (matching == varCount) {
                        tp.addFilterExpression(translator.translate(current));
                    }
                }

            }
        }
    }

    private Set<String> getVariables() {
        final Set<String> result = new HashSet<>();
        for (final TriplePattern tp : bgp) {
            if (TriplePatternUtils.isVariable(tp.getSubject())) {
                result.add(tp.getSubject());
            }

            if (TriplePatternUtils.isVariable(tp.getPredicate())) {
                result.add(tp.getPredicate());
            }

            if (TriplePatternUtils.isVariable(tp.getObject())) {
                result.add(tp.getObject());
            }
        }
        return result;
    }

    @Override
    public String getTag() {
        if (execCount < 1) {
            execCount++;
            return "BGP";
        } else {
            return "Result";
        }
    }

}
