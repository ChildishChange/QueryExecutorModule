package de.tf.uni.freiburg.sparkrdf.parser.query.expression;

import de.tf.uni.freiburg.sparkrdf.parser.query.expression.op.*;
import org.apache.jena.shared.PrefixMapping;
import org.apache.jena.sparql.expr.*;
import org.apache.jena.sparql.expr.ExprVar;
import org.apache.jena.sparql.expr.NodeValue;
import org.apache.jena.sparql.util.FmtUtils;

import java.util.Stack;

/**
 * Adapted class from Alexander Schaetzle. This visitor builds a tree of
 * {@link IExpression}s out of the expression tree from ARQ.
 *
 * @author Thorsten Berberich
 */
public class ExprCompiler implements ExprVisitor {

    /**
     * Stack which contains the parsed operators
     */
    private final Stack<IExpression> stack;
    /**
     * All prefixes
     */
    private final PrefixMapping prefixes;

    /**
     * Create a new compiler
     *
     * @param prefixes
     *            Prefixes to use
     */
    public ExprCompiler(PrefixMapping prefixes) {
        stack = new Stack<IExpression>();
        this.prefixes = prefixes;
    }

    /**
     * Translate an expression into SparkOperators
     *
     * @param expr
     *            Expression to translate
     * @return The first element of the stack
     */
    public IExpression translate(Expr expr) {
        ExprWalker.walkBottomUp(this, expr);
        return stack.pop();
    }

    @Override
    public void startVisit() {
    }

    public void visit(ExprFunction func) {
        if (func instanceof ExprFunction0) {
            visit((ExprFunction0) func);
        } else if (func instanceof ExprFunction1) {
            visit((ExprFunction1) func);
        } else if (func instanceof ExprFunction2) {
            visit((ExprFunction2) func);
        } else if (func instanceof ExprFunction3) {
            visit((ExprFunction3) func);
        } else if (func instanceof ExprFunctionN) {
            visit((ExprFunctionN) func);
        } else if (func instanceof ExprFunctionOp) {
            visit((ExprFunctionOp) func);
        }
    }

    @Override
    public void visit(ExprFunction1 func) {
        IExpression sub = stack.pop();

        IExpression result = null;

        if (func instanceof E_LogicalNot) {
            result = new Not(sub);
        } else if (func instanceof E_Bound) {
            result = new Bound(sub);
        } else if (func instanceof E_Lang) {
            result = new Lang(sub);
        }

        if (result == null) {
            throw new UnsupportedOperationException(
                    "Filter expression not supported yet!");
        } else {
            stack.push(result);
        }
    }

    @Override
    public void visit(ExprFunction2 func) {

        IExpression right = stack.pop();
        IExpression left = stack.pop();

        Expr2 operator = null;

        if (func instanceof E_GreaterThan) {
            operator = new GreaterThan(left, right);
        } else if (func instanceof E_GreaterThanOrEqual) {
            operator = new GreaterThenEqual(left, right);
        } else if (func instanceof E_LessThan) {
            operator = new LessThan(left, right);
        } else if (func instanceof E_LessThanOrEqual) {
            operator = new LessThanEqual(left, right);
        } else if (func instanceof E_Equals) {
            operator = new Equals(left, right);
        } else if (func instanceof E_NotEquals) {
            operator = new NotEquals(left, right);
        } else if (func instanceof E_LogicalAnd) {
            operator = new LogAnd(left, right);
        } else if (func instanceof E_LogicalOr) {
            operator = new LogOr(left, right);
        } else if (func instanceof E_Add) {
            operator = new Add(left, right);
        } else if (func instanceof E_Subtract) {
            operator = new Subtract(left, right);
        } else if (func instanceof E_LangMatches) {
            operator = new LangMatches(left, right);
        }

        if (operator == null) {
            throw new UnsupportedOperationException(
                    "Filter expression not supported yet!");
        } else {
            // New expression
            stack.push(operator);
        }
    }

    @Override
    public void visit(NodeValue nv) {
        if (nv.asNode().isLiteral()) {
            stack.push(new de.tf.uni.freiburg.sparkrdf.parser.query.expression.op.NodeValue(
                    nv.asQuotedString()));
        } else {
            stack.push(new de.tf.uni.freiburg.sparkrdf.parser.query.expression.op.NodeValue(
                    FmtUtils.stringForNode(nv.asNode(), prefixes)));
        }
    }

    @Override
    public void visit(ExprVar nv) {
        stack.push(new de.tf.uni.freiburg.sparkrdf.parser.query.expression.op.ExprVar(
                nv.getVarName()));
    }

    @Override
    public void visit(ExprFunction0 func) {
        throw new UnsupportedOperationException(
                "ExprFunction0 not supported yet.");
    }

    @Override
    public void visit(ExprFunction3 func) {
        throw new UnsupportedOperationException(
                "ExprFunction3 not supported yet.");
    }

    @Override
    public void visit(ExprFunctionN func) {
        if (func instanceof E_Regex) {
            IExpression right = stack.pop();
            IExpression left = stack.pop();

            Expr2 operator = new RegEx(left, right);
            stack.push(operator);

        } else if (func instanceof E_Function) {
            throw new UnsupportedOperationException(
                    "ExprFunctionN not supported yet!");
        } else {
            throw new UnsupportedOperationException(
                    "ExprFunctionN not supported yet!");
        }
    }

    @Override
    public void visit(ExprFunctionOp funcOp) {
        throw new UnsupportedOperationException(
                "ExprFunctionOp not supported yet.");
    }

    @Override
    public void visit(ExprAggregator eAgg) {
        throw new UnsupportedOperationException(
                "ExprAggregator not supported yet.");
    }

    @Override
    public void finishVisit() {
    }
}
