package de.tf.uni.freiburg.sparkrdf.model.rdf.executionresults;

import de.tf.uni.freiburg.sparkrdf.sparql.operator.result.util.SolutionMapping;

import org.apache.spark.rdd.RDD;

import java.util.Set;

/**
 * Class to store the execution result and the variables that occur
 *
 * @author Thorsten Berberich
 */
public class RDDVariablePair {

    /**
     * RDD of an execution
     */
    private final RDD<SolutionMapping> rdd;

    /**
     * Variables of the RDD
     */
    private final Set<String> variables;

    /**
     * Create a new pair
     *
     * @param rdd       RDD of an execution
     * @param variables All variables that occur in the RDD
     */
    public RDDVariablePair(RDD<SolutionMapping> rdd, Set<String> variables) {
        this.rdd = rdd;
        this.variables = variables;
    }

    /**
     * Get the stored RDD
     *
     * @return {@link RDD<SolutionMapping>}
     */
    public RDD<SolutionMapping> getRdd() {
        return rdd;
    }

    /**
     * Get all variables of the RDD
     *
     * @return Set of variables
     */
    public Set<String> getVariables() {
        return variables;
    }

}
