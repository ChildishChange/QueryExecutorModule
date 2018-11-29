package org.buaa.nlsde.jianglili.query.spark;


import de.tf.uni.freiburg.sparkrdf.constants.Const;
import de.tf.uni.freiburg.sparkrdf.model.rdf.executionresults.IntermediateResultsModel;
import de.tf.uni.freiburg.sparkrdf.parser.query.AlgebraTranslator;
import de.tf.uni.freiburg.sparkrdf.parser.query.AlgebraWalker;
import de.tf.uni.freiburg.sparkrdf.parser.query.op.SparkOp;
import de.tf.uni.freiburg.sparkrdf.sparql.SparkFacade;
import de.tf.uni.freiburg.sparkrdf.sparql.operator.result.util.SolutionMapping;
import org.apache.jena.query.*;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.shared.PrefixMapping;
import org.apache.jena.sparql.algebra.Algebra;
import org.apache.jena.sparql.algebra.Op;
import org.apache.log4j.Logger;
import org.buaa.nlsde.jianglili.reasoningquery.QueryRewrting;
import org.buaa.nlsde.jianglili.reasoningquery.conceptExtract.Concept;
import org.mortbay.jetty.handler.AbstractHandler;
import org.semanticweb.owlapi.model.OWLOntologyCreationException;
import org.semanticweb.owlapi.model.OWLOntologyStorageException;

import test.query.*;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.List;
import java.util.Queue;

/**
 * Created by jianglili on 2017/2/26.
 */
public class S2XTQueryFactory /*extends AbstractHandler */{

    public static void main(String[] args) throws OWLOntologyCreationException, OWLOntologyStorageException, FileNotFoundException {

       // args=new String[]{"E:/benchmarks/LUBM/datasets/nt1","3g","true", "E://benchmarks//LUBM//univ-benchQL-ub.owl"};
      //  init("E:/benchmarks/LUBM/datasets/nt1","3g",true);
        init("/Users/tewisong/Downloads/benchmarks/LUBM/datasets/nt1","3g",true);
//        init(args[0],args[1],args[2]=="true"?true:false);
        //Query query = QueryFactory.read("E:/benchmarks/LUBM/query28/query1.rq");
        Query query = QueryFactory.read("E:/benchmarks/LUBM/query28/query1.rq");
        System.out.println(query.toString());
        //load the schema
        //Concept   concept = QueryRewrting.initSchema("file:"+ "E://benchmarks//LUBM//univ-benchQL-ub.owl",0);
        ///Users/tewisong/Downloads/benchmarks/LUBM/univ-benchQL-ub.owl
        Concept   concept = QueryRewrting.initSchema("file:"+ "/Users/tewisong/Downloads/benchmarks/LUBM/univ-benchQL-ub.owl",0);
        List<SolutionMapping>  solutionMappings=runSPARQLQuery(query.toString());
        System.out.println("solutionMap: "+solutionMappings);
        List<SolutionMapping>  solutionMappings2=runSPARQLQueryRewrite(query.toString(),concept);
        System.out.println("solutionMap2: "+solutionMappings2);

    }

    public static void init(String data,String executorMem,boolean local)
    {
        // init the param
        Const.inputFile_$eq(data);
        Const.executorMem_$eq(executorMem);
        Const.locale_$eq(local);
        // create a new SparkContext
        SparkFacade.createSparkContext();
        // Load the graph
        SparkFacade.loadGraph();

    }

    public static List<SolutionMapping> runSPARQLQuery(String queryString) throws OWLOntologyCreationException, OWLOntologyStorageException, FileNotFoundException {

        Query query = QueryFactory.create(queryString);
        PrefixMapping prefixes = query.getPrefixMapping();
        Op opRoot = Algebra.compile(query);
        // rewrite the op
        AlgebraTranslator trans = new AlgebraTranslator(prefixes);
        opRoot.visit(new AlgebraWalker(trans));
        // Queue with all operators
        Queue<SparkOp> q = trans.getExecutionQueue();
        // Execute all operators from the queue
        while (!q.isEmpty()) {
            SparkOp actual = q.poll();
            actual.execute();
        }

        return IntermediateResultsModel
                .getInstance().getFinalResultAsList();

    }

    public static  List<SolutionMapping> runSPARQLQueryRewrite(String queryString, Concept concept) throws OWLOntologyCreationException, OWLOntologyStorageException, FileNotFoundException {

        Query query = QueryFactory.create(queryString);
        PrefixMapping prefixes = query.getPrefixMapping();
        Op opRoot = Algebra.compile(query);
        // rewrite the op
        Op opRootRewrite= QueryRewrting.transform(opRoot, concept,0);
        AlgebraTranslator trans = new AlgebraTranslator(prefixes);
        opRootRewrite.visit(new AlgebraWalker(trans));
        // Queue with all operators
        Queue<SparkOp> q = trans.getExecutionQueue();
        // Execute all operators from the queue
        while (!q.isEmpty()) {
            SparkOp actual = q.poll();
            actual.execute();
        }
        return IntermediateResultsModel
                .getInstance().getFinalResultAsList();

    }


    public static void close() {
        SparkFacade.closeContext();
    }

//    @Override
//    public void handle(String target, HttpServletRequest request, HttpServletResponse response, int dispatch) throws IOException, ServletException {
//
//    }
}
