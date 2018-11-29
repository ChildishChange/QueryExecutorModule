package org.buaa.nlsde.jianglili.query.JenaMemory;

import org.apache.hadoop.hdfs.server.blockmanagement.CorruptReplicasMap;
import org.apache.jena.query.*;
import org.apache.jena.rdf.model.*;
import org.apache.jena.reasoner.Reasoner;
import org.apache.jena.reasoner.ReasonerFactory;
import org.apache.jena.reasoner.rulesys.RDFSFBRuleReasonerFactory;
import org.apache.jena.reasoner.rulesys.RDFSForwardRuleReasoner;
import org.apache.jena.reasoner.transitiveReasoner.TransitiveReasonerFactory;
import org.apache.jena.riot.RDFDataMgr;
import org.apache.jena.sparql.algebra.Algebra;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.engine.QueryIterator;
import org.apache.jena.sparql.engine.binding.Binding;

import static org.apache.jena.sparql.vocabulary.VocabTestQuery.query;

/**
 * Created by jianglili on 2017/1/22.
 */
public class Memory {

    public static void main(String[] args) {

        //E:\benchmarks\LUBM\datasets\nt\Universities_1_new.nt
        //E://benchmarks//LUBM//univ-benchQL.owl
        String datafile="file:///E://benchmarks//LUBM//datasets//dl-rdf//University0_0.owl";
        String queryfile="E:\\benchmarks\\LUBM\\query28-ub\\query1.rq";

         query2(queryfile,datafile);

    }
    public static void query2(String  queryfile,String datafile) {

        Query query = QueryFactory.read(queryfile);
//        Dataset dataset = DatasetFactory.create(datafile);
//        QueryExecution qexec = QueryExecutionFactory.create(query, dataset) ;
//        // Execute it.
//        ResultSet resultSet=qexec.execSelect();
        Model model = RDFDataMgr.loadModel(datafile) ;
        // Results
        QueryExecution qexec = QueryExecutionFactory.create(query, model);
        ResultSet results = qexec.execSelect() ;
        for ( ; results.hasNext() ; )
        {
            QuerySolution soln = results.nextSolution() ;
            RDFNode x = soln.get("x") ;       // Get a result variable by name.
              RDFNode y = soln.get("y") ; // Get a result variable - must be a resource
//            RDFNode l = soln.get("z") ;   // Get a result variable - must be a literal

            System.out.println(x==null?"null":x.toString());
            if(y!=null) System.out.println(y.toString());

        }



    }
    public static void query(String  queryfile,String datafile) {

        Query query = QueryFactory.read(queryfile);
        Op op = Algebra.compile(query) ;
        op = Algebra.optimize(op) ;

        Dataset dataset = DatasetFactory.create(datafile);
        QueryExecution qexec = QueryExecutionFactory.create(query, dataset) ;
        // Execute it.
        QueryIterator qIter = Algebra.exec(op, dataset) ;

        // Results
        int results = 0;
        for ( ; qIter.hasNext() ; )
        {
            Binding b = qIter.nextBinding() ;
            results++;
            System.out.println(b) ;
        }
        qIter.close() ;
        System.out.println("# solution mappings: "+results);


    }
    public static void queryFileList(String  queryfile,String datafile) {

        Query query = QueryFactory.read(queryfile);
        Op op = Algebra.compile(query) ;
        op = Algebra.optimize(op) ;


        Dataset dataset = DatasetFactory.create(datafile);
        QueryExecution qexec = QueryExecutionFactory.create(query, dataset) ;
        // Execute it.
        QueryIterator qIter = Algebra.exec(op, dataset) ;

        // Results
        int results = 0;
        for ( ; qIter.hasNext() ; )
        {
            Binding b = qIter.nextBinding() ;
            results++;
            System.out.println(b) ;
        }
        qIter.close() ;
        System.out.println("# solution mappings: "+results);


    }


//        try {
//
//            ResultSet results2 = qexec.execSelect();
//
//            for (; results2.hasNext(); )
//
//            {
//                QuerySolution soln = results2.nextSolution();
//
//                RDFNode name = soln.get("fname");       // Get a result variable by name.
//
//                Resource x = soln.getResource("x"); // Get a result variable - must be a resource
//
//                //Literal l = soln.getLiteral("VarL") ;   // Get a result variable - must be a literal
//
//                System.out.println(name.toString() + " " + x.toString());
//            }
//        }
//       finally { qexec.close() ; }

}
