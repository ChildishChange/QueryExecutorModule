package org.buaa.nlsde.jianglili.query.spark;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import de.tf.uni.freiburg.sparkrdf.model.rdf.executionresults.IntermediateResultsModel;
import de.tf.uni.freiburg.sparkrdf.parser.query.op.SparkOp;
import de.tf.uni.freiburg.sparkrdf.sparql.operator.result.util.SolutionMapping;
import org.buaa.nlsde.jianglili.cache.CachePool;
import org.buaa.nlsde.jianglili.query.JenaMemory.lubm1;
import org.semanticweb.owlapi.model.OWLOntologyCreationException;
import org.semanticweb.owlapi.model.OWLOntologyStorageException;
import de.tf.uni.freiburg.sparkrdf.constants.Const;
import org.apache.jena.query.*;
import org.apache.jena.riot.RDFDataMgr;

import de.tf.uni.freiburg.sparkrdf.sparql.SparkFacade;
import org.buaa.nlsde.jianglili.reasoningquery.QueryRewrting;
import org.buaa.nlsde.jianglili.reasoningquery.conceptExtract.Concept;
import java.io.FileNotFoundException;
import java.util.Map;
import java.util.Queue;
import java.util.TreeMap;

import org.apache.jena.sparql.algebra.Algebra;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.engine.QueryIterator;
import org.apache.jena.sparql.engine.binding.Binding;
import org.apache.spark.rdd.RDD;
import de.tf.uni.freiburg.sparkrdf.parser.query.*;

import java.io.FileNotFoundException;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.jena.shared.PrefixMapping;


public class QueryEntrance {

    public static void main(String[] args)
            throws OWLOntologyCreationException,
                   OWLOntologyStorageException,
                   FileNotFoundException,
                   IOException
    {

        //parameter initialization
        String instanceFile = args[0];
        String schemaFile = args[1];
        String queryFile = args[2];
        String memOnEachCore = "3g";
        Boolean localFlag = true;

        //rewrite or not
        Boolean rewriteFlag = args[3].charAt(0)=='1';
        //rewrite options
        int limNum = Integer.parseInt(args[4]);

        //jena or s2xt
        Boolean jenaFlag = Integer.parseInt(args[5])==1;
        //cache pool or not
        Boolean cachePoolFlag =Integer.parseInt(args[6])==1;


        //不管重不重写，都需要初始化concept
        Concept concept  = QueryRewrting.initSchema("file:"+schemaFile, 0);

        if(jenaFlag)
        {
            Dataset instances =DatasetFactory.create(RDFDataMgr.loadModel(instanceFile));
            querySparql(instances,concept,queryFile,rewriteFlag,limNum);
        }
        else
        {
            initRuntimeEnvir(instanceFile,memOnEachCore,localFlag,schemaFile);
            runSPARQLQuery(queryFile,concept,limNum,cachePoolFlag,rewriteFlag);
        }
    }

    private static void querySparql(Dataset instances,
                                    Concept concept,
                                    String queryFile,
                                    Boolean rewriteFlag,
                                    int limNum)
            throws OWLOntologyCreationException,
                   OWLOntologyStorageException,
                   FileNotFoundException
    {
        Map<String, Long> operationDuration = new TreeMap<>();

        System.out.println("Search "+queryFile+" on "+instances);

        Query query = QueryFactory.read(queryFile);
        Op opRoot = Algebra.compile(query) ;

        Op opRootRewrite = (rewriteFlag)?QueryRewrting.transform(opRoot,concept,limNum):null;

        //Execute query
        QueryIterator qIter = Algebra.exec((rewriteFlag)?opRootRewrite:opRoot, instances) ;
        int results = 0;
        for ( ; qIter.hasNext() ; )
        {
            Binding b = qIter.nextBinding();
            results++;
        }
        qIter.close();
        System.out.println("# original query solution mappings: "+results);

    }

    public static void initRuntimeEnvir(String instanceFile,
                                        String executorMem,
                                        Boolean localFlag,
                                        String schemaFile)
    {
        Const.inputFile_$eq(instanceFile);
        Const.executorMem_$eq(executorMem);
        Const.locale_$eq(localFlag);
        Const.schema_$eq(schemaFile);
        SparkFacade.createSparkContext();
        SparkFacade.loadGraph();
    }
    public static List<SolutionMapping> runSPARQLQuery(String queryFile,
                                                     Concept concept,
                                                     int limNum,
                                                     boolean cachePoolFlag,
                                                     boolean rewriteFlag)
            throws OWLOntologyCreationException,
            OWLOntologyStorageException,
            FileNotFoundException,
            IOException
    {
        IntermediateResultsModel.getInstance().clearResults();

        Stream<String> stream = Files.lines(Paths.get(queryFile));

        String queryString = stream.collect(Collectors.joining(" "));
        Query query = QueryFactory.create(queryString);
        PrefixMapping prefixes = query.getPrefixMapping();

        Op opRoot = Algebra.compile(query);
        Op opRootRewrite = (rewriteFlag)?QueryRewrting.transform(opRoot,concept,limNum):null;

        AlgebraTranslator trans = new AlgebraTranslator(prefixes);

        if(rewriteFlag)
        {
            opRootRewrite.visit(new AlgebraWalker(trans));
            if(cachePoolFlag&&CachePool.contains(opRootRewrite))
            {
                return CachePool.getFinalResult(opRootRewrite).toJavaRDD().collect();
            }
        }
        else
        {
            opRoot.visit(new AlgebraWalker(trans));
            if(cachePoolFlag&&CachePool.contains(opRoot))
            {
                return CachePool.getFinalResult(opRoot).toJavaRDD().collect();
            }
        }

        Queue<SparkOp> q = trans.getExecutionQueue();
        while (!q.isEmpty()) {
            SparkOp actual = q.poll();
            if(cachePoolFlag)
                actual.executeCached();
            actual.execute();
        }
        if(cachePoolFlag)
        {
            if(rewriteFlag)
            {
                RDD<SolutionMapping> res = CachePool.getFinalResult(opRootRewrite);
                return (res!=null)?CachePool.getFinalResult(opRootRewrite).toJavaRDD().collect():null;
            }
            else
            {
                RDD<SolutionMapping> res = CachePool.getFinalResult(opRoot);
                return (res!=null)?CachePool.getFinalResult(opRoot).toJavaRDD().collect():null;
            }
        }
        return IntermediateResultsModel.getInstance().getFinalResultAsList();
    }
}
