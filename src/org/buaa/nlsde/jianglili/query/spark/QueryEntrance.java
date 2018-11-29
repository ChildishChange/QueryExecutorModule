package org.buaa.nlsde.jianglili.query.spark;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import de.tf.uni.freiburg.sparkrdf.model.rdf.executionresults.IntermediateResultsModel;
import de.tf.uni.freiburg.sparkrdf.parser.query.op.SparkOp;
import de.tf.uni.freiburg.sparkrdf.sparql.operator.result.util.SolutionMapping;
import jodd.io.FileUtil;
import org.apache.commons.io.FileUtils;
import org.buaa.nlsde.jianglili.cache.CachePool;
import org.buaa.nlsde.jianglili.query.JenaMemory.lubm1;
import org.json.JSONException;
import org.json.JSONObject;
import org.mortbay.jetty.HttpStatus;
import org.mortbay.jetty.Request;
import org.mortbay.jetty.Server;
import org.mortbay.jetty.handler.AbstractHandler;
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
import scala.tools.nsc.backend.icode.Opcodes;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import static jdk.nashorn.internal.objects.Global.println;


public class QueryEntrance extends AbstractHandler {

    public static String memOnEachCore;
    public static int serverPort;
    public static boolean localFlag = true;

    public static void main(String[] args)
            throws Exception
    {
        //load config from file
        JSONObject config = loadConfigFromFile(args[0]);
        if(config==null){return;}

        //setup cluster config
        memOnEachCore = config.getString("MEMORY");
        serverPort = Integer.parseInt(config.getString("PORT"));
        localFlag = true;

        //start server
        Server server = new Server(serverPort);
        server.setHandler(new QueryEntrance());
        server.start();
    }

    public static JSONObject loadConfigFromFile(String configFile)
            throws FileNotFoundException,IOException
    {
        JSONObject config = null;
        try
        {
            String input = FileUtils.readFileToString(new File(configFile));
            config = new JSONObject(input);
        }
        catch(Exception e)
        {
            e.printStackTrace();
            config = null;
        }

        return config;
    }


    @Override
    public void handle(String s,
                       HttpServletRequest httpServletRequest,
                       HttpServletResponse httpServletResponse,
                       int i)
            throws IOException,
            ServletException{
        //httpServletResponse.setContentType("text/html;charset=utf-8");
        //httpServletResponse.setStatus(HttpServletResponse.SC_OK);
        //((Request)httpServletRequest).setHandled(true);
        //httpServletResponse.getWriter().println("<h1>Hello World</h1>");


        //get the url and parameters
        String url = httpServletRequest.getRequestURI();
        httpServletResponse.setHeader("Access-Control-Allow-Origin", "*");
        httpServletResponse.setHeader("Access-Control-Allow-Methods", "GET, POST, DELETE, PUT");
        httpServletResponse.setHeader("Access-Control-Allow-Headers", "X-Requested-With,Content-Type,Accept,Origin");

        // get the query parameter
        String query = httpServletRequest.getParameter("q");
        String data = httpServletRequest.getParameter("data");

        //parameter initialization
        /*
        String instanceFile = args[0];
        String schemaFile = args[1];
        String queryFile = args[2];


        //rewrite or not
        Boolean rewriteFlag = args[3].charAt(0)=='1';
        //rewrite options
        int limNum = Integer.parseInt(args[4]);

        //jena or s2xt
        boolean jenaFlag = Integer.parseInt(args[5])==1;
        //cache pool or not
        boolean cachePoolFlag =Integer.parseInt(args[6])==1;


        //不管重不重写，都需要初始化concept
        try
        {
            Concept concept = QueryRewrting.initSchema("file:" + schemaFile, 0);

            if (jenaFlag)
            {
                Dataset instances = DatasetFactory.create(RDFDataMgr.loadModel(instanceFile));
                querySparql(instances, concept, queryFile, rewriteFlag, limNum);
            }
            else
            {
                initRuntimeEnvir(instanceFile, memOnEachCore, localFlag, schemaFile);
                runSPARQLQuery(queryFile, concept, limNum, cachePoolFlag, rewriteFlag);
            }
        }
        catch(Exception e)
        {

        }
        */


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
