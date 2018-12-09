package org.buaa.nlsde.jianglili.query.spark;

import de.tf.uni.freiburg.sparkrdf.constants.Const;
import de.tf.uni.freiburg.sparkrdf.model.rdf.executionresults.IntermediateResultsModel;
import de.tf.uni.freiburg.sparkrdf.parser.query.AlgebraTranslator;
import de.tf.uni.freiburg.sparkrdf.parser.query.AlgebraWalker;
import de.tf.uni.freiburg.sparkrdf.parser.query.op.SparkOp;
import de.tf.uni.freiburg.sparkrdf.sparql.SparkFacade;
import de.tf.uni.freiburg.sparkrdf.sparql.operator.result.util.SolutionMapping;
import org.apache.commons.io.FileUtils;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.NodeFactory;
import org.apache.jena.graph.Triple;
import org.apache.jena.query.Dataset;
import org.apache.jena.query.DatasetFactory;
import org.apache.jena.query.Query;
import org.apache.jena.query.QueryFactory;
import org.apache.jena.riot.RDFDataMgr;
import org.apache.jena.shared.PrefixMapping;
import org.apache.jena.sparql.algebra.Algebra;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.algebra.op.OpProject;
import org.apache.jena.sparql.core.TriplePath;
import org.apache.jena.sparql.core.Var;
import org.apache.jena.sparql.engine.QueryIterator;
import org.apache.jena.sparql.engine.binding.Binding;
import org.apache.jena.sparql.syntax.*;
import org.apache.jena.vocabulary.RDF;
import org.apache.spark.rdd.RDD;
import org.buaa.nlsde.jianglili.cache.CachePool;
import org.buaa.nlsde.jianglili.reasoningquery.QueryRewrting;
import org.buaa.nlsde.jianglili.reasoningquery.conceptExtract.Concept;
import org.json.JSONArray;
import org.json.JSONObject;
import org.mortbay.jetty.HttpStatus;
import org.mortbay.jetty.Request;
import org.mortbay.jetty.Server;
import org.mortbay.jetty.handler.AbstractHandler;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Queue;


public class QueryEntrance extends AbstractHandler {

    private static Concept concept = null;
    private static String memOnEachCore;
    private static JSONObject config;

    private static String currentInstanceFile = "";
    private static String currentSchemaFile = "";

    public static void main(String[] args)
            throws Exception {
        //load config from file
        config = loadJSONFromFile(args[0]);
        if(config==null){return;}

        //setup cluster config
        memOnEachCore = config.getString("MEMORY");
        int serverPort = Integer.parseInt(config.getString("PORT"));

        if(config.getBoolean("TESTMODE")) {
            //test config
            String queryString = config.getString("QUERY4");
            String instanceFile = config.getString("INSTANCE");
            String schemaFile = config.getString("SCHEMA");
            boolean rewriteFlag = false;
            int limNum = 0;
            boolean jenaFlag = true;
            boolean cachePoolFlag = false;

            try {
                concept = QueryRewrting.initSchema("file:" + schemaFile, 0);
                if (jenaFlag) {
                    Dataset instances = DatasetFactory.create(RDFDataMgr.loadModel(instanceFile));
                    querySparql(instances, queryString, rewriteFlag, limNum,config.getString("META"));
                } else {
                    initRuntimeEnvir(instanceFile, memOnEachCore, true, schemaFile);
                    List<SolutionMapping> a = runSPARQLQuery(queryString, concept, limNum, cachePoolFlag, rewriteFlag,config.getString("META"));
                    if(a==null)
                        System.out.println("Result:0");
                    else
                        System.out.println("Result:"+ a.size());
                }
            }
            catch(Exception e) {
                e.printStackTrace();
            }
        }
        else
        {
            //start server
            Server server = new Server(serverPort);
            server.setHandler(new QueryEntrance());
            server.start();
        }
    }

    private static JSONObject loadJSONFromFile(String configFile) {
        JSONObject config = null;
        try {
            String input = FileUtils.readFileToString(new File(configFile));
            config = new JSONObject(input);
        } catch(Exception e) {
            e.printStackTrace();
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
        //get the url and parameters
        String url = httpServletRequest.getRequestURI();
        System.out.println("Receive request"+url);

        httpServletResponse.setHeader("Access-Control-Allow-Origin", "*");
        httpServletResponse.setHeader("Access-Control-Allow-Methods", "GET, POST, DELETE, PUT");
        httpServletResponse.setHeader("Access-Control-Allow-Headers", "X-Requested-With,Content-Type,Accept,Origin");

        boolean rewriteFlag = false;
        boolean jenaFlag = true;
        boolean cachePoolFlag = false;
        int limNum = 0;

        try {
            JSONObject content = new JSONObject(ReadAsChars(httpServletRequest));
            //获取主要参数
            String queryString = content.getString("query");
            String knowledgeGraph = content.getString("graph");//LUBM DBPEDIA
            String instance = content.getString("instance");//LUBM1

            if (queryString == null ||
                knowledgeGraph == null ||
                instance == null) {
                httpServletResponse.setStatus(HttpStatus.ORDINAL_404_Not_Found);
                ((Request)httpServletRequest).setHandled(true);
                return;
            }

            //获取其他参数
            //rewriteFlag = Boolean.valueOf(httpServletRequest.getParameter("REWRITE"));
            //jenaFlag = Boolean.valueOf(httpServletRequest.getParameter("JENA"));
            //cachePoolFlag = Boolean.valueOf(httpServletRequest.getParameter("CACHE"));
            //limNum = Integer.parseInt(httpServletRequest.getParameter("LIM"));

            //根据GRAPH确定使用的schema、instance、meta
            JSONObject selectedGraph = config.getJSONObject("GRAPHS").getJSONObject(knowledgeGraph);
            //如果更换了图谱，需要重新初始化
            String schemaFile;
            if(currentSchemaFile.equals(selectedGraph.getString("SCHEMA"))) {
                schemaFile = currentSchemaFile;
            } else {
                schemaFile = selectedGraph.getString("SCHEMA");
                concept = QueryRewrting.initSchema("file:" + schemaFile, 0);
            }
            //如果更换了instance，需要重新启动
            String instanceFile;
            if(currentInstanceFile.equals(selectedGraph.getJSONObject("INSTANCE").getString(instance))) {
                instanceFile = currentInstanceFile;
            } else {
                instanceFile = selectedGraph.getJSONObject("INSTANCE").getString(instance);
                //closeSparkContext();
                if(!jenaFlag) {
                    initRuntimeEnvir(instanceFile, memOnEachCore, true, schemaFile);
                }
            }
            String metaFile = selectedGraph.getString("META");

            JSONObject jenaResults = null;
            List<SolutionMapping> s2xResults = null;
            if (jenaFlag) {
                Dataset instances = DatasetFactory.create(RDFDataMgr.loadModel(instanceFile));
                //TODO：修改这里
                jenaResults = querySparql(instances, queryString, rewriteFlag, limNum, metaFile);
            } else {
                s2xResults = runSPARQLQuery(queryString, concept, limNum, cachePoolFlag, rewriteFlag, metaFile);
            }

            httpServletResponse.setContentType("application/json;charset=utf-8");
            httpServletResponse.getWriter().println((jenaFlag)?jenaResults:s2xResults);
            httpServletResponse.setStatus(HttpStatus.ORDINAL_200_OK);
            ((Request)httpServletRequest).setHandled(true);
            httpServletResponse.getWriter().close();
        }
        catch(Exception e) {
            e.printStackTrace();
            httpServletResponse.setStatus(HttpStatus.ORDINAL_404_Not_Found);
            ((Request)httpServletRequest).setHandled(true);
        }
    }

    //TODO：添加返回值
    private static JSONObject querySparql(Dataset instances,
                                    String queryString,
                                    Boolean rewriteFlag,
                                    int limNum,
                                    String metaData)
            throws Exception {
        //concept = QueryRewrting.initSchema("file:" + schemaFile, 0);
        Query query =  ReWriteBasedOnStruct(queryString,metaData);
        Op opRoot = Algebra.compile(query) ;
        //System.out.println("opRoot:"+opRoot.toString());
        Op opRootRewrite = (rewriteFlag)?QueryRewrting.transform(opRoot,concept,limNum):null;
        //if(rewriteFlag)
          //  System.out.println("opRewrite:"+opRootRewrite.toString());

        QueryIterator qIter = Algebra.exec((rewriteFlag)?opRootRewrite:opRoot, instances);
        JSONObject results = new JSONObject();
        results.put("Vars",((OpProject)opRoot).getVars());
        ArrayList<String> resultStrings = new ArrayList<>();
        int resultNum = 0;
        for ( ; qIter.hasNext() ; ) {
            Binding b = qIter.nextBinding();
            Iterator<Var> b_var = b.vars();
            StringBuilder sb = new StringBuilder();
            for(;b_var.hasNext();)
            {
                Var temp = b_var.next();
                sb.append(b.get(temp)+"\t");
            }
            resultStrings.add(sb.toString());
            resultNum++;
        }
        results.put("Results",resultStrings);
        qIter.close();
        System.out.println("# original query solution mappings: "+resultNum);
        return results;
    }

    private static void initRuntimeEnvir(String instanceFile,
                                        String executorMem,
                                        Boolean localFlag,
                                        String schemaFile) {
        Const.inputFile_$eq(instanceFile);
        Const.executorMem_$eq(executorMem);
        Const.locale_$eq(localFlag);
        Const.schema_$eq(schemaFile);
        //concept = QueryRewrting.initSchema("file:" + schemaFile, 0);
        SparkFacade.createSparkContext();
        SparkFacade.loadGraph();
    }

    private static void closeSparkContext(){
        SparkFacade.closeContext();
    }

    private static List<SolutionMapping> runSPARQLQuery(String queryString,
                                                     Concept concept,
                                                     int limNum,
                                                     boolean cachePoolFlag,
                                                     boolean rewriteFlag,
                                                     String metaData)
    {
        try {
            (IntermediateResultsModel.getInstance()).clearResults();

            Query query = ReWriteBasedOnStruct(queryString,metaData);
            PrefixMapping prefixes = query.getPrefixMapping();
            Op opRoot = Algebra.compile(query);
            System.out.println("opRoot:"+opRoot.toString());
            Op opRootRewrite = (rewriteFlag) ? QueryRewrting.transform(opRoot, concept, limNum) : null;
            if(rewriteFlag)
                System.out.println("opRootRewrite:"+opRootRewrite);

            AlgebraTranslator trans = new AlgebraTranslator(prefixes);

            if (rewriteFlag) {
                opRootRewrite.visit(new AlgebraWalker(trans));
                if (cachePoolFlag && CachePool.contains(opRootRewrite))
                    return CachePool.getFinalResult(opRootRewrite).toJavaRDD().collect();
            }
            else {
                opRoot.visit(new AlgebraWalker(trans));
                if (cachePoolFlag && CachePool.contains(opRoot))
                    return CachePool.getFinalResult(opRoot).toJavaRDD().collect();
            }

            Queue<SparkOp> q = trans.getExecutionQueue();

            while (!q.isEmpty()) {
                SparkOp actual = q.poll();
                if (cachePoolFlag) {
                    actual.executeCached();
                    continue;
                }
                actual.execute();
            }

            if (cachePoolFlag) {
                if (rewriteFlag) {
                    RDD<SolutionMapping> res = CachePool.getFinalResult(opRootRewrite);
                    return (res != null) ? CachePool.getFinalResult(opRootRewrite).toJavaRDD().collect() : null;
                } else {
                    RDD<SolutionMapping> res = CachePool.getFinalResult(opRoot);
                    return (res != null) ? CachePool.getFinalResult(opRoot).toJavaRDD().collect() : null;
                }
            }
        }
        catch(Exception e)
        {
            e.printStackTrace();
        }
        return IntermediateResultsModel.getInstance().getFinalResultAsList();
    }


    private static Query ReWriteBasedOnStruct(String queryString, String metaData)
            throws Exception
    {
        JSONObject meta = loadJSONFromFile(metaData);
        JSONObject eqCls = meta.getJSONObject("equivalentCls");
        JSONObject eqPrp = meta.getJSONObject("equivalentPrp");
        JSONObject sameas = meta.getJSONObject("sameas");
        JSONObject subCls = meta.getJSONObject("subCls");
        JSONObject subPrp = meta.getJSONObject("subPrp");

        Query query = QueryFactory.create(queryString);
        System.out.println(query);

        ElementGroup element = (ElementGroup) query.getQueryPattern();
        List<Triple> triplesToUnion = new ArrayList<>();

        for(Element el : element.getElements()) {
            if(el instanceof ElementPathBlock) {
                Iterator<TriplePath> triples = ((ElementPathBlock)el).patternElts();
                List<Triple> newTriples = new ArrayList<>();
                while (triples.hasNext()) {
                    TriplePath triple = triples.next();
                    if(triple.isTriple()) {
                        Node s = triple.getSubject();
                        //首先检查meta中是否存在SPO的sameas关系，替换
                        s = getNode(s, sameas);
                        Node p = triple.getPredicate();
                        p = getNode(p, sameas);
                        Node o = triple.getObject();
                        o = getNode(o, sameas);
                        //检查P是否是rdf:type
                        if(p.toString().equals(RDF.type.toString())) {
                            //检查o 是否可以被 equivalentCls和subCls替换
                            o = getNode(o,eqCls);
                            if(!subCls.isNull("<"+o.toString()+">")) {
                                triplesToUnion.add(Triple.create(s,p,o));
                                triples.remove();
                                continue;
                            }

                        } else {
                            //检查p 是否可以被 equivalentPrp和subPrp 替换
                            p = getNode(p,eqPrp);
                            if(!subPrp.isNull("<"+p.toString()+">")) {
                                triplesToUnion.add(Triple.create(s,p,o));
                                triples.remove();
                                continue;
                            }
                        }
                        newTriples.add(Triple.create(s, p, o));
                    }
                    triples.remove();
                }
                for(Triple t : newTriples) {
                    ((ElementPathBlock)el).addTriple(t);
                }
            }
        }

        System.out.println(query);
        //替换subCls与subPrp
        for(Triple triple : triplesToUnion) {
            if(triple.getPredicate().toString().equals(RDF.type.toString())) {//替换o.subcls
                JSONArray subcls = subCls.getJSONArray("<"+triple.getObject().toString()+">");
                ElementUnion eU = new ElementUnion();
                for(int i = 0;i < subcls.length();i++) {
                    String _o = (String) subcls.get(i);
                    Triple pattern = Triple.create(triple.getSubject(),
                                                   triple.getPredicate(),
                                                   NodeFactory.createURI(_o.substring(1,_o.length()-1)));
                    ElementTriplesBlock block = new ElementTriplesBlock();
                    block.addTriple(pattern);
                    eU.addElement(block);
                }
                ElementTriplesBlock block = new ElementTriplesBlock();
                block.addTriple(triple);
                eU.addElement(block);
                element.addElement(eU);
            } else {//替换p,subprp
                JSONArray subprp = subPrp.getJSONArray("<"+triple.getPredicate().toString()+">");
                ElementUnion eU = new ElementUnion();
                for(int i = 0;i < subprp.length();i++) {
                    String _o = (String) subprp.get(i);
                    Triple pattern = Triple.create(triple.getSubject(),
                                                   NodeFactory.createURI(_o.substring(1,_o.length()-1)),
                                                   triple.getObject());
                    ElementTriplesBlock block = new ElementTriplesBlock();
                    block.addTriple(pattern);
                    eU.addElement(block);
                }
                ElementTriplesBlock block = new ElementTriplesBlock();
                block.addTriple(triple);
                eU.addElement(block);
                element.addElement(eU);
            }
        }
        System.out.println(query);
        return query;
    }

    private static Node getNode(Node node, JSONObject jsonObject) {
        if(node.isURI()) {
            if(!jsonObject.isNull("<"+node.toString()+">")) {
                try {
                    String _o = jsonObject.getString("<"+node.toString()+">");
                    node = NodeFactory.createURI(_o.substring(1,_o.length()-1));
                }
                catch(Exception e){ e.printStackTrace(); }
            }
        }
        return node;
    }


    private static String ReadAsChars(HttpServletRequest request)
    {
        BufferedReader br = null;
        StringBuilder sb = new StringBuilder();
        try {
            br = request.getReader();
            String str;
            while ((str = br.readLine()) != null) {
                sb.append(str);
            }
            br.close();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (null != br) {
                try {
                    br.close();
                }
                catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
        return sb.toString();
    }
}
