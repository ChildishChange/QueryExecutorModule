package org.buaa.nlsde.jianglili.reasoningquery;

import com.google.common.collect.Lists;
import org.apache.jena.query.Query;
import org.apache.jena.query.QueryFactory;
import org.apache.jena.sparql.algebra.Algebra;
import org.apache.jena.sparql.algebra.Op;
import org.buaa.nlsde.jianglili.reasoningquery.conceptExtract.Concept;
import org.semanticweb.owlapi.model.OWLOntologyCreationException;
import org.semanticweb.owlapi.model.OWLOntologyStorageException;
import org.task.sparql.rewriter.OptRewriter;
import java.io.File;
import java.io.FileNotFoundException;


/**
 * Created by jianglili on 2016/5/23.
 */
public class MainQueryLubm {
    public static void main(String[] args) throws Exception {
       // String  scehmafile="E://benchmarks//LUBM//univ-benchQL-ub.owl";
        String  scehmafile="/Users/tewisong/Downloads/benchmarks//LUBM//univ-benchQL-ub.owl";
        String  querydir="E:\\benchmarks\\LUBM\\query28-ub";
        Concept concept=QueryRewrting.initSchema("file:"+ scehmafile,0);
        for(File f: new File(querydir).listFiles()) {
            query(f.getAbsolutePath(),concept,1);
        }
    }

    public static void query(String queryfile, Concept concept,Integer qc) throws OWLOntologyCreationException, OWLOntologyStorageException, FileNotFoundException {
        System.out.println(queryfile);
        Query query = QueryFactory.read(queryfile);
        Op opRoot = Algebra.compile(query) ;
        String strOpPre=opRoot.toString();
        System.out.println(opRoot.toString());
        // rewrite the op
        Long tp=System.currentTimeMillis();
        Op opRootRewrite= QueryRewrting.transformDBpedia(opRoot, concept,false,0);


//        Element element=new AlgebraTransformer().transform(opRootRewrite);
//        query.setQueryPattern(element);
        String strOpAfter=opRootRewrite.toString();

        System.out.println(opRootRewrite.toString());
        //  System.out.println(queryfile.substring(queryfile.lastIndexOf("\\")+1)+":"+strOpPre.equals(strOpAfter)+":time:"+(System.currentTimeMillis()-tp));
        //   System.out.println(queryfile.substring(queryfile.lastIndexOf("\\")+1));
        System.out.println(strOpPre.equals(strOpAfter)?"否":"是");
        //     System.out.println((System.currentTimeMillis()-tp));


        Op opRootReRewrite=OptRewriter.optimize(opRootRewrite);
        String strOpAfterAfter=opRootReRewrite.toString();
        System.out.println("opRootReRewrite:");
        System.out.println(opRootReRewrite.toString());
        System.out.println(strOpAfter.equals(strOpAfterAfter)?"否":"是");

//        Op opRootReReRewrite= QueryReReWriting.transformDBpedia(opRoot, concept,false);
//        String strOpAfter3=opRootReReRewrite.toString();
//        System.out.println(opRootReReRewrite.toString());
//        System.out.println(strOpAfter.equals(strOpAfter3)?"3否":"3是");

    }

}
