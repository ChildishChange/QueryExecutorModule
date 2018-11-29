package org.buaa.nlsde.jianglili.reasoningquery.tripleUtils;

import org.apache.jena.graph.Node;
import org.apache.jena.graph.Triple;
import org.apache.jena.sparql.core.BasicPattern;
import org.apache.jena.sparql.core.Var;
import org.apache.jena.vocabulary.OWL;
import org.apache.jena.vocabulary.RDF;
import org.apache.jena.vocabulary.RDFS;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by jianglili on 2016/3/25.
 */
public class TripleUtil {

    // one var return true teo true false
    public static int varOfTriple(Triple triple){
        int count=(triple.getSubject().isVariable()?1:0)
                //subject is var
                +(triple.getPredicate().isVariable()?2:0)
                //predict is var
                +(triple.getObject().isVariable()?4:0);
        //object is var
        return count;
    }
    //judge if the predict is rdf:type
    public static boolean PredictIsType(Triple triple)
    {
        Node predict=triple.getPredicate();
        return predict.equals(RDF.Nodes.type);
    }
    //judge if the predict is var
    public static boolean PredictIsVar(Triple triple)
    {
        Node predict=triple.getPredicate();
        return predict.isVariable();
    }

    //judge if the predict is rdf:type
    public static boolean PredictIsSubProperty(Triple triple)
    {
        Node predict=triple.getPredicate();
        return predict.equals(RDFS.Nodes.subPropertyOf);
    }

    //judge if the predict is rdf:type
    public static boolean PredictIsSubClass(Triple triple)
    {
        Node predict=triple.getPredicate();
        return predict.equals(RDFS.Nodes.subClassOf);
    }
    //judge if the predict is rdf:type
    public static boolean PredictIsDomain(Triple triple)
    {
        Node predict=triple.getPredicate();
        return predict.equals(RDFS.Nodes.domain);
    }
    public static boolean PredictIsRange(Triple triple)
    {
        Node predict=triple.getPredicate();
        return predict.equals(RDFS.Nodes.range);
    }

    public static boolean PredictIsEquivalentClass(Triple triple){
        Node predict=triple.getPredicate();
        return predict.equals(OWL.equivalentClass.asNode());
    }

    //judge if the predict is rdf:type
    public static boolean PredictIsAxiom(Triple triple, List<Node> nodeReplace)
    {
        if(PredictIsSubClass(triple)){
            addTripleResult(triple,nodeReplace);
            return true;
        }

        else if(PredictIsSubProperty(triple)) {
            addTripleResult(triple,nodeReplace);
            return true;
        }
        else if(PredictIsSubClass(triple)) {
            addTripleResult(triple,nodeReplace);
            return true;
        }
        else  if(PredictIsEquivalentClass(triple)) {
            addTripleResult(triple,nodeReplace);
            return true;
        }
        else if(PredictIsDomain(triple)) {
            return true;
        }
        else if(PredictIsRange(triple)) {
            return true;
        }
        else
            return false;
    }
    // add result for triple axiom
    public static void addTripleResult(Triple triple, List<Node> nodeReplace){
        if(triple.getSubject().isConcrete())
            nodeReplace.add(triple.getSubject());
        else if(triple.getObject().isConcrete())
            nodeReplace.add(triple.getObject());
    }
    //judge if the triple is ?y rdf  ObjectProperty
    public static boolean IsTypePropertyOrClass(Triple triple, BasicPattern  basicPattern)
    {
           if(triple.getObject().equals(OWL.ObjectProperty.asNode())&&triple.getPredicate().equals(RDF.Nodes.type)&&triple.getSubject().isVariable())
                return true;
           else if (triple.getObject().equals(OWL.DatatypeProperty.asNode())&&triple.getPredicate().equals(RDF.Nodes.type)&&triple.getSubject().isVariable())
               return  true;
           else if (triple.getObject().equals(OWL.Class.asNode())&&triple.getPredicate().equals(RDF.Nodes.type)&&triple.getSubject().isVariable())
               return  true;
           else
               return false;
    }

    public static Boolean containsVar(Triple triple,Var var){
        return triple.getSubject().equals(var)
                || triple.getPredicate().equals(var)
                || triple.getObject().equals(var);
    }

    // get the var list of the triples
    public static List<Var> getVars(Triple triple){
        List<Var> varList=new ArrayList<>();
      if(triple.getSubject().isVariable()){
          varList.add((Var)triple.getSubject());
      }
      if (triple.getPredicate().isVariable()){
          varList.add((Var)triple.getPredicate());
      }
      if(triple.getObject().isVariable()){
            varList.add((Var)triple.getObject());
      }
        return varList;
    }

}
