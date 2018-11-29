package org.buaa.nlsde.jianglili.reasoningquery.conceptExtract;

import com.clarkparsia.owlapiv3.OWL;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.NodeFactory;
import org.semanticweb.owlapi.model.*;

import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * the main purpose of the class is wrapper the methods of ConceptUtil
 * Created by jianglili on 2016/3/25.
 */

public class ConceptUtilNodeWrapper {

    private static ConceptUtil conceptUtil;
    private static ConceptUtilNodeWrapper singleton=new ConceptUtilNodeWrapper();
    private  ConceptUtilNodeWrapper()    {}

    // used by algebraTransformer
    public static ConceptUtilNodeWrapper getSingleton(Concept concept,Boolean extractFlag) throws OWLOntologyCreationException, OWLOntologyStorageException, FileNotFoundException {
        //init to conceptUtil parameters
        conceptUtil= ConceptUtil.getSingleton(concept) ;
        conceptUtil.generateReasoning(concept,extractFlag);
        return singleton;
    }
    // used by algebraTransformer
    public static ConceptUtilNodeWrapper getSingleton() throws OWLOntologyCreationException {
        //init to conceptUtil parameters
        return singleton;
    }

    public static ConceptUtil  getConceptUtil() {
        return conceptUtil;
    }

    public void setConceptUtil(ConceptUtil conceptUtil) {
        this.conceptUtil = conceptUtil;
    }
    //Heuristics methods to get the replacenode if  equivalent calss is null then return the subcalss
    public Set<Node>  getReplaceNodeOfClass(Node node)
    {
        OWLClass ontclass= OWL.Class(node.toString());
        Set<OWLClass>  owlClasses=conceptUtil.getEquivalentClasses(ontclass);
        if(owlClasses.size()!=0)
            return TransformClassToNode(owlClasses);
        else
           return TransformClassToNode(conceptUtil.getSubClasses(ontclass));
    }
    //Heuristics methods to get the replacenode if  equivalent calss is null then return the subcalss
    public Set<Node>  getReplaceNodeOfSubClass(Node node)
    {
        OWLClass ontclass= OWL.Class(node.toString());
         return TransformClassToNode(conceptUtil.getSubClasses(ontclass));
    }
    //Heuristics methods to get the replacenode if  equivalent calss is null then return the subcalss
    public Set<Node>  getReplaceNodeOfSuperClass(Node node)
    {
        OWLClass ontclass= OWL.Class(node.toString());
        return TransformClassToNode(conceptUtil.getSubClasses(ontclass));
    }

    /**
     * get the join equilvalent  classes
     * @param node
     * @return
     */
    public Set<Set<Node>>  getReplaceNodeOfJoinEquivalentClass(Node node)
    {
        OWLClass ontclass= OWL.Class(node.toString());
        Set<Set<OWLClass>>  owlClasses=conceptUtil.getEquivalentJoinClasses(ontclass);
        return TransformJoinClassToNode(owlClasses);

    }
    //Heuristics methods to get the replacenode
    // first judge if the properties is object or data
    // if  equivalent calss is null then return the subclass
    public Set<Node>  getReplaceNodeOfSubProperties(Node node)   {

        if(conceptUtil.IsObjectProperty(node))
        {
            OWLObjectProperty ontObjectProperty= OWL.ObjectProperty(node.toString());
            Set<OWLObjectProperty> owlObjectProperties=conceptUtil.getEquivalentObjectProperties(ontObjectProperty);
            if(owlObjectProperties.size()!=0)
                return TransformObjectPropertyToNode(owlObjectProperties);
            else
                return TransformObjectPropertyToNode(conceptUtil.getSubObjectProperties(ontObjectProperty));
        }
        else if(conceptUtil.IsDataProperty(node))
        {
            OWLDataProperty ontDataProperty= OWL.DataProperty(node.toString());
            Set<OWLDataProperty> owlDataProperties=conceptUtil.getEquivalentDataProperties(ontDataProperty);
            if(owlDataProperties!=null)
                return TransformDataPropertyToNode(owlDataProperties);
            else
                return TransformDataPropertyToNode(conceptUtil.getSubDataProperties(ontDataProperty));
        }
        return null;
    }
    //Heuristics methods to get the replacenode of role chain
    // first judge if the properties is object or data
    // if  equivalent calss is null then return the subcalss
    public Set<List<Node>> getRoleObjectProperties(Node node)    {
        if(conceptUtil.IsObjectProperty(node)){
            OWLObjectProperty ontObjectProperty= OWL.ObjectProperty(node.toString());
            Set<List<OWLObjectPropertyExpression>> axioms = conceptUtil.getRoleObjectProperties(ontObjectProperty);
            return TransformRoleObjectPropertyExpressionToNode(axioms);
        }
        return  null;
    }

    /**
     * get the join inverseOf property
     * @param node
     * @return
     */
    public Set<Node>  getReplaceInverseOfProperty(Node node)
    {
        OWLObjectProperty ontObjectProperty= OWL.ObjectProperty(node.toString());
        Set<OWLObjectProperty>  objectInverseProperties=conceptUtil.getInverseProperties(ontObjectProperty);
        return TransformObjectPropertyToNode(objectInverseProperties);

    }
     //judge if the node object in a triple is class
    public  boolean IsOWLClass(Node object)
    {
        return conceptUtil.IsOWLClass(object);
    }
    // from set<object>  to set<node>
    public Set<Node> TransformClassToNode(Set<OWLClass> ontclasses)
    {
        Set<Node> nodes=new HashSet<Node>();
        for(OWLClass ontclass:ontclasses)
            nodes.add( NodeFactory.createURI(ontclass.getIRI().toString()));
        return nodes;
    }
    // from set<object>  to set<node>
    public Set<Set<Node>> TransformJoinClassToNode(Set<Set<OWLClass>> ontjoinclasses)
    {
        Set<Set<Node>> nodeJoinSets=new HashSet<>();
        for(Set<OWLClass> ontclasses:ontjoinclasses) {
            Set<Node> nodes=new HashSet<Node>();
            for (OWLClass ontclass : ontclasses)
                nodes.add(NodeFactory.createURI(ontclass.getIRI().toString()));
            if(nodes.size()!=0)
                nodeJoinSets.add(nodes);
        }
        return nodeJoinSets;
    }
    // from set<OWLObjectPropertyExpression>  to set<node>
    public Set<List<Node>> TransformRoleObjectPropertyExpressionToNode(Set<List<OWLObjectPropertyExpression>> owlObjectPropertyExpressions )
    {
        Set<List<Node>> nodeListSet=new HashSet<List<Node>>();
        for(List<OWLObjectPropertyExpression> ontObjectPropertyList:owlObjectPropertyExpressions) {
            List<Node> nodeList=new ArrayList<>();
            for (OWLObjectPropertyExpression ontObjectProperty :ontObjectPropertyList)
                nodeList.add(NodeFactory.createURI(((OWLObjectProperty) ontObjectProperty).getIRI().toString()));
            nodeListSet.add(nodeList);
        }
        return nodeListSet;
    }
    // from set<object>  to set<node>
    public Set<Node> TransformObjectPropertyToNode(Set<OWLObjectProperty> owlObjectProperties )
    {
        Set<Node> nodes=new HashSet<Node>();
        for(OWLObjectProperty ontObjectProperty:owlObjectProperties)
            nodes.add( NodeFactory.createURI(ontObjectProperty.getIRI().toString()));
        return nodes;
    }
    // from set<object>  to set<node>
    public Set<Node> TransformDataPropertyToNode(Set<OWLDataProperty> owlDataProperties )
    {
        Set<Node> nodes=new HashSet<Node>();
        for(OWLDataProperty ontDataProperty:owlDataProperties)
            nodes.add(NodeFactory.createURI(ontDataProperty.getIRI().toString()));
        return nodes;
    }


}
