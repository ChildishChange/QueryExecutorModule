package org.buaa.nlsde.jianglili.reasoningquery.conceptExtract;


import org.apache.jena.rdf.model.Model;
import org.semanticweb.owlapi.model.OWLClass;
import org.semanticweb.owlapi.model.OWLEntity;
import org.semanticweb.owlapi.model.OWLOntology;
import org.semanticweb.owlapi.model.OWLProperty;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;


/**
 * Created by jianglili on 2016/3/24.
 */
public class Concept {
    //class concept in Op
    private List<OWLClass> classes=new ArrayList<>();
    //Properties concept in Op
    private List<OWLProperty> properties=new ArrayList<>();
    //Both class  and properties
    private Set<OWLEntity> objects=new HashSet<>();
    //the module ontology
    private OWLOntology ontology;
    //the schema graph
    private Model model;

    public Model getModel() {
        return model;
    }

    public void setModel(Model model) {
        this.model = model;
    }
    public Concept() {
    }
    // the module classifier
//    private IncrementalClassifier classifier;

    public boolean addClass(OWLClass ontClass)
    {
        synchronized (this)
        {
            classes.add(ontClass);
            objects.add(ontClass);
        }
       return true;
    }

    public List<OWLClass> getClasses() {
        return classes;
    }
    public boolean addProperty(OWLProperty property)
   {
       synchronized (this)
       {
           properties.add(property);
           objects.add(property);
       }
       return true;

   }

    public List<OWLProperty> getProperties() {
        return properties;
    }

    public Set<OWLEntity> getEntities() {     return objects;   }


//    public IncrementalClassifier getClassifier() {
//        return classifier;
//    }
//
//    public void setClassifier(IncrementalClassifier classifier) {
//        this.classifier = classifier;
//    }

    public OWLOntology getOntology() {
        return ontology;
    }

    public void setOntology(OWLOntology ontology) {

        this.ontology = ontology;
    }

}
