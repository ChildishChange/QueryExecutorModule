package org.buaa.nlsde.jianglili.utils;

import java.util.*;

/**
 * Created by jianglili on 2016/2/28.
 */
public class queryLists {
    public static String LB = System.getProperty("line.separator");

    public static void main(String[] args)  {

//        org.semanticweb.HermiT.sparql.BenchmarkSPARQLOWL.main();

        HashMap queryID2query = new HashMap();
        String queryString = "SELECT ?x WHERE { " + LB + "?x rdf:type ub:GraduateStudent. " + LB + "?x ub:takesCourse <http://www.Department0.University0.edu/GraduateCourse0>. " + LB + "?x rdf:type owl:NamedIndividual. }" + LB;
        queryID2query.put("Query 01", queryString);
        queryString = "SELECT ?x ?y ?z WHERE { " + LB + "?x rdf:type ub:GraduateStudent. " + LB + "?y rdf:type ub:University. " + LB + "?z rdf:type ub:Department. " + LB + "?x ub:memberOf ?z. " + LB + "?z ub:subOrganizationOf ?y. " + LB + "?x ub:undergraduateDegreeFrom ?y." + LB + "?x rdf:type owl:NamedIndividual. " + LB + "?y rdf:type owl:NamedIndividual. " + LB + "?z rdf:type owl:NamedIndividual. }" + LB;
        queryID2query.put("Query 02", queryString);
        queryString = "SELECT ?x WHERE { " + LB + "?x rdf:type ub:Publication. " + LB + "?x ub:publicationAuthor <http://www.Department0.University0.edu/AssistantProfessor0>.} " + LB;
        queryID2query.put("Query 03", queryString);
        queryString = "SELECT ?x ?y1 ?y2 ?y3 WHERE { " + LB + "?x rdf:type ub:Professor. " + LB + "?x ub:worksFor <http://www.Department0.University0.edu>. " + LB + "?x ub:name ?y1. " + LB + "?x ub:emailAddress ?y2. " + LB + "?x ub:telephone ?y3. }" + LB;
        queryID2query.put("Query 04", queryString);
        queryString = "SELECT ?x WHERE { " + LB + "?x rdf:type ub:Person. " + LB + "?x ub:memberOf <http://www.Department0.University0.edu>. }" + LB;
        queryID2query.put("Query 05", queryString);
        queryString = "SELECT ?x WHERE { " + LB + "?x rdf:type ub:Student. }" + LB;
        queryID2query.put("Query 06", queryString);
        queryString = "SELECT ?x ?y WHERE { " + LB + "?x rdf:type ub:Student. " + LB + "?y rdf:type ub:Course. " + LB + "?x ub:takesCourse ?y. " + LB + "<http://www.Department0.University0.edu/AssociateProfessor0> ub:teacherOf ?y. }" + LB;
        queryID2query.put("Query 07", queryString);
        queryString = "SELECT ?x ?y ?z WHERE { " + LB + "?x rdf:type ub:Student. " + LB + "?y rdf:type ub:Department. " + LB + "?x ub:memberOf ?y. " + LB + "?y ub:subOrganizationOf <http://www.University0.edu>." + "?x ub:emailAddress ?z." + LB + "?x rdf:type owl:NamedIndividual. " + LB + "?y rdf:type owl:NamedIndividual. }" + LB;
        queryID2query.put("Query 08", queryString);
        queryString = "SELECT ?x ?y ?z WHERE { " + LB + "?x rdf:type ub:Student. " + LB + "?y rdf:type ub:Faculty. " + LB + "?z rdf:type ub:Course. " + LB + "?x ub:advisor ?y. " + LB + "?y ub:teacherOf ?z. " + LB + "?x ub:takesCourse ?z. " + LB + "?x rdf:type owl:NamedIndividual. " + LB + "?y rdf:type owl:NamedIndividual. " + LB + "?z rdf:type owl:NamedIndividual. }" + LB;
        queryID2query.put("Query 09", queryString);
        queryString = "SELECT ?x WHERE { " + LB + "?x rdf:type ub:Student. " + LB + "?x ub:takesCourse <http://www.Department0.University0.edu/GraduateCourse0>." + LB + "?x rdf:type owl:NamedIndividual. }" + LB;
        queryID2query.put("Query 10", queryString);
        queryString = "SELECT ?x WHERE { " + LB + "?x rdf:type ub:ResearchGroup. " + LB + "?x ub:subOrganizationOf <http://www.University0.edu>." + LB + "?x rdf:type owl:NamedIndividual. }" + LB;
        queryID2query.put("Query 11", queryString);
        queryString = "SELECT ?x ?y WHERE { " + LB + "?x rdf:type ub:Chair. " + LB + "?y rdf:type ub:Department. " + LB + "?x ub:worksFor ?y. " + LB + "?y ub:subOrganizationOf <http://www.University0.edu>." + LB + "?x rdf:type owl:NamedIndividual. " + LB + "?y rdf:type owl:NamedIndividual. }" + LB;
        queryID2query.put("Query 12", queryString);
        queryString = "SELECT ?x WHERE { " + LB + "?x rdf:type ub:Person. " + LB + "<http://www.University0.edu> ub:hasAlumnus ?x." + LB + "?x rdf:type owl:NamedIndividual. }" + LB;
        queryID2query.put("Query 13", queryString);
        queryString = "SELECT ?x WHERE { " + LB + "?x rdf:type ub:UndergraduateStudent. }" + LB;
        queryID2query.put("Query 14", queryString);
        for(Object entry:queryID2query.entrySet()){
            System.out.println(((Map.Entry<String,String>)entry).getKey());
            System.out.println(((Map.Entry<String, String>) entry).getValue());
//            System.out.println(((Map.Entry<String,String>)entry));
        }
//        for(Object entry:queryID2query.values()){
//            System.out.println(entry);
//        }
    }

    public static void main2(String[] args) {
        new HashMap();
        HashMap queryID2query = new HashMap();
        String queryString = "SELECT ?x WHERE { " + LB + "g:Infection rdfs:subClassOf  _:o. " + LB + "_:o rdf:type owl:Restriction. " + LB + "_:o owl:onProperty g:HasCausalLinkTo. " + LB + "_:o owl:someValuesFrom ?x. }" + LB;
        queryID2query.put("Query 1", queryString);
        queryString = "SELECT ?x ?y WHERE { " + LB + "g:Infection rdfs:subClassOf  _:o. " + LB + "_:o rdf:type owl:Restriction. " + LB + "_:o owl:onProperty ?y. " + LB + "_:o owl:someValuesFrom ?x. " + LB + " ?y rdf:type owl:ObjectProperty. }" + LB;
        queryID2query.put("Query 2", queryString);
        queryString = "SELECT ?x ?y WHERE { " + LB + "?x rdfs:subClassOf [" + LB + "   a owl:Class ; " + LB + "      owl:intersectionOf (" + LB + "         g:Infection " + LB + "         [" + LB + "            a owl:Restriction ;" + LB + "            owl:onProperty g:hasCausalAgent ; " + LB + "            owl:someValuesFrom ?y " + LB + "         ]" + LB + "      ) " + LB + "] . }" + LB;
        queryID2query.put("Query 3", queryString);
        queryString = "SELECT ?x ?y ?z WHERE { " + LB + "g:NAMEDLigament rdfs:subClassOf [" + LB + "   a owl:Class ; " + LB + "      owl:intersectionOf (" + LB + "         ?x " + LB + "         g:NAMEDInternalBodyPart" + LB + "      ) " + LB + "] . " + LB + "?x rdfs:subClassOf [" + LB + "   a owl:Restriction ; " + LB + "   owl:onProperty g:hasShapeAnalagousTo ; " + LB + "   owl:someValuesFrom [" + LB + "      a owl:Class ;" + LB + "      owl:intersectionOf (" + LB + "         ?y " + LB + "         [" + LB + "            a owl:Restriction ;" + LB + "            owl:onProperty ?z ; " + LB + "            owl:someValuesFrom g:linear " + LB + "         ]" + LB + "      ) " + LB + "   ]" + LB + "] . }" + LB;
        queryID2query.put("Query 4", queryString);
        queryString = "SELECT ?x ?y ?z ?w WHERE { " + LB + "?x rdfs:subClassOf g:NonNormalCondition. " + LB + "?z rdfs:subPropertyOf g:ModifierAttribute." + LB + "?x rdfs:subClassOf [" + LB + "   a owl:Restriction ; " + LB + "   owl:onProperty ?y ; " + LB + "   owl:someValuesFrom g:Status]. " + LB + "g:Bacterium rdfs:subClassOf [" + LB + "   a owl:Restriction ; " + LB + "   owl:onProperty ?z ; " + LB + "   owl:someValuesFrom ?w]. " + LB + "?z rdf:type owl:ObjectProperty ." + LB + "?w rdfs:subClassOf g:AbstractStatus. " + LB + "?y rdfs:subPropertyOf g:StatusAttribute. }" + LB;
        queryID2query.put("Query 5", queryString);
        for(Object entry:queryID2query.entrySet()){
            System.out.println(((Map.Entry<String,String>)entry).getKey());
            System.out.println(((Map.Entry<String, String>) entry).getKey());
//            System.out.println(((Map.Entry<String,String>)entry).getValue());
        }
//        for(Object entry:queryID2query.values()){
//            System.out.println(entry);
//        }
    }


}
