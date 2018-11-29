package org.buaa.nlsde.jianglili.utils.lubm;

import com.google.common.collect.Lists;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import static java.util.stream.Collectors.joining;

/**
 * Created by jianglili on 2016/5/23.
 */
public class queryFiles {

    public static void main(String[] args) throws IOException {
        String  queryfile="d://Users//git//iswc2014-benchmark-master//Ontop//OntopQueries.txt";
        String queries = Files.lines(Paths.get(queryfile), StandardCharsets.UTF_8).collect(joining("\n"));
        Iterator<String> querysPre= Arrays.asList(queries.split("#Query\\d+(\\s|\\w)*\\n\\t")).stream().filter(s -> s.startsWith("SELECT"))
                .map(s -> "PREFIX ub:<http://swat.cse.lehigh.edu/onto/univ-bench.owl#>\n" +
                        "PREFIX rdf:   <http://www.w3.org/1999/02/22-rdf-syntax-ns#>\n" +
                        "PREFIX owl:   <http://www.w3.org/2002/07/owl#>\n" +
                        "PREFIX xsd:   <http://www.w3.org/2001/XMLSchema#>\n" +
                        "PREFIX rdfs:  <http://www.w3.org/2000/01/rdf-schema#>\n"+s)
                .map(s -> s.replace("\n"," ").replace("\t","")).iterator();
        Iterator<String> querysKey= Arrays.asList(queries.split("#")).stream().filter(s-> s.startsWith("Query\\d+(\\s|\\w)*\\n\\t"))
                .map(s->((s.substring(5,9).split("(\\s|\\w)*"))[0])).iterator();

        List<String> querys= Lists.newArrayList(querysPre);

    }
}
