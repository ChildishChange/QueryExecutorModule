package org.buaa.nlsde.jianglili.utils.uba.src.edu.lehigh.swat.benth.uba;

import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;

import java.io.*;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 * Created by jianglili on 2016/4/27.
 */
public class OwlToNt4 {

    public static void main(String[] args) throws FileNotFoundException {
        int countpre = 0;
        File path = new File("e://benchmarks//LUBM//datasets//dl-rdf//");
//        LinkedList<File> list = new LinkedList<File>();
        Map<String, Integer> universities = new LinkedHashMap<String, Integer>();
        Map<String, List<File>> universitiesfile = new LinkedHashMap<String, List<File>>();
        if (path.exists()) {
            File[] files = path.listFiles();
            for (File file : files) {
                if (!file.isDirectory() && file.toString().endsWith("owl")) {
                    countpre++;
//                    list.add(file);
                    String[] universityEntry = file.getName().substring(file.getName().indexOf('y') + 1, file.getName().lastIndexOf('.')).split("_");
                    String university = universityEntry[0];
//                    String universitynumber=universityEntry[1];
                    if (universities.get(university) == null) {
                        universities.put(university, 1);
                        universitiesfile.put(university, new LinkedList<File>());
                    } else
                        universities.put(university, universities.get(university) + 1);
                    List<File> universityfiles = universitiesfile.get(university);
                    universityfiles.add(file);
//                    System.out.println(file);
                }
            }
        }
        System.out.println("文件个数:" + countpre);
        System.out.println("university个数:" + universities.keySet().size());
//        for (String key : universities.keySet()) {
//            System.out.println(key + ":" + (universities.get(key)==universitiesfile.get(key).size()));
////            for (File file : universitiesfile.get(key)) {
////                System.out.println(file.getName());
////            }
//        }

        Model modelT = ModelFactory.createDefaultModel();
        int countfile=0;
        int countUniver=2000 ;
        String fileOut = "e://benchmarks//LUBM//datasets//nt//Universities_" + countUniver + ".nt";
        PrintStream printStream = new PrintStream(new File(fileOut));
        for(int i=0;i<countUniver;i++){
            for(File file:universitiesfile.get(((Integer) i).toString())) {
                InputStream in = new FileInputStream(file.getAbsoluteFile());
                Model model = ModelFactory.createDefaultModel();
                model.read(in, "RDF/XML");
                modelT.add(model);
                modelT.write(printStream, "N-TRIPLES");
                modelT.remove(model);
                model.close();
                countfile++;
            }
            System.out.println("university:"+i+"numbers;"+universitiesfile.get(((Integer) i).toString()).size());
        }
        System.out.println(countfile);

    }
}
