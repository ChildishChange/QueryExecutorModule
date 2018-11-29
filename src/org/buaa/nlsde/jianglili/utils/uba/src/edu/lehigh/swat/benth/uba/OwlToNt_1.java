package org.buaa.nlsde.jianglili.utils.uba.src.edu.lehigh.swat.benth.uba;

import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;

import java.io.*;

/**
 * Created by jianglili on 2017/2/3.
 */
public class OwlToNt_1 {
    public static void main(String[] args) throws FileNotFoundException {
        File file = new File("E:\\benchmarks\\LUBM\\datasets\\dl-rdf\\University0_0.owl");
        Model modelT = ModelFactory.createDefaultModel();
        String fileOut = "E:\\benchmarks\\LUBM\\datasets\\nt1-1\\Universities_1.nt";
        PrintStream printStream = new PrintStream(new File(fileOut));

        InputStream in = new FileInputStream(file.getAbsoluteFile());
        Model model = ModelFactory.createDefaultModel();
        model.read(in, "RDF/XML");
        modelT.add(model);
        modelT.write(printStream, "N-TRIPLES");
        modelT.remove(model);
        model.close();


    }
}
