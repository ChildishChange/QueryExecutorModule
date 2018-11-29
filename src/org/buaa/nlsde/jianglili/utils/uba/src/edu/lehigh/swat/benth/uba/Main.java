package org.buaa.nlsde.jianglili.utils.uba.src.edu.lehigh.swat.benth.uba;

 /**
 * Created by jianglili on 2016/4/27.
 */
public class Main {

    public static void main(String[] args) {
        new Generator().start(1000, 0, 0, false, "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl","E:\\benchmarks\\LUBM\\datasets\\dl-rdf");
//        new Generator().start(2000, 0, 0, false, "http://swat.cse.lehigh.edu/onto/univ-bench-dl.owl",
//                "E:\\benchmarks\\LUBM\\datasets\\dl-rdf");
//        new Generator().start(1000, 0, 0, false, "file: E:\\benchmarks\\LUBM\\univ-benchQL.owl");
    }
}
