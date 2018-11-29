package org.buaa.nlsde.jianglili.utils;

import java.io.*;
import java.nio.charset.Charset;

/**
 * Created by jianglili on 2017/2/4.
 */
public class urlReplace {

    public static void main(String[] args) throws IOException {
        String in="E://benchmarks//LUBM//univ-benchQL.owl";
        String out="E://benchmarks//LUBM//univ-benchQL-2.owl";
        File filein=new File(in);


        InputStream fis = new FileInputStream(filein);
        InputStreamReader isr = new InputStreamReader(fis, Charset.forName("UTF-8"));
        BufferedReader br = new BufferedReader(isr);



        File outq=new File(out);
        OutputStream fos = new FileOutputStream(outq);
        OutputStreamWriter osr = new OutputStreamWriter(fos, Charset.forName("UTF-8"));
        BufferedWriter bw = new BufferedWriter(osr);
        String line;

        while ((line=br.readLine())!=null) {
            System.out.println(line);
            line = line.replace("http://swat.cse.lehigh.edu/onto/univ-bench.owl",
                    "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl");
            bw.write(line + "\n");
            System.out.println(line);
        }
        bw.close();

    }
}
