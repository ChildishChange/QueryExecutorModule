package org.buaa.nlsde.jianglili.utils;

import com.esotericsoftware.kryo.io.Output;
//import groovy.sql.OutParameter;

import java.io.*;
import java.nio.charset.Charset;

/**
 * Created by jianglili on 2017/2/3.
 */
public class ubparse {

    public static void main(String[] args) throws IOException {
        String dir="E:\\benchmarks\\LUBM\\query28";
        String out="E:\\benchmarks\\LUBM\\query28-ub";
        File dirfile=new File(dir);
        for(File qf:dirfile.listFiles())
        {
            System.out.println(qf.getName());
            InputStream fis = new FileInputStream(qf);
            InputStreamReader isr = new InputStreamReader(fis, Charset.forName("UTF-8"));
            BufferedReader br = new BufferedReader(isr);



            File outq=new File(out+"\\"+qf.getName());
            OutputStream fos = new FileOutputStream(outq);
            OutputStreamWriter osr = new OutputStreamWriter(fos, Charset.forName("UTF-8"));
            BufferedWriter bw = new BufferedWriter(osr);
            String line;
            while ((line=br.readLine())!=null) {
                System.out.println(line);
                 line = line.replace("<http://swat.cse.lehigh.edu/onto/univ-bench.owl#>",
                         " <http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#>");
                 bw.write(line + "\n");
                System.out.println(line);
            }
            bw.close();
        }
    }
}
