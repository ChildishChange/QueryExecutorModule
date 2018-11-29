package org.buaa.nlsde.jianglili.utils.uba.src.edu.lehigh.swat.benth.uba;

import java.io.*;

/**
 * Created by jianglili on 2016/6/19.
 */
public class PrefixReplace {

    public static void main(String[] args) throws IOException {
        long count=  Replace("http://swat.cse.lehigh.edu/onto/univ-bench-dl.owl","http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl");
        System.out.println(count);
    }
    public static long Replace(String pre, String after) throws IOException {
        File pathin = new File("E:\\benchmarks\\LUBM\\datasets\\nt\\Universities_2000.nt");
        FileReader fri = new FileReader(pathin);
        BufferedReader in = new BufferedReader(fri); // 在定义好的流基础上套接一个处理流，用于更加效率的读取文件（一行一行的读取）
        String line=in.readLine();
        long count = 1; // 用于统计行数，从1开

        File pathout = new File("E:\\benchmarks\\LUBM\\datasets\\nt\\Universities_2000_new.nt");
        FileWriter frw = new FileWriter(pathout);
        BufferedWriter out=new BufferedWriter(frw);
        while(line!=null) { // readLine()方法是按行读的，返回值是这行的内容
            line = line.replaceAll(pre,after);
            out.write(line+"\n");
            line=in.readLine();         count++; // 每读一行，则变量x累加1
        }
        in.close();
        out.close();
        return count;
    }
}
