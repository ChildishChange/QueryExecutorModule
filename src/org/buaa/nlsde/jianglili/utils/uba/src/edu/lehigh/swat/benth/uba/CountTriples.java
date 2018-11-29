package org.buaa.nlsde.jianglili.utils.uba.src.edu.lehigh.swat.benth.uba;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 * Created by jianglili on 2016/6/17.
 */
public class CountTriples {

    public static void main(String[] args) throws IOException {
       long count=  getTextLines("E:\\benchmarks\\LUBM\\datasets\\nt\\Universities_100.nt");
        System.out.println("universities100:"+count);
        long numberfiles=  getNumberFiles(100);
        System.out.println(numberfiles);
//        long count2=  getTextLines("E:\\benchmarks\\LUBM\\datasets\\nt\\Universities_500.nt");
//        System.out.println(count2);
//        long count3=  getTextLines("E:\\benchmarks\\LUBM\\datasets\\nt\\Universities_1000.nt");
//        System.out.println(count3);
//        long count4=  getTextLines("E:\\benchmarks\\LUBM\\datasets\\nt\\Universities_2000.nt");
//        System.out.println(count4);
    }
    public static long getTextLines(String file) throws IOException {
        String path = file ;// 定义文件路径
        FileReader fr = new FileReader(path); //这里定义一个字符流的输入流的节点流，用于读取文件（一个字符一个字符的读取）
        BufferedReader br = new BufferedReader(fr); // 在定义好的流基础上套接一个处理流，用于更加效率的读取文件（一行一行的读取）
        long x = 0; // 用于统计行数，从0开始
        while(br.readLine() != null) { // readLine()方法是按行读的，返回值是这行的内容
            x++; // 每读一行，则变量x累加1
        }
        return x; //返回总的行数
    }
    public static long getNumberFiles(Integer  number) throws IOException {
        File path = new File("e://benchmarks//LUBM//datasets//dl-rdf//");
        Map<String, Integer> universities = new LinkedHashMap<String, Integer>();
        if (path.exists()) {
            File[] files = path.listFiles();
            for (File file : files) {
                if (!file.isDirectory() && file.toString().endsWith("owl")) {

//                    list.add(file);
                    String[] universityEntry = file.getName().substring(file.getName().indexOf('y') + 1, file.getName().lastIndexOf('.')).split("_");
                    String university = universityEntry[0];
//                    String universitynumber=universityEntry[1];
                    if (universities.get(university) == null) {
                        universities.put(university, 1);
                    } else
                        universities.put(university, universities.get(university) + 1);

                }
            }
        }
        int filenumbers=0;
        for (int i=0;i<number;i++) {
            filenumbers += universities.get(Integer.toString(i));
        }
        return filenumbers;
    }

}
