package org.buaa.nlsde.jianglili.utils;

import java.io.File;

/**
 * Created by jianglili on 2017/2/4.
 */
public class manifestjar {

    public static void main(String[] args) {
        File dir=new File("D:\\Users\\git\\sparkForSparql\\out\\artifacts\\sparkForSparql_jar");


        for(File f:dir.listFiles()){
            System.out.println("lib\\"+f.getName());
        }
    }
}
