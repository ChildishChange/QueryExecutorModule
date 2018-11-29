package org.buaa.nlsde.jianglili.utils;

import shared.SharedObjectFactory;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by jianglili on 2016/5/25.
 */
public class jarDir {

    SharedObjectFactory sharedObjectFactory;


    public static void main(String[] args) {

        List<String> jarList = new ArrayList<String>();
        String dirString = "D:\\Users\\git\\sparkForSparql\\out\\artifacts\\sparkForSparql_jar\\lib";
        File dir = new File(dirString);
        if (dir.isDirectory()) {
            File[] filelist = dir.listFiles();
            for (int i = 0; i < filelist.length; i++) {
                if (!filelist[i].isDirectory()) {
                    jarList.add(filelist[i].getName());
                }
                else
                {
                    File[] list = filelist[i].listFiles();
                    for (int j = 0; j < list.length; j++){
                        if (!list[j].isDirectory()) {
                            jarList.add(list[j].getName());
                        };
                    };
                };
            };
        };
        for(String jar:jarList){
            System.out.println(jar);
        }

       // System.out.println(jarList);
    }
}
