package org.buaa.nlsde.jianglili.utils;

/**
 * Created by jianglili on 2017/2/3.
 */
public class resultCount {
    public static  String  res=
            "[2017-02-14  08:43:54]/home/cluster/jianglili/lubm/query28/query20.rq;2743;3217;2090;205364;72927\n" +
                    "[2017-02-14  08:47:48]/home/cluster/jianglili/lubm/query28/query20.rq;0;0;2;181835;72927\n" +
                    "[2017-02-14  08:51:37]/home/cluster/jianglili/lubm/query28/query20.rq;0;0;0;184161;72927\n" +
                    "\n" +
                    "[2017-02-14  08:54:00]/home/cluster/jianglili/lubm/query28/query22.rq;2751;3292;2340;104119;2\n" +
                    "[2017-02-14  08:55:37]/home/cluster/jianglili/lubm/query28/query22.rq;0;0;0;70649;2\n" +
                    "[2017-02-14  08:57:14]/home/cluster/jianglili/lubm/query28/query22.rq;0;0;0;70624;2";

    public static void main(String[] args) {

        String[]  reslines= res.split("\n");
        boolean flag=false;
        double d=0;
        int count=0;
        for(String line:reslines) {
            if (flag&&line.startsWith("[")) {
                String[] lineSplits = line.split(";");
     //             System.out.println(line);
                  d+=Double.parseDouble(lineSplits[lineSplits.length - 2]) / 1000;
//                System.out.println(d/2);
//                if(d<4)
                    System.out.println(String.format("%.0f",d/2));
//                else
//                    System.out.println(Math.round(d/2));
                d=0;
                flag=false;
            }
            else if(!flag&&line.startsWith("[")){
                String[] lineSplits = line.split(";");
             //   System.out.println(line);
              //  d+=Double.parseDouble(lineSplits[lineSplits.length - 2]) / 1000;
                count++;
                if(count==2) {
                    d+=Double.parseDouble(lineSplits[lineSplits.length - 2]) / 1000;
                    flag = true;
                    count=0;
                }
            }

        }

    }
}
