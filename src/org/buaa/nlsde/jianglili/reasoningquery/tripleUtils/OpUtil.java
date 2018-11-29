package org.buaa.nlsde.jianglili.reasoningquery.tripleUtils;

import org.apache.jena.graph.Triple;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.algebra.op.OpBGP;
import org.apache.jena.sparql.core.Var;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Created by jianglili on 2016/6/6.
 */
public class OpUtil {

    public static Op getRelatedOp(Map<Op,List<Var>>  opUnionMap, List<Var> vars){
        for(Map.Entry<Op,List<Var>> entry: opUnionMap.entrySet()){
            for(Var var:entry.getValue())
            if(vars.contains(var))
                return entry.getKey();
        }
        return null;
    }
    public static List<Var> getOpBGPVars(OpBGP opBGP){
        List<Var> varList=new ArrayList<>();
       for(Triple triple: opBGP.getPattern()){
           varList.addAll(TripleUtil.getVars(triple));
       }
        return varList;
    }
}
