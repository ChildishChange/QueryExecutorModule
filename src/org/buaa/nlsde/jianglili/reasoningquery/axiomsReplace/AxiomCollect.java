package org.buaa.nlsde.jianglili.reasoningquery.axiomsReplace;

import org.apache.jena.graph.Graph;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.Triple;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.sparql.algebra.Algebra;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.algebra.OpVisitorBase;
import org.apache.jena.sparql.algebra.op.OpBGP;
import org.apache.jena.sparql.algebra.op.OpProject;
import org.apache.jena.sparql.algebra.op.OpTriple;
import org.apache.jena.sparql.core.BasicPattern;
import org.apache.jena.sparql.core.Var;
import org.apache.jena.sparql.engine.QueryIterator;
import org.apache.jena.sparql.engine.binding.Binding;
import org.apache.jena.vocabulary.OWL;
import org.buaa.nlsde.jianglili.reasoningquery.assertionReplace.AlgebraWalker;
import org.buaa.nlsde.jianglili.reasoningquery.tripleUtils.TripleUtil;

import java.util.*;


/**
 * Created by jianglili on 2016/5/23.
 */
public class AxiomCollect extends OpVisitorBase {

    public static   BasicPattern  axiomPattern=new BasicPattern();
    Map<Var,List<Node>> axiomVar=new HashMap<Var,List<Node>>();

    Graph  graph =null;
    public  Op AxiomReplaceOpproject(Op op,Model model){
        graph=model.getGraph();
        //get all axiom and axiomvar
        AlgebraWalker.walkBottomUp(this, op);
        //for each axiomvar change the op
        // store op var
    //    System.out.println("size of axiomVar is "+axiomVar.size());
        for(Map.Entry<Var,List<Node>>  varEntry: axiomVar.entrySet()){
            if(op instanceof OpProject)
                ((OpProject)op).getVars().remove(varEntry.getKey());
            op= OpConceptVarReplace.replaceAxiomOp(op,varEntry);
        }
        return op;
    }
    //optimize by stw
    public  Op AxiomEasyReplaceOpproject(Op op,Model model){
        graph=model.getGraph();
        //get all axiom and axiomvar
        AlgebraWalker.walkBottomUp(this, op);
        //for each axiomvar change the op
        // store op var
        int index=0;
        for(Map.Entry<Var,List<Node>>  varEntry: axiomVar.entrySet()){
            if(index>0)
                break;
            if(op instanceof OpProject) {
                ((OpProject) op).getVars().remove(varEntry.getKey());

            }
            op= OpConceptVarReplace.replaceAxiomOp(op,varEntry);
            index++;
        }
        return op;
    }

    //transform each single pattern
    @Override
    public void visit(OpBGP opBGP) {
        BasicPattern basicPattern=opBGP.getPattern();
        Iterator<Triple>  tripleIterator=basicPattern.iterator();
        while (tripleIterator.hasNext()) {
            Triple  triple=tripleIterator.next();
            List<Node> varResult=new ArrayList<Node>();
           if(TripleUtil.PredictIsAxiom(triple,varResult)) {
                axiomPattern.add(triple);
                Var var=triple.getSubject().isVariable()? (Var)triple.getSubject():(Var)triple.getObject();

               if(varResult.size()==0)
               {
                   QueryIterator qIter = exe(triple);
                   for (; qIter.hasNext(); ) {
                       Binding b = qIter.nextBinding();
                       varResult.add(b.get(var));
                   }
                   varResult.remove(OWL.Nothing.asNode());
               }
                //get the intersection of the pre and now results
                if(axiomVar.get(var)!=null){
                    List<Node> varResultPre=axiomVar.get(var);
                    for(Node node:varResult){
                        if(!varResultPre.contains(node))
                            varResultPre.remove(node);
                    }
                }
                else
                  axiomVar.put(var,varResult);
            }
            else if(TripleUtil.IsTypePropertyOrClass(triple, basicPattern)) {     //can just delete   ?x rdf:type   class
                 tripleIterator.remove();
            }
        }
    }

    public  QueryIterator exe(Triple triple){
       return Algebra.exec(new OpTriple(triple), graph) ;
    }

    public static BasicPattern getAxiomPattern() {
        return axiomPattern;
    }
}
