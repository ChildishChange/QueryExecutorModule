package org.buaa.nlsde.jianglili.reasoningquery.axiomsReplace;

import org.apache.jena.graph.Node;
import org.apache.jena.graph.Triple;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.algebra.OpVisitorBase;
import org.apache.jena.sparql.algebra.op.*;
import org.apache.jena.sparql.core.BasicPattern;
import org.apache.jena.sparql.core.Var;
import org.buaa.nlsde.jianglili.reasoningquery.assertionReplace.AlgebraWalker;
import org.buaa.nlsde.jianglili.reasoningquery.tripleUtils.TripleUtil;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Stack;


/**
 * Created by jianglili on 2016/5/24.
 */
public class OpConceptVarReplace extends OpVisitorBase {
    private final Stack<Op> stack;
    static BasicPattern  axiomPattern;
    static Var var;
    static List<Node> replaceNodes;
    public static Op replaceAxiomOp(Op op, Map.Entry<Var,List<Node>>  varEntry) {
        axiomPattern = AxiomCollect.getAxiomPattern();
        var = varEntry.getKey();
        replaceNodes = varEntry.getValue();
        if(replaceNodes.size()==0)    return op;
        return new OpConceptVarReplace().transform(op);
    }
    public OpConceptVarReplace(){
        stack = new Stack<Op>();
    }

    public Op transform(Op op){

        AlgebraWalker.walkBottomUp(this, op);
        return stack.pop();
    }
    //transform each single pattern
    @Override
    public void visit(OpBGP opBGP) {
        BasicPattern  basicPattern=opBGP.getPattern();
        BasicPattern  newPattern=new BasicPattern();
        Op  pre=null;
        boolean flag=false;
        for(Triple triple: basicPattern) {
          List<Triple>  axiomPatternList=axiomPattern.getList();
            if(!axiomPatternList.contains(triple)){
                if(TripleUtil.containsVar(triple,var)){
                    if(!flag){
                        //get the first unionOp
                        pre=replaceTriple(triple);
                        flag=true;
                    }
                    else {
                        //compose the pre Op
                        Op op = replaceTriple(triple);
                        Op opJoin;
                        if(op instanceof OpBGP && pre instanceof OpBGP) {
                            ((OpBGP) pre).getPattern().addAll(((OpBGP) op).getPattern());
                             opJoin = pre;
                        }
                        else
                             opJoin = OpJoin.create(pre, op);
                        pre = opJoin;
                    }
                }
                else{
                    //triple that do not have to replace
                    newPattern.add(triple);
                }
            }
        }
        if(!flag)     stack.push(opBGP);
        else
        {
            Op basic=new OpBGP(newPattern);
            Op opAllJoin;
            if(pre instanceof OpBGP) {
                ((OpBGP) pre).getPattern().addAll(((OpBGP) basic).getPattern());
                opAllJoin = pre;
            }
            else
               opAllJoin=OpJoin.create(basic,pre);
            stack.push(opAllJoin);
        }
    }

    public Op replaceTriple(Triple triple){
        Op op=null;
        Node subject;
        Node predictate;
        Node object;
        List<Triple> opTriples=new ArrayList<Triple>();
        for(Node node:replaceNodes){
            subject=triple.getSubject().equals(var)?node:triple.getSubject();
            predictate=triple.getPredicate().equals(var)?node:triple.getPredicate();
            object=triple.getObject().equals(var)?node:triple.getObject();
            Triple newtriple= new Triple(subject,predictate,object);
            if(!TripleUtil.getVars(newtriple).isEmpty())
                opTriples.add(newtriple);
        }
        if(!opTriples.isEmpty()) {
            op = new OpTriple(opTriples.get(0)).asBGP();
            for (Triple opTriple : opTriples.subList(1, opTriples.size())) {
                Op opUnion = new OpUnion(op, new OpTriple(opTriple).asBGP());
                op = opUnion;
            }
        }
        return op;
    }


    @Override
    public void visit(OpFilter opFilter) {
        Op subOp = stack.pop();
        stack.push(OpFilter.filterDirect(opFilter.getExprs(),subOp));
    }
    @Override
    public void visit(OpJoin opJoin) {
        Op rightOp = stack.pop();
        Op leftOp = stack.pop();
        stack.push(OpJoin.create(leftOp, rightOp));
    }
    @Override
    public void visit(OpSequence opSequence) {
        OpSequence Sequence = OpSequence.create();
        for(int i=0; i<opSequence.size(); i++) {
            Sequence.add(stack.pop());
        }
        stack.push(Sequence);

    }
    @Override
    public void visit(OpLeftJoin opLeftJoin) {
        Op rightOp = stack.pop();
        Op leftOp = stack.pop();
        stack.push(OpLeftJoin.create(leftOp, rightOp,  opLeftJoin.getExprs()));
    }

    @Override
    public void visit(OpConditional opConditional) {
        Op rightOp = stack.pop();
        Op leftOp = stack.pop();
        stack.push(new OpConditional(leftOp, rightOp));
    }

    @Override
    public void visit(OpUnion opUnion) {
        Op rightOp = stack.pop();
        Op leftOp = stack.pop();
        stack.push(new OpUnion(leftOp, rightOp));

    }

    @Override
    public void visit(OpProject opProject) {
        Op subOp = stack.pop();
        stack.push(new OpProject(subOp,opProject.getVars()));
    }

    @Override
    public void visit(OpDistinct opDistinct) {
        Op subOp = stack.pop();
        stack.push(new OpDistinct(subOp));
    }

    @Override
    public void visit(OpOrder opOrder) {
        Op subOp = stack.pop();
        stack.push(new OpOrder(subOp, opOrder.getConditions()));
    }

    @Override
    public void visit(OpSlice opSlice) {
        Op subOp = stack.pop();
        stack.push(new OpSlice(subOp, opSlice.getStart(),opSlice.getLength()));
    }

    @Override
    public void visit(OpReduced opReduced) {
        Op subOp = stack.pop();
        stack.push(OpReduced.create(subOp));
    }
}
