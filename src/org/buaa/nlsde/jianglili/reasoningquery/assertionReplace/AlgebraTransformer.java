package org.buaa.nlsde.jianglili.reasoningquery.assertionReplace;

import com.clarkparsia.owlapiv3.OWL;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.NodeFactory;
import org.apache.jena.graph.Triple;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.algebra.OpVisitorBase;
import org.apache.jena.sparql.algebra.op.*;
import org.apache.jena.sparql.core.BasicPattern;
import org.apache.jena.sparql.core.Var;
import org.buaa.nlsde.jianglili.reasoningquery.axiomsReplace.AxiomCollect;
import org.buaa.nlsde.jianglili.reasoningquery.conceptExtract.Concept;
import org.buaa.nlsde.jianglili.reasoningquery.conceptExtract.ConceptUtilNodeWrapper;
import org.buaa.nlsde.jianglili.reasoningquery.tripleUtils.OpUtil;
import org.buaa.nlsde.jianglili.reasoningquery.tripleUtils.TripleUtil;
import org.semanticweb.owlapi.model.OWLOntologyCreationException;
import org.semanticweb.owlapi.model.OWLOntologyStorageException;

import java.io.FileNotFoundException;
import java.util.*;

/**
 * Created by jianglili on 2016/3/25.
 */
public class AlgebraTransformer extends OpVisitorBase {
    private final Stack<Op> stack;
    private final Concept concepts;
    private final ConceptUtilNodeWrapper conceptUtilWrapper;
    private final int limitation;
//    private final OpProject op;

    public AlgebraTransformer(Concept concept,Boolean extractFlag, int limit) throws OWLOntologyCreationException, OWLOntologyStorageException, FileNotFoundException {
        stack = new Stack<Op>();
        concepts = concept;
        conceptUtilWrapper = ConceptUtilNodeWrapper.getSingleton(concept,extractFlag);
        limitation = limit;
    }
    //before the rewrite first replace the axioms        //
    //judge if it was an axiom
   // triple= SubAxiomReplace.replaceAxiomVar(triple);
    public Op AxiomTransform(Op op){

        return new AxiomCollect().AxiomReplaceOpproject(op,concepts.getModel());
    }

    // both transform
    public Op BothTransform(Op op){

        op= new AxiomCollect().AxiomReplaceOpproject(op,concepts.getModel());
        return   transform(op);
    }
    //
    public Op BothTransform2(Op op){

        op= new AxiomCollect().AxiomEasyReplaceOpproject(op,concepts.getModel());
        return   transform(op);
    }
    // triple assetions transform
    public Op transform(Op op) {
        AlgebraWalker.walkBottomUp(this, op);
        return stack.pop();
    }
    // get the equivalent of a triple assetions
    public TripleEquivalent getEquivalentTriple(Triple triple) {
        TripleEquivalent tripleEquivalent=new TripleEquivalent();
        //one var or two vars
        int count = TripleUtil.varOfTriple(triple);
        // the predict is not var
        if (!TripleUtil.PredictIsVar(triple)) {
            //judge if the predict is rdf:type   //count=1 && ? rdf:type class
            if (TripleUtil.PredictIsType(triple)&&count == 1) {

                Node object = triple.getObject();
                //judge is the object is class
                if (conceptUtilWrapper.IsOWLClass(object)) {    //get the equivalent class of object
                    concepts.addClass(OWL.Class(object.toString()));
                    //get subclasses
                    Set<Node> classes = conceptUtilWrapper.getReplaceNodeOfClass(object);
                    //reassemble the triple
                    if (classes==null||classes.size() == 0) {
                        //   ��¼û���Ƴ��������?
                        // A=B��C&D
                        // get the join equivalent set
                        Set<Set<Node>> objectSetSet=conceptUtilWrapper.getReplaceNodeOfJoinEquivalentClass(object);
                        if(objectSetSet==null||objectSetSet.size()==0){
                            return  tripleEquivalent;
                        }
                        Op op=new OpTriple(triple).asBGP();
                        List<OpBGP>  opBGPs=new ArrayList<>();
                        // if the role chain is not empty
                        int i=0;  Node varPre=null;
                        for(Set<Node> nodeSet:objectSetSet) {
                            BasicPattern  basicPattern=new BasicPattern();
                            for (Node node : nodeSet) {
                                basicPattern.add(new Triple(triple.getSubject(), triple.getPredicate(),node));
                            }
                            opBGPs.add(new OpBGP(basicPattern));
                        }// end of for join equivalent

                        for(OpBGP opbgp:opBGPs) {
                            Op opUnion = new OpUnion(op,opbgp);
                            op=opUnion;
                        }
                        tripleEquivalent.setE(true);
                        tripleEquivalent.setOp(op);
                        return tripleEquivalent;
                    }  // end of Join equivalent set

                    // generate the equivalent classes triple
                    List<OpTriple> opTriples=new ArrayList<>();
                    int number = 0;
                    for (Node node : classes)
                    {
                        if(limitation!=0&&number>limitation-1)
                            break;
                        opTriples.add(new OpTriple(new Triple(triple.getSubject(), triple.getPredicate(), node)));
                        number ++;
                    }
                    Op  op=new OpTriple(triple).asBGP();
                    for(OpTriple opTriple:opTriples) {
                      Op opUnion = new OpUnion(op,opTriple.asBGP());
                        op=opUnion;
                    }
                    tripleEquivalent.setE(true);
                    tripleEquivalent.setOp(op);
                }
            }    //end of  count=1 && ? rdf:type class
            // the p is the nornal property now only support object property
            // a p ? or ? p a
            else {
                Node predict = triple.getPredicate();
                concepts.addProperty(OWL.ObjectProperty(predict.toString()));
                // get subproperties
                Set<Node> predicts = conceptUtilWrapper.getReplaceNodeOfSubProperties(predict);
                //  get inversesubproperties
                Set<Node> inversePredicts=conceptUtilWrapper.getReplaceInverseOfProperty(predict);

                //do not have equivalent or sub properties
                if ((predicts == null||predicts.size()==0)&&(inversePredicts == null||inversePredicts.size()==0)){
                    // get role chain
                    Set<List<Node>> predictListSet=conceptUtilWrapper.getRoleObjectProperties(predict);
                    if(predictListSet==null||predictListSet.size()==0){
                        return  tripleEquivalent;
                    }
                    Op op=new OpTriple(triple).asBGP();
                    List<OpBGP>  opBGPs=new ArrayList<>();
                    // if the role chain is not empty
                    int i=0;  Node varPre=null;
                    for(List<Node> nodeList:predictListSet) {
                        BasicPattern  basicPattern=new BasicPattern();
                        for (Node node : nodeList) {
                            Node var= NodeFactory.createVariable("N_"+String.valueOf(i++));
                            if(basicPattern.size()==0) {
                                //the first interator
                                basicPattern.add(new Triple(triple.getSubject(), node, var));
                                varPre=var;
                            }
                            else if(basicPattern.size()==nodeList.size()-1){
                                //the last iterator
                                basicPattern.add(new Triple(varPre, node, triple.getObject()));
                            }
                            else {
                                //the middle iterator
                                basicPattern.add(new Triple(varPre, node, var));
                                varPre=var;
                            }
                        }
                        opBGPs.add(new OpBGP(basicPattern));
                    }// end of for

                    for(OpBGP opbgp:opBGPs) {
                        Op opUnion = new OpUnion(op,opbgp);
                        op=opUnion;
                    }
                    tripleEquivalent.setE(true);
                    tripleEquivalent.setOp(op);
                    return tripleEquivalent;

                }  // end of role chain

                //reassemble the triple
                // generate the equivalent or subproperties properties triple
                List<OpTriple> opTriples=new ArrayList<>();
                int number = 0;
                for (Node node : predicts)
                {
                    if(limitation!=0&&number>limitation-1)
                        break;
                    opTriples.add(new OpTriple(new Triple(triple.getSubject(), node, triple.getObject())));
                    number++;
                }
                //  generate the inverseOf properties triple
                //reassemble the triple
                number = 0;
                for (Node node : inversePredicts)
                {
                    if(limitation!=0&&number>limitation-1)
                        break;
                    opTriples.add(new OpTriple(new Triple(triple.getObject(), node, triple.getSubject())));
                    number++;
                }
                Op  op=new OpTriple(triple).asBGP();
                for(OpTriple opTriple:opTriples) {
                    Op opUnion = new OpUnion(op,opTriple.asBGP());
                    op=opUnion;
                }
                tripleEquivalent.setE(true);
                tripleEquivalent.setOp(op);

            }// end of sub properties
        }
        return tripleEquivalent;
    }

    //transform each single pattern
    @Override
    public void visit(OpBGP opBGP) {
        BasicPattern  basicPattern=opBGP.getPattern();
        BasicPattern  newPattern=new BasicPattern();
//        Op  pre=null;
        Map<Op,List<Var>>  opUnionList= new HashMap<Op,List<Var>>();
      //  Map<Triple,TripleEquivalent>   basicTripleMap=new HashMap<Triple,TripleEquivalent>();
        boolean flag=false;
        for(Triple triple: basicPattern) {
            // get the equivalent triples
            TripleEquivalent tripleEquivalent= getEquivalentTriple(triple);
            if(tripleEquivalent.hasE()) {
                List<Var> tripleVars= TripleUtil.getVars(triple);
                opUnionList.put(tripleEquivalent.getOp(), tripleVars);
                //get the first unionOp
                // pre=basicTripleMap.get(triple).getOp();
                if(!flag)    flag=true;
                //compose the Union Op
//                Op opUnion = basicTripleMap.get(triple).getOp();
//                Op opJoin = OpJoin.create(pre, opUnion);
//                pre = opJoin;

            }
            else{
                //triple that do not have eauivalent
                newPattern.add(triple);
            }

        }
        if(!flag)     stack.push(opBGP);
        else
        {
            if(!newPattern.isEmpty()) {
                Op basic = new OpBGP(newPattern);
                opUnionList.put(basic, OpUtil.getOpBGPVars((OpBGP) basic));
            }
            List<Op> opkeys=new ArrayList<Op>(opUnionList.keySet());
            Op   pre= opkeys.remove(0);
            List<Var>  varListPre=opUnionList.get(pre);
            opUnionList.remove(pre);
            while (!opkeys.isEmpty()){
                // get related op
                Op opToJoin= OpUtil.getRelatedOp(opUnionList, varListPre);
                // new join op
                Op opJoin = OpJoin.create(pre, opToJoin);
                // add new var to op pre
                varListPre.addAll(opUnionList.get(opToJoin));
                //pre to new op
                pre=opJoin;
                // op set remove
                opkeys.remove(opToJoin);
                opUnionList.remove(opToJoin);
            }
            stack.push(pre);
//            if(!newPattern.isEmpty()) {
//                Op basic = new OpBGP(newPattern);
//                Op opAllJoin = OpJoin.create(basic, pre);
//                stack.push(opAllJoin);
//            }
//            else {
//                stack.push(pre);
//            }
        }
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
