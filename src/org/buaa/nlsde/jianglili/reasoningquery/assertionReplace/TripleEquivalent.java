package org.buaa.nlsde.jianglili.reasoningquery.assertionReplace;

import org.apache.jena.sparql.algebra.Op;

/**
 * Created by jianglili on 2016/4/1.
 */
public class TripleEquivalent {
    // have equivalent or not
    private boolean e=false;
    private Op op=null;

    // return is or not equivalent
    public boolean hasE() {
        return e;
    }

    public void setE(boolean e) {
        this.e = e;
    }

    public Op getOp() {
        return op;
    }

    public void setOp(Op op) {
        this.op = op;
    }
}
