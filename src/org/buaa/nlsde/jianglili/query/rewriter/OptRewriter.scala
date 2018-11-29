package org.buaa.nlsde.jianglili.rewriter

import java.util
import java.util.function.Consumer

import org.apache.jena.graph.{Node, NodeFactory, Node_Literal, Node_URI, Triple}
import org.apache.jena.query.{ARQ, QueryFactory}
import org.apache.jena.rdf.model.ModelFactory
import org.apache.jena.sparql.algebra.op._
import org.apache.jena.sparql.algebra.{Algebra, AlgebraGenerator, Op}
import org.apache.jena.sparql.core.BasicPattern
import org.apache.jena.sparql.expr
import org.apache.jena.sparql.expr.{Expr, ExprList}
import org.apache.jena.system.JenaSystem

import scala.collection.mutable
import scala.collection.mutable.{MutableList, Queue, Set, TreeSet}

object OptRewriter{

  val ctx = new TreeSet[String]()

  val serializer = new OpSerializer
  val rewriter = new RewriteVisitor(serializer)

  def main(args: Array[String]): Unit = {
    // initialize jena and spark context
    ARQ.init()
    JenaSystem.init()
    ARQ.enableOptimizer(ARQ.getContext, false)
    //   AlgebraGenerator.applySimplification = false

    val query = QueryFactory.read("file:query.opt/query2.rq")
    println(query)
    val op = Algebra.compile(query)

    print(op)
    print(optimize(op))
  }

  def optimize(op: Op): Op = {
    println("start optimizing...")
    optimizeRewrite(op)
  }

  def reset(): Unit = {
    this.ctx.clear()
  }

  private def optimizeRewrite(op: Op): Op = {
    rewriter.visit(op)
  }

  def serialize(op: Op): String = {
    op.toString
  }

  def serializeNew(op: Op): String = {
    val text = op match {
      case (opAssign: OpAssign) => serializer.serialize(opAssign)
      case (opBGP: OpBGP) => serializer.serialize(opBGP)
      case (opConditional: OpConditional) => serializer.serialize(opConditional)
      case (opDatasetNames: OpDatasetNames) => serializer.serialize(opDatasetNames)
      case (opDiff: OpDiff) => serializer.serialize(opDiff)
      case (opDisjunction: OpDisjunction) => serializer.serialize(opDisjunction)
      case (opDistinct: OpDistinct) => serializer.serialize(opDistinct)
      case (opExt: OpExt) => serializer.serialize(opExt)
      case (opExtend: OpExtend) => serializer.serialize(opExtend)
      case (opFilter: OpFilter) => serializer.serialize(opFilter)
      case (opGraph: OpGraph) => serializer.serialize(opGraph)
      case (opGroup: OpGroup) => serializer.serialize(opGroup)
      case (opJoin: OpJoin) => serializer.serialize(opJoin)
      case (opLabel: OpLabel) => serializer.serialize(opLabel)
      case (opLeftJoin: OpLeftJoin) => serializer.serialize(opLeftJoin)
      case (opList: OpList) => serializer.serialize(opList)
      case (opMinus: OpMinus) => serializer.serialize(opMinus)
      case (opNull: OpNull) => serializer.serialize(opNull)
      case (opOrder: OpOrder) => serializer.serialize(opOrder)
      case (opPath: OpPath) => serializer.serialize(opPath)
      case (opProcedure: OpProcedure) => serializer.serialize(opProcedure)
      case (opProject: OpProject) => serializer.serialize(opProject)
      case (opPropFunc: OpPropFunc) => serializer.serialize(opPropFunc)
      case (opQuad: OpQuad) => serializer.serialize(opQuad)
      case (opQuadBlock: OpQuadBlock) => serializer.serialize(opQuadBlock)
      case (opQuadPattern: OpQuadPattern) => serializer.serialize(opQuadPattern)
      case (opReduced: OpReduced) => serializer.serialize(opReduced)
      case (opSequence: OpSequence) => serializer.serialize(opSequence)
      case (opService: OpService) => serializer.serialize(opService)
      case (opSlice: OpSlice) => serializer.serialize(opSlice)
      case (opTable: OpTable) => serializer.serialize(opTable)
      case (opTopN: OpTopN) => serializer.serialize(opTopN)
      case (opTriple: OpTriple) => serializer.serialize(opTriple)
      case (opUnion: OpUnion) => serializer.serialize(opUnion)
    }
    text.mkString
  }
}

/**
  * Query rewrite: BGPs as Joins and re-order commutative sequential operators.
  */
class RewriteVisitor(_serializer: OpSerializer) {

  val serializer = _serializer

  def optGeneralise = false

  def visit(op: Op): Op = {
    op match {
      case (opAssign: OpAssign) => this.visit(opAssign)
      case (opBGP: OpBGP) => this.visit(opBGP)
      case (opConditional: OpConditional) => this.visit(opConditional)
      case (opDatasetNames: OpDatasetNames) => this.visit(opDatasetNames)
      case (opDiff: OpDiff) => this.visit(opDiff)
      case (opDisjunction: OpDisjunction) => this.visit(opDisjunction)
      case (opDistinct: OpDistinct) => this.visit(opDistinct)
      case (opExt: OpExt) => this.visit(opExt)
      case (opExtend: OpExtend) => this.visit(opExtend)
      case (opFilter: OpFilter) => this.visit(opFilter)
      case (opGraph: OpGraph) => this.visit(opGraph)
      case (opGroup: OpGroup) => this.visit(opGroup)
      case (opJoin: OpJoin) => this.visit(opJoin)
      case (opLabel: OpLabel) => this.visit(opLabel)
      case (opLeftJoin: OpLeftJoin) => this.visit(opLeftJoin)
      case (opList: OpList) => this.visit(opList)
      case (opMinus: OpMinus) => this.visit(opMinus)
      case (opNull: OpNull) => this.visit(opNull)
      case (opOrder: OpOrder) => this.visit(opOrder)
      case (opPath: OpPath) => this.visit(opPath)
      case (opProcedure: OpProcedure) => this.visit(opProcedure)
      case (opProject: OpProject) => this.visit(opProject)
      case (opPropFunc: OpPropFunc) => this.visit(opPropFunc)
      case (opQuad: OpQuad) => this.visit(opQuad)
      case (opQuadBlock: OpQuadBlock) => this.visit(opQuadBlock)
      case (opQuadPattern: OpQuadPattern) => this.visit(opQuadPattern)
      case (opReduced: OpReduced) => this.visit(opReduced)
      case (opSequence: OpSequence) => this.visit(opSequence)
      case (opService: OpService) => this.visit(opService)
      case (opSlice: OpSlice) => this.visit(opSlice)
      case (opTable: OpTable) => this.visit(opTable)
      case (opTopN: OpTopN) => this.visit(opTopN)
      case (opTriple: OpTriple) => this.visit(opTriple)
      case (opUnion: OpUnion) => this.visit(opUnion)
    }
  }

  def visit(opAssign: OpAssign): Op = {
    OpAssign.create(
      visit(opAssign.getSubOp),
      opAssign.getVarExprList)
  }

  // split bgp as joins
  def visit(opBGP: OpBGP): Op = {
    val triples: util.List[Triple] = opBGP.getPattern.getList
    if (triples.size() <= 1) {
      opBGP
    } else {
      var target: Op = new OpBGP(BasicPattern.wrap(triples.subList(0, 1)))
      for (i <- 1 until triples.size() ) {
        target = OpJoin.create(target, new OpBGP(BasicPattern.wrap(triples.subList(i, i + 1))))
      }
      visit(target) // flatten the join(s)
    }
  }

  def visit(opConditional: OpConditional): Op = {
    opConditional
  }

  def visit(opDatasetNames: OpDatasetNames): Op = {
    opDatasetNames
  }

  def visit(opDiff: OpDiff): Op = {
    opDiff
  }

  def visit(opDisjunction: OpDisjunction): Op = {
    opDisjunction
  }

  def visit(opDistinct: OpDistinct): Op = {
    OpDistinct.create(opDistinct.getSubOp)
  }

  def visit(opExt: OpExt): Op = {
    opExt
  }

  def visit(opExtend: OpExtend): Op = {
    opExtend
  }
  def filterBy(exprs: ExprList, op: Op): Op =  { if (exprs == null || exprs.isEmpty)  { return op}
    val f: OpFilter = op.asInstanceOf[OpFilter]
    f.getExprs.addAll(exprs)
    return f
  }

  /**************************************************************
    * Filter操作的优化
    *
    * 1. filter的内联
    *
    *    对于 filter(p, join(A, B))
    *    重写为 join(filter(p, A), filter(p, B))
    *    如此，join运算的两个输入的规模更小，能够更快
    *
    * 2. filter条件的融合和合并
    *
    *    filter(p1, filter(p2, ...)) => filter ((p1 . p2), ...)
    *
    *    减少一次Filter运算
    *
    * 3. filter内联进BGP
    *
    *     对于 filter(bgp(?x ?p ?y), ?y=xxx)
    *     重写为 bgp(?x, ?p, xxx)
    */
  def visit(opFilter: OpFilter): Op = {
    var subOp = opFilter.getSubOp
    var exprs = opFilter.getExprs

    subOp match {
      case op: OpTable => // eliminate the (table unit) element
        if (op.isJoinIdentity) {
          op
        } else {
          opFilter
        }
      ///////   filter的内联进Join、Union、LeftJoin
      case op: OpUnion => visit(OpUnion.create(
        filterBy(exprs, op.getLeft),
        filterBy(exprs, op.getRight)))
      case op: OpJoin => visit(OpJoin.create(
        visit(filterBy(exprs, op.getLeft)),
        visit(filterBy(exprs, op.getRight))))
      case op: OpLeftJoin => visit(OpLeftJoin.create(
        filterBy(exprs, op.getLeft),
        filterBy(exprs, op.getRight),
        opFilter.getExprs))
      case op: OpBGP =>
        ///////////////           filter内联进BGP
        if (optGeneralise) {
          filterBy(exprs, visit(subOp))
        } else {
          filterBy(exprs, visit(subOp))
          if (op.getPattern.getList.size() > 1) {
            // if we have more than one patterns in bgp, split it first.
            visit(filterBy(exprs, visit(op)))
          } else {
            // now the filter only apply on one triple.
            val triple = op.getPattern.getList.get(0)
            val exprs2: java.util.List[Expr] = new java.util.LinkedList()
            val mapping: java.util.Map[String, Expr] = new util.HashMap()
            exprs.getList.forEach(new Consumer[Expr] {
              override def accept(t: Expr): Unit = {
                // currently we only inline the "=" filters.
                if (t.getFunction.getOpName.equals("=")) {
                  val x = t.getFunction.getArgs.get(0)
                  val y = t.getFunction.getArgs.get(1)
                  if (x.isVariable && y.isConstant) {
                    mapping.put(x.getVarName, y)
                  } else if (x.isConstant && y.isVariable) {
                    mapping.put(y.getVarName, x)
                  } else {
                    // do nothing for other kind of filter.
                    exprs2.add(t)
                  }
                } else {
                  exprs2.add(t)
                }
              }
            })
            var subj = triple.getSubject
            var obj = triple.getObject
            if (subj.isVariable && mapping.containsKey(subj.getName)) {
              val v = mapping.get(subj.getName)
              if (v.getConstant.isLiteral) {
                subj = NodeFactory.createLiteral(v.getConstant.getString)
              } else if (v.getConstant.isIRI) {
                subj = NodeFactory.createURI(v.getConstant.getDatatypeURI)
              }
            }
            if (obj.isVariable && mapping.containsKey(obj.getName)) {
              val v = mapping.get(obj.getName)
              if (v.getConstant.isLiteral) {
                obj = NodeFactory.createLiteral(v.getConstant.getString)
              } else if (v.getConstant.isIRI) {
                obj = NodeFactory.createURI(v.getConstant.getDatatypeURI)
              }
            }
            val triple2 = Triple.create(subj, triple.getPredicate, obj)
            val pat = new BasicPattern()
            pat.add(triple2)
            val newOpBGP = new OpBGP(pat)
            if (exprs2.isEmpty) {
              newOpBGP
            } else {
              val expr3=new expr.ExprList(exprs2)
              filterBy(expr3, newOpBGP)
            }
          }
        }
      case op =>
        /////////////////        filter条件的融合和合并
        var progressed = false
        while (subOp.isInstanceOf[OpFilter]) {
          progressed = true
          val op = subOp.asInstanceOf[OpFilter]
          exprs.addAll(op.getExprs)
          subOp = op.getSubOp
        }
        if (progressed) {
          visit(filterBy(exprs, subOp))
        } else {
          filterBy(exprs, visit(subOp))
        }
    }
  }

  def visit(opGraph: OpGraph): Op = {
    opGraph
  }

  def visit(opGroup: OpGroup): Op = {
    new OpGroup(
      opGroup.getSubOp,
      opGroup.getGroupVars,
      opGroup.getAggregators)
  }

  /**********************************************
    * 对多个连续Join的优化
    *
    * step1: 知道所有的进行连续Join操作的子查询
    * step2: 重新排序整理
    */
  def visit(opJoin: OpJoin): Op = {
    def isJoinIdentity(op: Op): Boolean = {
      op.isInstanceOf[OpTable] && op.asInstanceOf[OpTable].isJoinIdentity
    }
    val left = visit(opJoin.getLeft)
    val right = visit(opJoin.getRight)

    // flatten multiple joins.
    val operands: MutableList[Op] = MutableList()

    val q1: mutable.Queue[Op] = mutable.Queue()
    if (!isJoinIdentity(left)) {
      q1 += left
    }
    if (!isJoinIdentity(right)) {
      q1 += right
    }
    assert(q1.nonEmpty, "opJoin: one left/right mustn't be OpTable Unit!")
    while (q1.nonEmpty) {
      val x = q1.dequeue()
      if (x.isInstanceOf[OpJoin]) {
        val t = x.asInstanceOf[OpJoin]
        val l = visit(t.getLeft)
        val r = visit(t.getRight)
        if (!isJoinIdentity(l)) {
          q1 += l
        }
        if (!isJoinIdentity(r)) {
          q1 += r
        }
        assert(!isJoinIdentity(l) || !isJoinIdentity(r))
      } else {
        operands += x
      }
    }
    //merge
    val q2: mutable.Queue[Op] = mutable.Queue()
    val ids: mutable.Set[String] = mutable.Set()
    q2 ++= operands
    while (q2.size != 1) {
      val l = q2.dequeue()
      val lh = serializer.serialize(l)
      val r = q2.dequeue()
      val rh = serializer.serialize(r)
      if (lh == rh) {
        q2 += l
      } else if (!ids.contains(lh) && !ids.contains(rh)) {    ////////  公共子查询剔除
        q2 += OpJoin.create(l, r)
        ids += lh
        ids += rh
      } else {
        if (!ids.contains(lh)) {
          q2 += l
        }
        if (!ids.contains(rh)) {
          q2 += r
        }
      }
    }
    q2.dequeue()
  }

  def visit(opLabel: OpLabel): Op = {
    OpLabel.create(
      opLabel.getObject,
      opLabel.getSubOp)
  }

  def visit(opLeftJoin: OpLeftJoin): Op = {
    OpLeftJoin.create(
      visit(opLeftJoin.getLeft),
      visit(opLeftJoin.getRight),
      opLeftJoin.getExprs)
  }

  def visit(opList: OpList): Op = {
    new OpList(visit(opList.getSubOp))
  }

  def visit(opMinus: OpMinus): Op = {
    OpMinus.create(
      visit(opMinus.getLeft),
      visit(opMinus.getRight))
  }

  def visit(opNull: OpNull): Op = {
    opNull
  }

  def visit(opOrder: OpOrder): Op = {
    opOrder
  }

  def visit(opPath: OpPath): Op = {
    opPath
  }

  def visit(opProcedure: OpProcedure): Op = {
    opProcedure
  }

  def visit(opProject: OpProject): Op = {
    new OpProject(visit(opProject.getSubOp), opProject.getVars)
  }

  def visit(opPropFunc: OpPropFunc): Op = {
    opPropFunc
  }

  def visit(opQuad: OpQuad): Op = {
    opQuad
  }

  def visit(opQuadBlock: OpQuadBlock): Op = {
    opQuadBlock
  }

  def visit(opQuadPattern: OpQuadPattern): Op = {
    opQuadPattern
  }

  def visit(opReduced: OpReduced): Op = {
    opReduced
  }

  def visit(opSequence: OpSequence): Op = {
    opSequence
  }

  def visit(opService: OpService): Op = {
    opService
  }

  def visit(opSlice: OpSlice): Op = {
    opSlice
  }

  def visit(opTable: OpTable): Op = {
    opTable
  }

  def visit(opTopN: OpTopN): Op = {
    opTopN
  }

  def visit(opTriple: OpTriple): Op = {
    opTriple
  }

  /**********************************************
    * 对多个连续Union的优化
    *
    * step1: 知道所有的进行连续Union操作的子查询
    * step2: 重新排序整理
    */
  def visit(op: OpUnion): Op = {
    val operands: MutableList [Op] = MutableList()

    // flatten
    val q1: Queue[Op] = Queue()
    q1 += op.getLeft
    q1 += op.getRight
    while (q1.nonEmpty) {
      val x = q1.dequeue()
      if (x.isInstanceOf[OpUnion]) {
        val t = x.asInstanceOf[OpUnion]
        q1 += t.getLeft
        q1 += t.getRight
      } else {
        operands += x
      }
    }
    // merge
    val q2: Queue[Op] = Queue()
    val ids: mutable.Set[String] = mutable.Set()
    q2 ++= operands.map(visit)
    while (q2.size != 1) {
      val l = q2.dequeue()
      val lh = serializer.serialize(l)
      val r = q2.dequeue()
      val rh = serializer.serialize(r)
      if (lh == rh) {
        q2 += l
      } else if (!ids.contains(lh) && !ids.contains(rh)) {    ////////  公共子查询剔除
        q2 += OpUnion.create(l, r)
        ids += lh
        ids += rh
      } else {
        if (!ids.contains(lh)) {
          q2 += l
        }
        if (!ids.contains(rh)) {
          q2 += r
        }
      }
    }
    q2.dequeue()
  }
}

/**
  * Get unique optimized string identity representation of an Op object.
  */
class OpSerializer {
  def serialize(op: Op): String = op.toString()
}

