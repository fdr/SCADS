package edu.berkeley.cs.scads.piql

import java.io.File
import edu.berkeley.cs.scads.piql.parser._
import edu.berkeley.cs.scads.piql.DynamicDispatch._
import edu.berkeley.cs.scads.storage.TestScalaEngine

case class DotGraph(name: String, components: Iterable[DotComponent])

object DotComponent {
  private val curId = new java.util.concurrent.atomic.AtomicInteger
  def nextId = curId.getAndIncrement()
}

abstract class DotComponent {val id: String}

case class SubGraph(label: String, nodes: List[DotComponent]) extends DotComponent {
  val graphId = "cluster" + DotComponent.nextId
  val id = nodes.first.id
}

case class DotNode(label: String, children: List[DotComponent] = Nil) extends DotComponent {
  val id = "dotNode" + DotComponent.nextId
  def this(label: String, child: DotNode) = this(label, List(child))
}

object GraphVis {
  def main(args: Array[String]): Unit = {
    val piql = Compiler.readFile(new File(args(0)))
    val ast = Compiler.getAST(piql)
    val boundAst = new Binder(ast).bind
    val spec = new Optimizer(boundAst).optimizedSpec
    val code = ScalaGen(spec)
    val compiler = new ScalaCompiler
    compiler.compile(code)
    val configurator = compiler.classLoader.loadClass("piql.Configurator")

    implicit val env: Environment = configurator.callStatic("configure", List(TestScalaEngine.cluster)) match {
      case e: Environment => e
      case err => throw new RuntimeException("Bad configurator return type: " + err.getClass)
    }

    val orphanPlans = spec.orphanQueries.map(q => SubGraph(q._1, List(generateGraph(q._2.plan))))
    val instancePlans = spec.entities.flatMap(e => {
      e._2.queries.map(q => SubGraph(e._1 + "." + q._1, List(generateGraph(q._2.plan))))
    })

    println(DotGen(DotGraph("QueryPlans", orphanPlans ++ instancePlans)))
    System.exit(0)
  }

  protected def getPlans(spec: BoundSpec): Iterable[QueryPlan] = spec.orphanQueries.map(_._2).map(_.plan) ++ spec.entities.map(_._2).flatMap(_.queries).map(_._2).map(_.plan)

  protected def getPredicates(ns: String, keySpec: List[BoundValue], child: DotNode)(implicit env: Environment): DotNode = {
    val namespace = env.namespaces(ns)

    keySpec.zipWithIndex.foldLeft(child) {
      case (subPlan: DotNode, (value: BoundValue, idx: Int)) => {
        val fieldName = namespace.keySchema.getFields.get(idx).name
        DotNode("selection " + fieldName + "=" + value, List(subPlan))
      }
    }
  }

  def getJoinPredicates(ns: String, conditions: Seq[JoinCondition], child: DotNode)(implicit env: Environment): DotNode = {
    val namespace = env.namespaces(ns)

    conditions.zipWithIndex.foldLeft(child) {
      case (subPlan: DotNode, (value: JoinCondition, idx: Int)) => {
        val fieldName = namespace.keySchema.getFields.get(idx).name
        DotNode("selection " + fieldName + "=" + value, List(subPlan))
      }
    }
  }

  def generateGraph(plan: QueryPlan)(implicit env: Environment): DotComponent = {
    plan match {
      case SingleGet(ns, key) => SubGraph("SingleGet", List(getPredicates(ns, key, DotNode(ns))))
      case PrefixGet(ns, prefix, limit, ascending) => SubGraph("PrefixGet", List(getPredicates(ns, prefix, DotNode(ns))))
      case SequentialDereferenceIndex(ns, child) => SubGraph("SequentialDeref", List(DotNode("deref", List(DotNode(ns), generateGraph(child)))))
      case PrefixJoin(ns, conditions, limit, ascending, child) => SubGraph("PrefixJoin", List(getJoinPredicates(ns, conditions, DotNode("join", List(DotNode(ns), generateGraph(child))))))
      case PointerJoin(ns, conditions, child) => SubGraph("PointerJoin", List(getJoinPredicates(ns, conditions, DotNode("join", List(DotNode(ns), generateGraph(child))))))
      case Materialize(entityType, child) => generateGraph(child)
      case Selection(equalityMap, child) => DotNode("selection", List(generateGraph(child)))
      case Sort(fields, ascending, child) => DotNode("sort", List(generateGraph(child)))
      case TopK(k, child) => DotNode("topk " + k, List(generateGraph(child)))
    }
  }
}

object DotGen extends Generator[DotGraph] {
  private val curId = new java.util.concurrent.atomic.AtomicInteger

  protected def generate(graph: DotGraph)(implicit sb: StringBuilder, indnt: Indentation): Unit = {
    outputBraced("digraph ", graph.name) {
      graph.components.foreach(generateSubGraph)
    }
  }

  protected def generateSubGraph(graph: DotComponent)(implicit sb: StringBuilder, indnt: Indentation): Unit = {
    graph match {
      case node: DotNode => {
        output(node.id, "[label=", quote(node.label), "];")
        node.children.foreach(generateSubGraph)
        node.children.map(_.id).foreach(id => output(node.id, " -> ", id, ";"))
      }
      case subGraph: SubGraph => {
        outputBraced("subgraph ", subGraph.graphId) {
          output("label=", quote(subGraph.label), ";")
          subGraph.nodes.foreach(generateSubGraph)
        }
      }
    }
  }
}