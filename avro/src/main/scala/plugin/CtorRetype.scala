package edu.berkeley.cs.avro
package plugin

import scala.tools._
import nsc.Global
import nsc.Phase
import nsc.plugins.Plugin
import nsc.plugins.PluginComponent
import nsc.transform.Transform
import nsc.transform.InfoTransform
import nsc.transform.TypingTransformers
import nsc.symtab.Flags._
import nsc.util.Position
import nsc.util.NoPosition
import nsc.ast.TreeDSL
import nsc.typechecker
import scala.annotation.tailrec

import scala.collection.JavaConversions._

trait CtorRetype  extends ScalaAvroPluginComponent
                  with    Transform
                  with    TypingTransformers
                  with    TreeDSL {
  import global._
  import definitions._
  	  
  val runsAfter = List[String]("extender")
  override val runsRightAfter = Some("extender")
  val phaseName = "ctorretype"
  def newTransformer(unit: CompilationUnit) = new CtorRetypeTransformer(unit)    

  class CtorRetypeTransformer(unit: CompilationUnit) extends TypingTransformer(unit) {
    import CODE._

    override def transform(tree: Tree) : Tree = {
      val newTree = tree match {
        // REMOVE synthethic hashCode and toString methods generated by typer
        // for an AvroRecord (we want to use the impl in SpecificRecordBase)
        case cd @ ClassDef(mods, name, tparams, impl) if isMarked(cd) =>
          debug("removing synthetic methods for: " + currentClass.fullName)
          val newBody = impl.body.filterNot { 
            case d @ DefDef(_, _, _, _, _, _) => 
              d.symbol.hasFlag(SYNTHETICMETH) && 
              (d.symbol.name == nme.toString_ || d.symbol.name == nme.hashCode_)
            case _ => false
          }
          val newImpl = treeCopy.Template(impl, impl.parents, impl.self, newBody)
          treeCopy.ClassDef(tree, mods, name, tparams, newImpl)
        case a @ Apply(sel @ Select(sup @ Super(qual, name), name1), args) => 
          if (isMarked(currentClass)) {
            debug("retyping ctor reference for: " + currentClass.fullName)
            localTyper typed {
              Apply(Select(Super(qual, name) setPos sup.pos, name1) setPos sel.pos, transformTrees(args)) setPos tree.pos
            }
          } else {
            debug("skipping super reference: " + currentClass.fullName)
            tree
          }
        case _ => tree
      }
      super.transform(newTree)
    }    
  }

}
