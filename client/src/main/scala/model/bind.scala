package edu.berkeley.cs.scads.model.parser

import org.apache.log4j.Logger

/* Exceptions that can occur during binding */
sealed class BindingException extends Exception
case class DuplicateEntityException(entityName: String) extends BindingException
case class DuplicateAttributeException(entityName: String, attributeName: String) extends BindingException
case class DuplicateRelationException(relationName: String) extends BindingException
case class UnknownEntityException(entityName: String) extends BindingException
case class DuplicateQueryException(queryName: String) extends BindingException
case class DuplicateParameterException(queryName: String) extends BindingException
case class BadParameterOrdinals(queryName:String) extends BindingException
case class AmbigiousThisParameter(queryName: String) extends BindingException
case class UnknownRelationshipException(queryName :String) extends BindingException
case class AmbiguiousJoinAlias(queryName: String, alias: String) extends BindingException
case class UnsupportedPredicateException(queryName: String, predicate: Predicate) extends BindingException
case class AmbiguiousAttribute(queryName: String, attribute: String) extends BindingException
case class UnknownAttributeException(queryName: String, attribute: String) extends BindingException
case class UnknownFetchAlias(queryName: String, alias: String) extends BindingException
case class InconsistentParameterTyping(queryName: String, paramName: String) extends BindingException

/* Bound counterparts for some of the AST */
case class BoundRelationship(target: String, cardinality: Cardinality)
case class BoundEntity(attributes: scala.collection.mutable.HashMap[String, AttributeType], keys: List[String]) {
	val relationships = new scala.collection.mutable.HashMap[String, BoundRelationship]()
	val queries = new scala.collection.mutable.HashMap[String, BoundQuery]()

	def this(e: Entity) {
		this(new scala.collection.mutable.HashMap[String, AttributeType](), e.keys)

		e.attributes.foreach((a) => {
			attributes.get(a.name) match {
				case Some(_) => throw new DuplicateAttributeException(e.name, a.name)
				case None => this.attributes.put(a.name, a.attrType)
			}
		})
	}
}

case class BoundQuery

abstract class BoundPredicate
object BoundThisEqualityPredicate extends BoundPredicate
case class BoundEqualityPredicate(attributeName: String, value: FixedValue) extends BoundPredicate

case class BoundFetch(entity: BoundEntity, child: Option[BoundFetch], relation: Option[BoundRelationship]) {
	val predicates = new scala.collection.mutable.ArrayBuffer[BoundPredicate]
}

object Binder {
	val logger = Logger.getLogger("scads.binding")

	def bind(spec: Spec) {
		/* Bind entities into a map and check for duplicate names */
		val entityMap = new scala.collection.mutable.HashMap[String, BoundEntity]()
		spec.entities.foreach((e) => {
			entityMap.get(e.name) match
			{
				case Some(_) => throw new DuplicateEntityException(e.name)
				case None => entityMap.put(e.name, new BoundEntity(e))
			}
		})

		/* Bind relationships to the entities they link, check for bad entity names and duplicate relationship names */
		spec.relationships.foreach((r) => {
			entityMap.get(r.from) match {
				case None => throw new UnknownEntityException(r.from)
				case Some(entity) => entity.relationships.put(r.name, new BoundRelationship(r.to, r.cardinality))
			}

			entityMap.get(r.to) match {
				case None => throw new UnknownEntityException(r.to)
				case Some(entity) => entity.relationships.put(r.name, new BoundRelationship(r.from, r.cardinality))
			}
		})

		val orphanQueryMap = new scala.collection.mutable.HashMap[String, BoundQuery]()
		spec.queries.foreach((q) => {
			/* Extract all Parameters from Predicates */
			val predParameters: List[Parameter] =
				q.fetch.predicates.map(
					_ match {
						case EqualityPredicate(op1, op2) => {
							val p1 = op1 match {case p: Parameter => Array(p); case _ => Array[Parameter]()}
							val p2 = op2 match {case p: Parameter => Array(p); case _ => Array[Parameter]()}
							p1 ++ p2
						}
					}).flatten
			/* Extract possible parameter from the limit clause */
			val limitParameters: Array[Parameter] =
				q.fetch.range match {
					case Limit(p, _) => p match {case p: Parameter => Array(p); case _ => Array[Parameter]()}
					case Paginate(p, _) => p match {case p: Parameter => Array(p); case _ => Array[Parameter]()}
					case Unlimited => Array[Parameter]()
				}
			val parameters = predParameters ++ limitParameters

			/* Ensure any duplicate parameter names are actually the same parameter */
			if(Set(parameters: _*).size != Set(parameters.map(_.name): _*).size)
				throw new DuplicateParameterException(q.name)

			/* Ensure that parameter ordinals are contiguious starting at 1 */
			parameters.sort(_.ordinal > _.ordinal).foldRight(1)((p: Parameter, o: Int) => {
				logger.debug("Ordinal checking, found " + p + " expected " + o)
				if(p.ordinal != o)
					throw new BadParameterOrdinals(q.name)
				o + 1
			})

			/* Build the fetch tree and alias map */
			val fetchAliases = new scala.collection.mutable.HashMap[String, BoundFetch]()
			val duplicateAliases = new scala.collection.mutable.HashSet[String]()

			val attributeMap = new scala.collection.mutable.HashMap[String, BoundFetch]()
			val duplicateAttributes = new scala.collection.mutable.HashSet[String]()

			val fetchTree: BoundFetch = q.fetch.joins.foldRight[(Option[BoundFetch], Option[String])]((None,None))((j: Join, child: (Option[BoundFetch], Option[String])) => {
				logger.debug("Looking for relationship " + child._2 + " in " + j + " with child " + child._1)
				val entity = entityMap.get(j.entity) match {
					case Some(e) => e
					case None => throw new UnknownEntityException(j.entity)
				}
				val relationship: Option[BoundRelationship] = child._2 match {
					case None => None
					case Some(relName) => entity.relationships.get(relName) match {
						case Some(rel) => Some(rel)
						case None => throw new UnknownRelationshipException(relName)
					}
				}

				val fetch = new BoundFetch(entity, child._1, relationship)
				val relToParent = if(j.relationship == null)
					None
				else
					Some(j.relationship)

				if(!duplicateAliases.contains(j.entity)) {
					fetchAliases.get(j.entity) match {
						case None => fetchAliases.put(j.entity, fetch)
						case Some(_) => {
							logger.debug("Fetch alias " + j.entity + " is ambiguious in query " + q.name + " and therefore can't be used in predicates")
							fetchAliases -= j.entity
							duplicateAliases += j.entity
						}
					}
				}
				if(j.alias != null)
					fetchAliases.get(j.alias) match {
						case None => fetchAliases.put(j.alias, fetch)
						case Some(_) => throw new AmbiguiousJoinAlias(q.name, j.alias)
					}

				entity.attributes.keys.foreach((a) => {
					if(!duplicateAttributes.contains(a))
						attributeMap.get(a) match {
							case None => attributeMap.put(a, fetch)
							case Some(_) => {
								logger.debug("Attribute " + a + " is ambiguious in query " + q.name + " and therefore can't be used in predicates without a fetch specifier")
								attributeMap -= a
								duplicateAttributes += a
							}
						}
				})

				(Some(fetch), relToParent)
			})._1.get
			logger.debug("Generated fetch tree for " + q.name + ": " + fetchTree)

			/* Check this parameter typing */
			val thisTypes: List[String] = q.fetch.predicates.map(
				_ match {
					case EqualityPredicate(Field(null, thisType), ThisParameter) => Array(thisType)
					case EqualityPredicate(ThisParameter, Field(null, thisType)) => Array(thisType)
					case _ => Array[String]()
				}).flatten
			logger.debug("this types detected for " + q.name + ": " + Set(thisTypes))

			val thisType: Option[String] = Set(thisTypes: _*).size match {
					case 0 => None
					case 1 => Some(thisTypes.head)
					case _ => throw new AmbigiousThisParameter(q.name)
				}
			logger.debug("Detected thisType of " + thisType + " for query " + q.name)

			/* Helper functions for identifying the BoundFetch that is being referenced in a given predicate */
			def resolveFetch(alias: String):BoundFetch = {
				if(duplicateAliases.contains(alias))
					throw new AmbiguiousJoinAlias(q.name, alias)
				fetchAliases.get(alias) match {
					case None => throw UnknownFetchAlias(q.name, alias)
					case Some(bf) => bf
				}
			}

			def resolveField(f:Field):BoundFetch = {
				if(f.entity == null) {
					if(duplicateAttributes.contains(f.name))
						throw new AmbiguiousAttribute(q.name, f.name)
					return attributeMap.get(f.name) match {
						case None => throw new UnknownAttributeException(q.name, f.name)
						case Some(bf) => bf
					}
				}
				else {
					val bf = resolveFetch(f.entity)
					if(!bf.entity.attributes.contains(f.name))
						throw UnknownAttributeException(q.name, f.name)
					return bf
				}
			}

			/* Helper function for assigning and validating parameter types */
			val paramTypes = new scala.collection.mutable.HashMap[String, AttributeType]
			def addAndType(f: Field, v:FixedValue) {
				val fetch = resolveField(f)

				(fetch.entity.attributes.get(f.name), paramTypes.get(f.name)) match {
					case (Some(t1), Some(t2)) => if(t1 != t2) throw InconsistentParameterTyping(q.name, f.name)
					case (Some(t), None) => paramTypes.put(f.name, t)
					case _ => throw UnknownAttributeException(q.name, f.name)
				}
				fetch.predicates.append(BoundEqualityPredicate(f.name, v))
			}

			/* Bind predicates to the proper node of the Fetch Tree */
			q.fetch.predicates.foreach( _ match {
				case EqualityPredicate(Field(null, alias), ThisParameter) => resolveFetch(alias).predicates.append(BoundThisEqualityPredicate)
			  	case EqualityPredicate(ThisParameter, Field(null, alias)) => resolveFetch(alias)
				case EqualityPredicate(f: Field, v: FixedValue) => addAndType(f,v)
				case EqualityPredicate(v :FixedValue, f: Field) => addAndType(f,v)
				case usp: Predicate => throw UnsupportedPredicateException(q.name, usp)
			})
		})
	}

}