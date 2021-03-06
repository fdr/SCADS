package edu.berkeley.cs.avro

import org.apache.avro.generic.{GenericContainer, IndexedRecord}

package object runtime {
  type TypedSchema[T] = org.apache.avro.TypedSchema[T]
  implicit def genericContainerSchema[C <: GenericContainer](implicit r: Manifest[C]): TypedSchema[C] = {
    val schema = TypedSchemas.findSchema[C]
    new TypedSchema[C](schema)
  }

  implicit def toRichIndexedRecord[RecType <: IndexedRecord](record: RecType) = new RichIndexedRecord(record)
  implicit def toJsonObject(json: String) = new JsonObject(json)
  implicit def toOption[A](a: A) = Option(a)
}
