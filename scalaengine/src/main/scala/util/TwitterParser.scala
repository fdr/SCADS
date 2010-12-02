package edu.berkeley.cs.scads.util
package twitter

import scala.collection.mutable.{HashMap, ListBuffer}

import org.codehaus.jackson.{JsonFactory, JsonToken, JsonParseException}

object JsonParser {
  private val factory = new JsonFactory

  def parseJson(json: String): HashMap[String, Any] = {
    val parser = factory.createJsonParser(json)
    val rec = new HashMap[String, Any]

    var prefix: ListBuffer[String] = ListBuffer()
    var prefix_string: String = ""
    var full_fieldname: String = ""
    var fieldname: String = ""

    var running = true
    var parsable = true
    var currToken: JsonToken = null

    while(running) {
      try {
        currToken = parser.nextToken
        parsable = true
        if (currToken == null)
          running = false
      } catch {
        case jpe: JsonParseException =>
          parsable = false
          println("Found an unparsable token\n");
      }

      if (running && parsable) {
        currToken match {
          // Handle OBJECT start and end
          case JsonToken.START_OBJECT =>
            //println("Starting a new object");
            if (fieldname.length > 0) {
              prefix += fieldname
              if (prefix.size > 0)
                prefix_string = prefix.map(_ + '-').reduceRight(_ + _)
              else
                prefix_string = ""
            }

          case JsonToken.END_OBJECT =>
            //println("Ending an object");
            if (prefix.size > 0) {
              prefix.trimEnd(1)
            }
            if (prefix.size > 0)
              prefix_string = prefix.map(_ + '-').reduceRight(_ + _)
            else
              prefix_string = ""

          // Handle FIELD_NAME
          case JsonToken.FIELD_NAME =>
            fieldname = parser.getCurrentName
            full_fieldname = prefix_string + fieldname

          // Handle VALUE tokens
          case JsonToken.VALUE_STRING =>
            rec(full_fieldname) = parser.getText
          case JsonToken.VALUE_NUMBER_INT =>
            try
              rec(full_fieldname) = parser.getIntValue
            catch {
              case jpe: JsonParseException =>
              //case jpe: Exception =>
                rec(full_fieldname) = new BigInt(parser.getBigIntegerValue)
            }
          case JsonToken.VALUE_TRUE => 
            rec(full_fieldname) = true
          case JsonToken.VALUE_FALSE => 
            rec(full_fieldname) = false
          case JsonToken.VALUE_NULL =>
            rec(full_fieldname) = null
          case unexp => //println("Don't know how to populate field '" + fieldname + "', found: " + currToken)
        }
      }
    }

    return rec
  }
}
