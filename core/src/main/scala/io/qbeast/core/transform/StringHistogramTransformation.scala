package io.qbeast.core.transform

import com.fasterxml.jackson.core.{JsonFactory, JsonGenerator, JsonParser, TreeNode}
import com.fasterxml.jackson.databind.annotation.{JsonDeserialize, JsonSerialize}
import com.fasterxml.jackson.databind.deser.std.StdDeserializer
import com.fasterxml.jackson.databind.jsontype.TypeSerializer
import com.fasterxml.jackson.databind.node.ArrayNode
import com.fasterxml.jackson.databind.ser.std.StdSerializer
import com.fasterxml.jackson.databind.{DeserializationContext, SerializerProvider}

import scala.collection.Searching._

@JsonSerialize(using = classOf[StringHistogramTransformationSerializer])
@JsonDeserialize(using = classOf[StringHistogramTransformationDeserializer])
case class StringHistogramTransformation(stringHist: Array[String]) extends Transformation {

  /**
   * Converts a real number to a normalized value.
   *
   * @param value a real number to convert
   * @return a real number between 0 and 1
   */
  override def transform(value: Any): Double = {
    val v: String = value match {
      case s: String => s
      case null => "null"
      case _ => value.toString
    }

    stringHist.search(v) match {
      case Found(foundIndex) => foundIndex.toDouble / stringHist.length
      case InsertionPoint(insertionPoint) => linearMapping(insertionPoint)
    }
  }

  private def linearMapping(pos: Int): Double = {
    // TODO, linearly mapping string position within its corresponding bin
    if (pos == 0) 0d
    else if (pos == stringHist.length) 1d
    else pos.toDouble / stringHist.length
  }

  /**
   * This method should determine if the new data will cause the creation of a new revision.
   *
   * @param newTransformation the new transformation created with statistics over the new data
   * @return true if the domain of the newTransformation is not fully contained in this one.
   */
  override def isSupersededBy(newTransformation: Transformation): Boolean = {
    // TODO: When do we need to change the histogram?
    false
  }

  /**
   * Merges two transformations. The domain of the resulting transformation is the union of this
   *
   * @param other Transformation
   * @return a new Transformation that contains both this and other.
   */
  override def merge(other: Transformation): Transformation = {
    // TODO: How do we merge two histograms?
    this
  }

}

class StringHistogramTransformationSerializer
    extends StdSerializer[StringHistogramTransformation](classOf[StringHistogramTransformation]) {
  val jsonFactory = new JsonFactory()

  override def serializeWithType(
      value: StringHistogramTransformation,
      gen: JsonGenerator,
      serializers: SerializerProvider,
      typeSer: TypeSerializer): Unit = {
    gen.writeStartObject()
    typeSer.getPropertyName
    gen.writeStringField(typeSer.getPropertyName, typeSer.getTypeIdResolver.idFromValue(value))

    gen.writeFieldName("stringHist")
    gen.writeStartArray()
    value.stringHist.foreach(gen.writeString)
    gen.writeEndArray()

    gen.writeEndObject()
  }

  override def serialize(
      value: StringHistogramTransformation,
      gen: JsonGenerator,
      provider: SerializerProvider): Unit = {
    gen.writeStartObject()

    gen.writeStartArray("stringHist")
    value.stringHist.foreach(gen.writeString)
    gen.writeEndArray()

    gen.writeEndObject()
  }

}

class StringHistogramTransformationDeserializer
    extends StdDeserializer[StringHistogramTransformation](
      classOf[StringHistogramTransformation]) {

  override def deserialize(
      p: JsonParser,
      ctxt: DeserializationContext): StringHistogramTransformation = {
    val stringHistBuilder = Array.newBuilder[String]

    val root: TreeNode = p.getCodec.readTree(p)
    root.get("stringHist") match {
      case an: ArrayNode =>
        (0 until an.size()).foreach(i => stringHistBuilder += an.get(i).asText())
    }

    StringHistogramTransformation(stringHistBuilder.result())
  }

}
