/*
 * Copyright 2021 Qbeast Analytics, S.L.
 */
package io.qbeast.transform

import com.fasterxml.jackson.annotation.JsonTypeInfo
import io.qbeast.model.{OrderedDataType, QDataType, StringDataType}

import java.util.Locale

object Transformer {

  private val transformersRegistry: Map[String, TransformerType] =
    Seq(LinearTransformer, HashTransformer).map(a => (a.transformerSimpleName, a)).toMap

  def apply(transformerTypeName: String, columnName: String, dataType: QDataType): Transformer = {

    val tt = transformerTypeName.toLowerCase(Locale.ROOT)
    transformersRegistry(tt)(columnName, dataType)
  }

  def apply(columnName: String, dataType: QDataType): Transformer = {
    getDefaultTransformerForType(dataType)(columnName, dataType)
  }

  def getDefaultTransformerForType(dataType: QDataType): TransformerType = transformersRegistry {
    dataType match {
      case _: OrderedDataType => LinearTransformer.transformerSimpleName
      case StringDataType => HashTransformer.transformerSimpleName
      case _ => throw new RuntimeException(s"There's not default transformer for $dataType")
    }

  }

}

private[transform] trait TransformerType {
  def transformerSimpleName: String

  def apply(columnName: String, dataType: QDataType): Transformer
}

@JsonTypeInfo(
  use = JsonTypeInfo.Id.CLASS,
  include = JsonTypeInfo.As.PROPERTY,
  property = "className")
trait Transformer extends Serializable {

  protected def transformerType: TransformerType
  def columnName: String
  def stats: ColumnStats
  def makeTransformation(row: String => Any): Transformation

  def maybeUpdateTransformation(
      currentTransformation: Transformation,
      row: Map[String, Any]): Option[Transformation] = {
    val newDataTransformation = makeTransformation(row)
    if (currentTransformation.isSupersededBy(newDataTransformation)) {
      Some(currentTransformation.merge(newDataTransformation))
    } else {
      None
    }
  }

  def spec: String = s"$columnName/${transformerType.transformerSimpleName}"

}

object NoColumnStats extends ColumnStats(Nil, Nil)

case class ColumnStats(names: Seq[String], columns: Seq[String]) extends Serializable {
  def getValues(row: Map[String, Any]): Seq[Any] = names.map(column => row(column))
}
