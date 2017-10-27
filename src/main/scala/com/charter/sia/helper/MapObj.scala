package com.charter.sia.helper

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.fasterxml.jackson.module.scala.experimental.ScalaObjectMapper

object MapObj {
  val mapper: ObjectMapper = (new ObjectMapper() with ScalaObjectMapper).registerModule(DefaultScalaModule)

  def map2Obj[T](map: Map[String, AnyRef], clazz: Class[T]): T =
    mapper.convertValue(map, clazz)

  def obj2Map[T](t: T): Map[String, AnyRef] =
    mapper.convertValue(t, classOf[Map[String, AnyRef]])

  def print(map: Map[String, AnyRef]): String =
    mapper.writerWithDefaultPrettyPrinter().writeValueAsString(map)
}