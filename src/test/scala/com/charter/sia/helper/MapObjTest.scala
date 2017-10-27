package com.charter.sia.helper

import org.scalatest.{FunSuite, Matchers}

class MapObjTest extends FunSuite with Matchers {

  test("Test Pretty Print") {
    val map = Map("keyA" -> "valueA", "keyB" -> "valueB")
    println(MapObj.print(map))
  }
}
