package com.charter.sia.es

import com.charter.sia.model.{Address, Person}
import org.scalatest.{FunSuite, Matchers}

class PersonTest extends FunSuite with Matchers {

  val addr1 = new Address("450 E Ohio st.", "Marquette", "MI", "49855")
  val addr2 = new Address("450 E Ohio st.", "Marquette", "MI", "49855")
  val addr3 = new Address("450 E Ohio st.", "Marquette", "MI", "49855")
  val addr4 = new Address("450 E Ohio st.", "Munising", "MI", "49855")

  test("Test Address Equality") {
    addr1 should be (addr2)
    addr3 should not be addr4
  }

  val person1 = new Person("Dave", "English", addr1)
  val person2 = new Person("Dave", "English", addr2)
  val person3 = new Person("Dan", "English", addr3)
  val person4 = new Person("Dave", "English", addr4)

  test("Test Person Equality") {
    person1 should be (person2)
    person3 should not be person4
  }
}
