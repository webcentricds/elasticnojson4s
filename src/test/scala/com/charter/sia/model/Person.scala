package com.charter.sia.model

import org.apache.commons.lang3.builder.{EqualsBuilder, HashCodeBuilder}

class Address(val street: String,
              val city: String,
              val state: String,
              val zip: String) {
  override def toString: String = street + ", " + city + " " + state + " " + zip
  def canEqual(any: Any): Boolean = any.isInstanceOf[Address]
  override def equals(that: Any): Boolean =
    that match {
      case that: Address => that.canEqual(this) &&
        new EqualsBuilder()
          .append(this.street, that.street)
          .append(this.city, that.city)
          .append(this.state, that.state)
          .append(this.zip, that.zip)
          .isEquals
      case _ => false
    }
  override def hashCode: Int =
    new HashCodeBuilder()
      .append(street)
      .append(city)
      .append(state)
      .append(zip)
      .toHashCode
}

class Person(val firstName: String,
             val lastName: String,
             val address: Address) {
  override def toString: String = firstName + " " + lastName + " Address: " + address.toString
  def canEqual(any: Any): Boolean = any.isInstanceOf[Person]
  override def equals(that: Any): Boolean =
    that match {
      case that: Person => that.canEqual(this) &&
        new EqualsBuilder()
          .append(this.firstName, that.firstName)
          .append(this.lastName, that.lastName)
          .append(this.address, that.address)
          .isEquals
      case _ => false
    }
  override def hashCode: Int =
    new HashCodeBuilder()
      .append(firstName)
      .append(lastName)
      .append(address)
      .toHashCode
}
