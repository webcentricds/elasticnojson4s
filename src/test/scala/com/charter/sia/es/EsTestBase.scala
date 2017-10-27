package com.charter.sia.es

import java.util.UUID

import com.charter.sia.helper.EsObj
import com.charter.sia.model.{Address, Person}
import com.sksamuel.elastic4s.{ElasticsearchClientUri, TcpClient}
import org.elasticsearch.common.settings.Settings

import scala.collection.mutable.ListBuffer

trait EsTestBase {
  val settingsBuilder: Settings.Builder = Settings.builder().put("cluster.name", "docker-cluster")
  val tcpClient: TcpClient = TcpClient.transport(settingsBuilder.build, ElasticsearchClientUri("localhost", 9300))

  def sleep(seconds: Int): Unit = Thread.sleep(seconds * 1000)
}

object EsTestBase {
  def alterPersons(esObjs: List[EsObj[Person]]): List[EsObj[Person]] =
    esObjs.map(esObj => new EsObj[Person](esObj.id, new Person(esObj.data.firstName + "xxx",
                                                               esObj.data.lastName,
                                                               esObj.data.address)))
  def randomPersons(num: Int): List[EsObj[Person]] = {
    val listBuffer = new ListBuffer[EsObj[Person]]
    for(i <- 0 until num) listBuffer += new EsObj[Person](uniquePtr, randomPerson())
    listBuffer.toList
  }
  def randomPerson() = new Person(EsTestBase.uniquePtr,
                                  EsTestBase.uniquePtr,
                                  new Address(EsTestBase.uniquePtr,
                                              EsTestBase.uniquePtr,
                                              EsTestBase.uniquePtr,
                                              EsTestBase.uniquePtr))
  def uniquePtr: String = UUID.randomUUID().toString.replace("-", "")
}
