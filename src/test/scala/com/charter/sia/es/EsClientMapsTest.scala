package com.charter.sia.es

import java.util.UUID

import com.charter.sia.helper.{EsMap, MapObj}
import com.sksamuel.elastic4s.{ElasticsearchClientUri, TcpClient}
import org.elasticsearch.common.settings.Settings
import org.scalatest.{FunSuite, Matchers}

class EsClientMapsTest extends FunSuite with Matchers {
  private val indexName = "test-index"
  private val indexType = "test-type"
  private val fnf = "firstName"
  private val lnf = "lastName"

  test("Test Insert/Update/Upsert Behavior") {
    val settingsBuilder = Settings.builder().put("cluster.name", "docker-cluster")
    val tcpClient = TcpClient.transport(settingsBuilder.build, ElasticsearchClientUri("localhost", 9300))
    val esIndex = new EsIndex(tcpClient)
    val esClientMaps = new EsClientMaps(tcpClient)

    esIndex.deleteIfExists(indexName)

    val map1 = Map(fnf -> "Dave", lnf -> "Peterson")
    val response1 = esClientMaps.insertMap(indexName, indexType, map1)
    println("response1: " + response1)
    val testId = response1.id
    println("testId: " + testId)

    sleep(1)
    val opt = esClientMaps.getEsMap(indexName, indexType, testId)
    opt should not be None
    val esMap: EsMap = opt.get
    println("esMap: " + MapObj.print(esMap.map))

    sleep(2)
    val esMaps = esClientMaps.getEsMaps(indexName, indexType).toList
    println("retVals[" + esMaps.size + "]:")
    esMaps.foreach(esMap => MapObj.print(esMap.map))
  }
  def uniqePtr: String = UUID.randomUUID().toString.replace("-", "")
  def sleep(seconds: Int): Unit = Thread.sleep(seconds * 1000)
}
