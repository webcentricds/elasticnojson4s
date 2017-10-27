package com.charter.sia.es

import java.util.concurrent.atomic.AtomicInteger

import com.charter.sia.helper.EsObj
import com.charter.sia.model.{Address, Person}
import com.sksamuel.elastic4s.ElasticDsl._
import org.scalatest.{FunSuite, Matchers}

import scala.collection.mutable.ListBuffer

class EsClientTest extends FunSuite with Matchers with EsTestBase {
  private val indexName = "esclienttest-index"
  private val indexType = "esclienttest-type"
  private val esIndex = new EsIndex(tcpClient)
  private val esClient = new EsClient[Person](tcpClient, indexName, indexType, classOf[Person])

  test("Test Scan") {
    esIndex.deleteIfExists(indexName)
    esIndex.exists(indexName) should be (false)

    val num = 4
    val list = EsTestBase.randomPersons(num)
    val ids = list.map(esObj => esObj.id)
    esClient.upsertEsObjs(list.iterator).await(EsClientMaps.awaitDur)
    sleep(3)
    esClient.countIndex should be (num)
    esClient.countIndexType should be (num)

    val ai = new AtomicInteger(0)
    val consumer = (esObj: EsObj[Person]) => println(ai.getAndIncrement() + ": " + esObj.id)
    esClient.scan(consumer, 0, Option(idsQuery(list.head.id)))

    esIndex.delete(indexName)
  }

  test("Test Get and Delete by Query") {
    esIndex.deleteIfExists(indexName)
    esIndex.exists(indexName) should be (false)

    val num = 4
    val list = EsTestBase.randomPersons(num)
    val ids = list.map(esObj => esObj.id)
    esClient.upsertEsObjs(list.iterator).await(EsClientMaps.awaitDur)
    sleep(3)
    esClient.countIndex should be (num)
    esClient.countIndexType should be (num)

    val subList = List(list.head, list(1))
    //print("subList", subList)
    subList.size should be (2)
    val subIds = subList.map(esObj => esObj.id)
    subIds.size should be (2)
    val retSubList = esClient.getEsObjsByQuery(idsQuery(subIds)).toList
    //print("retSubList", retSubList)
    retSubList.size should be (subList.size)

    esClient.deleteByQuery(Option(idsQuery(subIds)))
    sleep(3)
    esClient.countIndex should be (2)
    esClient.countIndexType should be (2)

    esIndex.delete(indexName)
  }

  test("Test Notifications") {
    esIndex.deleteIfExists(indexName)
    esIndex.exists(indexName) should be (false)

    val num = 100
    val list = EsTestBase.randomPersons(num)
    val map = asMap(list)
    esClient.upsertEsObjs(list.iterator).await(EsClientMaps.awaitDur)
    sleep(3)
    esClient.countIndex should be (num)
    esClient.countIndexType should be (num)

    val listBuffer = new ListBuffer[String]
    val consumer = (esObj: EsObj[Person]) => {
      map.contains(esObj.id) should be (true)
      map(esObj.id) should be (esObj.data)
      listBuffer += esObj.id
    } : Unit
    esClient.notifyEsObjs(consumer)
    listBuffer.toList.size should be (num)

    esIndex.delete(indexName)
  }

  test("Test Upserts") {
    esIndex.deleteIfExists(indexName)
    esIndex.exists(indexName) should be (false)

    val num = 100
    val list = EsTestBase.randomPersons(num)
    esClient.upsertEsObjs(list.iterator).await(EsClientMaps.awaitDur)
    sleep(3)
    esClient.countIndex should be (num)
    esClient.countIndexType should be (num)

    val alteredList = EsTestBase.alterPersons(list)
    esClient.upsertEsObjs(alteredList.iterator).await(EsClientMaps.awaitDur)
    sleep(3)
    esClient.countIndex should be (num)
    esClient.countIndexType should be (num)
    alteredList.size should be (list.size)

    val retAlteredList = esClient.getEsObjsAll().toList
    retAlteredList.size should be (list.size)
    val retAlteredMap = asMap(retAlteredList)
    for(esObj <- alteredList) esObj.data should be (retAlteredMap(esObj.id))

    esClient.deleteIndexType()
    sleep(3)
    esClient.countIndex should be (0)
    esClient.countIndexType should be (0)

    esIndex.delete(indexName)
  }

  test("Test Upsert") {
    esIndex.deleteIfExists(indexName)
    esIndex.exists(indexName) should be (false)

    val stew = new EsObj(EsTestBase.uniquePtr, new Person("Stew", "English", new Address("450 E Ohio st.", "Marquette", "MI", "49855")))
    val stew2 = new EsObj(stew.id, new Person("Stewart", "English", new Address("450 E Ohio st.", "Marquette", "MI", "49855")))
    val list = List(new EsObj(EsTestBase.uniquePtr, new Person("Dave", "English", new Address("450 E Ohio st.", "Marquette", "MI", "49855"))),
                    new EsObj(EsTestBase.uniquePtr, new Person("Carla", "English", new Address("450 E Ohio st.", "Marquette", "MI", "49855"))))

    esClient.upsertEsObj(stew).await(EsClientMaps.awaitDur)
    sleep(1)
    esClient.countIndex should be (1)
    esClient.countIndexType should be (1)
    esClient.countQuery(idsQuery(stew.id)) should be (1)

    val opt1 = esClient.getEsObj(stew.id)
    opt1 should not be None
    opt1.get.data should be (stew.data)

    esClient.upsertEsObj(stew2).await(EsClientMaps.awaitDur)
    sleep(1)
    esClient.countIndex should be (1)
    esClient.countIndexType should be (1)
    esClient.countQuery(idsQuery(stew2.id)) should be (1)
    val opt2 = esClient.getEsObj(stew2.id)
    opt2 should not be None
    opt2.get.data should be (stew2.data)

    esIndex.delete(indexName)
  }

  test("Test Updates") {
    esIndex.deleteIfExists(indexName)
    esIndex.exists(indexName) should be (false)

    val list = List(new EsObj(EsTestBase.uniquePtr, new Person("Dave", "English", new Address("450 E Ohio st.", "Marquette", "MI", "49855"))),
                    new EsObj(EsTestBase.uniquePtr, new Person("Carla", "English", new Address("450 E Ohio st.", "Marquette", "MI", "49855"))))
    val ids = list.map(esObj => esObj.id)
    val map = asMap(list)
    esClient.insertEsObjs(list.iterator).await(EsClientMaps.awaitDur)
    sleep(3)
    esClient.countIndex should be (list.size)
    esClient.countIndexType should be (list.size)
    esClient.countQuery(idsQuery(ids)) should be (list.size)

    val retList = esClient.getEsObjsByIds(ids).toList
    for(esObj <- retList)
      map(esObj.id) should be (esObj.data)

    esClient.deleteByIds(ids)
    sleep(3)
    esClient.countIndex should be (0)
    esClient.countIndexType should be (0)
    esIndex.delete(indexName)
  }

  test("Test Single Update") {
    esIndex.deleteIfExists(indexName)
    esIndex.exists(indexName) should be (false)

    val person = new Person("Dave", "English", new Address("450 E Ohio st.", "Marquette", "MI", "49855"))
    val esObj = new EsObj(EsTestBase.uniquePtr, person)
    esClient.insertEsObj(esObj).await(EsClientMaps.awaitDur)
    sleep(3)
    esClient.countIndex should be (1)
    esClient.countIndexType should be (1)
    esClient.countQuery(idsQuery(esObj.id)) should be (1)

    val retEsObjOption = esClient.getEsObj(esObj.id)
    retEsObjOption should not be None
    retEsObjOption.get.data should be (esObj.data)

    val updatePerson = new Person("Dan", "English", new Address("450 E Ohio st.", "Marquette", "MI", "49855"))
    val updateEsObj = new EsObj(esObj.id, updatePerson)
    esClient.updateEsObj(updateEsObj).await(EsClientMaps.awaitDur)
    sleep(3)
    val opt = esClient.getEsObj(esObj.id)
    opt should not be None
    opt.get.data should be (updatePerson)
    esClient.countIndex should be (1)
    esClient.countIndexType should be (1)
    esClient.countQuery(idsQuery(esObj.id)) should be (1)

    esIndex.delete(indexName)
  }

  test("Test All") {
    esIndex.deleteIfExists(indexName)
    esIndex.exists(indexName) should be (false)

    val id = EsTestBase.uniquePtr
    esClient.insertEsObj(new EsObj(id, EsTestBase.randomPerson())).await(EsClientMaps.awaitDur)
    sleep(3)
    esClient.countIndex should be (1)
    esClient.countIndexType should be (1)
    esClient.countQuery(idsQuery(id)) should be (1)

    val num = 100
    val esObjs = EsTestBase.randomPersons(num)
    esClient.insertEsObjs(esObjs.iterator).await(EsClientMaps.awaitDur)
    sleep(3)
    esClient.countIndex should be (1 + num)
    esClient.countIndexType should be (1 + num)
    val ids = esObjs.map(esObj => esObj.id)
    //println("ids.size: " + ids.size)
    //ids.foreach(id => println("id: " + id))
    ids.size should be (num)

    esClient.countQuery(idsQuery(id)) should be (1)
    esClient.countQuery(idsQuery(ids.head)) should be (1)
    esClient.countQuery(idsQuery(id, ids.head)) should be (2)
    esClient.countQuery(idsQuery(ids)) should be (num)

    esIndex.delete(indexName)
  }

  private def print(title: String,
                    esObjs: List[EsObj[Person]]): Unit = {
    println(title)
    esObjs.foreach(esObj => println(" [" + esObj.id + "]: " + esObj.data.toString))
  }

  private def asMap(esObjs: List[EsObj[Person]]): Map[String, Person] = {
    val map = scala.collection.mutable.Map[String, Person]()
    esObjs.foreach(esObj => map += (esObj.id -> esObj.data))
    map.toMap
  }
}