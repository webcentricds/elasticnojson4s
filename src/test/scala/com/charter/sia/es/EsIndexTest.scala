package com.charter.sia.es

import org.elasticsearch.action.admin.indices.create.CreateIndexResponse
import org.elasticsearch.action.admin.indices.delete.DeleteIndexResponse
import org.scalatest.{FunSuite, Matchers}

class EsIndexTest extends FunSuite with Matchers with EsTestBase {

  val indexName = "EsIndexTest-Index"
  val indexType = "EsIndexTest-Type"

  val esIndex = new EsIndex(tcpClient)

  ignore("Delete Index") {

    val indexExists1 = esIndex.exists(indexName)
    indexExists1 should be (true)

    val deleteResultResonse: DeleteIndexResponse = esIndex.delete(indexName)
    deleteResultResonse.isAcknowledged should be(true)

    val indexExists2 = esIndex.exists(indexName)
    indexExists2 should be (false)
  }

  ignore("Create Index") {
    val createIndexResonse: CreateIndexResponse = esIndex.create(indexName)
    createIndexResonse.isAcknowledged should be (true)

    val indexExists = esIndex.exists(indexName)
    indexExists should be (true)
  }

  ignore("Delete Index If Exists") {
    val indexExists1 = esIndex.exists(indexName)
    indexExists1 should be (true)

    esIndex.deleteIfExists(indexName)

    val indexExists2 = esIndex.exists(indexName)
    indexExists2 should be (false)
  }
}
