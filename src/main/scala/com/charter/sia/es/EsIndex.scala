package com.charter.sia.es

import com.sksamuel.elastic4s.ElasticDsl._
import com.sksamuel.elastic4s.TcpClient
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse
import org.elasticsearch.action.admin.indices.delete.DeleteIndexResponse
import org.slf4j.LoggerFactory

class EsIndex(val tcpClient: TcpClient) {
  private val LOG = LoggerFactory.getLogger(classOf[EsIndex])

  def exists(indexName: String): Boolean =
    tcpClient.execute {
      index exists indexName
    }.await.isExists

  def create(indexName: String): CreateIndexResponse =
    tcpClient.execute {
      LOG.info("Creating Index: " + indexName)
      createIndex(indexName)
    }.await

  def delete(indexName: String): DeleteIndexResponse =
    tcpClient.execute {
      LOG.info("Deleting Index: " + indexName)
      deleteIndex(indexName)
    }.await

  def deleteIfExists(indexName: String): Unit = {
    if (exists(indexName)) {
      LOG.info("Deleting Existing Index: " + indexName)
      delete(indexName)
    } else {
      LOG.info("Deleting Existing Index " + indexName + " does not exists.  Doing Nothing.")
    }
  }
}
