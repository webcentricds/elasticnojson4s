package com.charter.sia.es

import java.util.UUID

import com.charter.sia.helper.MapObj
import com.charter.sia.model.{Address, Person}
import com.sksamuel.elastic4s.indexes.IndexDefinition
import com.sksamuel.elastic4s.{ElasticsearchClientUri, TcpClient}
import org.elasticsearch.action.support.WriteRequest.RefreshPolicy
import org.elasticsearch.common.settings.Settings
import org.scalatest.FunSuite

class EsExporterTest extends FunSuite /*with Matchers*/ {
  private val indexName = "test-index"
  private val indexType = "test-type"
  private val fnf = "firstName"
  private val lnf = "lastName"
  test("Hello World") {
    println("Hello World")
  }

  test("A Complete Elastic4s Example") {
    import com.sksamuel.elastic4s.ElasticDsl._
    //val client: HttpClient = HttpClient(ElasticsearchClientUri("localhost", 9200))
    //val client = TcpClient.transport(ElasticsearchClientUri("localhost", 9300))
    val settingsBuilder = Settings.builder().put("cluster.name", "docker-cluster")
    val tcpClient: TcpClient = TcpClient.transport(settingsBuilder.build, ElasticsearchClientUri("localhost", 9300))

    tcpClient.execute {
      deleteIndex(indexName)
    }.await

    val newPerson = new Person("Hoit", "Hoyle", new Address("2286 Dailey St.", "Superior", "CO", "80027"))
    val rbr1 = tcpClient.execute {
      bulk(
        indexInto(indexName / indexType).fields(MapObj.obj2Map(newPerson))
      ).refresh(RefreshPolicy.WAIT_UNTIL)
    }.await

    val people: List[Person] = List(new Person("David", "English", new Address("2286 Dailey St.", "Superior", "CO", "80027")),
                                    new Person("Carla", "English", new Address("2286 Dailey St.", "Superior", "CO", "80027")))
    val buldOps: List[IndexDefinition] = for(person <- people) yield { indexInto(indexName / indexType).fields(MapObj.obj2Map(person)) }
    val rbr2 = tcpClient.execute {
      bulk(buldOps: _*)
    }.await



//    client.execute {
//      create index indexName
//    }
//
//    // await is a helper method to make this operation synchronous instead of async
//    // You would normally avoid doing this in a real program as it will block your
//    // thread.  JUST USE IT FOR TESTING.
//    val indexResonse: RichBulkResponse = client.execute {
//      bulk(
//        indexInto(indexName / indexType).fields(fnf -> "Carla", lnf -> "English"),
//        indexInto(indexName / indexType).fields(fnf -> "David", lnf -> "English"),
//        indexInto(indexName / indexType).fields(fnf -> "David", lnf -> "Peterson")
//      ).refresh(RefreshPolicy.WAIT_UNTIL)
//    }.await
//    //println("indexResonse: " + indexResonse.toString)
//
//    val resp: RichSearchResponse = client.execute {
//      search(indexName).matchQuery(fnf, "David")
//    }.await
//
//    println("---- Search Hit Parsed ----")
//    resp.to[Person].foreach(println)
//
//    val countResp: RichSearchResponse = client.execute {
//      search(indexName -> indexType).from(0).size(0)
//    }.await
  }
//
//  def iterateResults(searchRequestBuilder: SearchRequestBuilder,
//                     consumer: (java.util.Map[String, AnyRef]) => Unit,
//                     scrollTimeout: TimeValue = TimeValue.timeValueMinutes(2),
//                     quitAfter: Long = 0): Unit = {
//
//    val scrollResponse = searchRequestBuilder.setScroll(scrollTimeout).execute.actionGet
//    val searchHits = scrollResponse.getHits.getHits
//    do {
//      searchHits.foreach{ sh =>
//        val map = sh.getSourceAsMap
//        consumer(map)
//      }
//      scrollResponse = client
//                               .prepareSearchScroll(scrollResponse.getScrollId)
//                               .setScroll(scrollTimeout)
//                               .execute()
//                               .actionGet
//      searchHits = scrollResponse.getHits.getHits
//    } while(searchHits.length != 0);
//  }
  def uniqePtr: String = UUID.randomUUID().toString.replace("-", "")
  def sleep(seconds: Int): Unit = Thread.sleep(seconds * 1000)
}