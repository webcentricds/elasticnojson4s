# elasticnojson4s
This is an elastic search library optimized for speed and reduced memory footprint implemented in elastic4s

There is a **src/test/docker/docker-compose.yml** file you can use to spin up
a cluster of two elasticsearch 5.6 nodes so the unit tests will compile.

The unit tests depend on an elasticsearch cluster at **elastic5-6:9200** for http
connection and **elastic5-6:9200**.  You can put an entry in your **/etc/hosts**
file to "point" to the location of the docker server running the above docker-compose.yml
file.

* ElasticSearch Version: 5.6
* Scala Version: 2.12.2
* Scala Library Version: 2.12.2
* Scalatest Version: 3.0.4

Here are some features for this project:

1. It is a fully functional implementation.
1. It contains a **docker-compose.yml** file used to start up
a test elasticsearch cluster in docker to run unit tests.
1. It is optimized for speed and efficient use of memory
1. Project structure is maven with pom.xml files.
1. Scalatest framework is used for unit tests.

