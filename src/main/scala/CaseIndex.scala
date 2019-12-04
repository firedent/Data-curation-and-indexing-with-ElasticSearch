import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import scalaj.http._
import scala.xml.XML
import org.json4s._
import org.json4s.native.JsonMethods._
import org.json4s.native.Serialization
import org.json4s.JsonDSL._

object CaseIndex {
  // the entity typr required
  val SPECIFIED_ENTIRY_TYPES = Set("PERSON", "ORGANIZATION", "LOCATION")
  // protocol request uesd
  val HTTP = "http://"
  //  address of elasticsearch
  val esServer = "localhost:9200"
  // address of nlp server
  val nlpServer = "localhost:9000"
  // type of request body in headers
  val MIME_JSON = "application/json"

  def main(args: Array[String]) {
    println("V7")
    // get the full path of directory with cases
    val inputFile = args(0)
    // Create Conf
    val conf = new SparkConf().setAppName("CaseIndex").setMaster("local")
    // Create a Scala Spark Context with conf
    val sc = new SparkContext(conf)
    // reading XML file from specified path
    val files = sc.wholeTextFiles(inputFile)
    // create new index
    createIndexWithMappings()
    // parsing XML string to XML object
    val XMLs = files.map(x => (x._1, XML.loadString(x._2)))
    // analyse XML object and get the
    val esRecord = XMLs.map(x => analyseXML(x._1, x._2))
    esRecord.foreach(x => updateDocument(x))
    println("[FINISH]")
  }

  // send a string to NLP server and request the analysation
  // after that, renturn a set contains token and it's 'ner'
  // eg: this method receiving "John travelled to Sydney" will return set(("John","person"),("Sydney","location"))
  def analyseViaNLP(s: String): scala.collection.mutable.Set[(String, String)] = {
    val ners = scala.collection.mutable.Set[(String, String)]()
    // construct URL params
    // the value of params
    val urlParamsValue = Map("annotators" -> "ner", "outputFormat" -> "json")
    // convert Map object to json string
    val urlParamsKey = Map("properties" -> compact(render(urlParamsValue)))
    // set request headers
    val requestHeads = Map("Content-Type" -> "text/plain; charset=utf-8")
    // build HTTP request
    val requestAnalyse = Http(HTTP + nlpServer).method("POST").headers(requestHeads).postData(s).params(urlParamsKey).timeout(1000, 60000)
    // send request
    val response = requestAnalyse.execute()
    // remove irrelevant field from json
    val jsonPrased = parse(response.body) \ "sentences" \ "tokens" removeField {
      // keep 'word' and 'ner'
      case ("word", _) => false
      case ("ner", JString(x)) => false
      case _ => true
    }
    // convert json to scala object
    val result = jsonPrased.values.asInstanceOf[List[List[Map[String, String]]]]
    for (i <- result) {
      for (j <- i) {
        // keep the tuple whose value exists in set(person,location,organization)
        if (SPECIFIED_ENTIRY_TYPES.contains(j.apply("ner"))) {
          ners.add((j.apply("word"), j.apply("ner")))
        }
      }
    }
    ners
  }

  // analyse XML instance and return a Map instance containing all fields
  def analyseXML(fn: String, xm: scala.xml.Elem): scala.collection.mutable.Map[String, Any] = {
    // Map initialization
    val result = scala.collection.mutable.Map[String, Any]()
    // extract filename, name, AustLII
    result += ("filename" -> new java.io.File(fn).getName)
    result += ("name" -> (xm \\ "name").text)
    result += ("AustLII" -> (xm \\ "AustLII").text)
    // extract catchphrases and convert each nodes of it into string
    val catchphrases = (xm \\ "catchphrase").map(_.text)
    result += ("catchphrases" -> catchphrases)
    // extract sentences and convert each nodes of it into string
    val sentences = (xm \\ "sentence").map(_.text)
    result += ("sentences" -> sentences)

    // initialization three entry fields
    for (k <- SPECIFIED_ENTIRY_TYPES.map(_.toLowerCase)) {
      result += (k -> List())
    }

    // analyse each catchphrase and reduce the results from mulit-set to single-set
    val nlpCatchphrases = catchphrases.map(analyseViaNLP).reduce(_ ++ _)
    // analyse each sentences and reduce the results from mulit-set to single-set
    val nlpSentences = sentences.map(analyseViaNLP).reduce(_ ++ _)
    // merge nlpCatchphrases and nlpSentences, and group by 'ner'
    val finalEntry = (nlpSentences ++ nlpCatchphrases).groupBy[String](_._2)
    // extract all values with same 'entry name' and convert to a list containing them
    for (k <- finalEntry.keys) {
      result += (k.toLowerCase -> finalEntry.apply(k).map(_._1).toList)
    }
    result
  }

  // create index and mapping in elasticsearch
  def createIndexWithMappings(): Unit = {
    // data type of field
    val textType = "type" -> "text"
    // define mapping
    val pro =
      "properties" ->
        ("filename" -> textType) ~
          ("name" -> textType) ~
          ("AustLII" -> textType) ~
          ("catchphrases" -> textType) ~
          ("sentences" -> textType) ~
          ("person" -> textType) ~
          ("location" -> textType) ~
          ("organization" -> textType)
    // construct final json data construct
    val json = "mappings" -> ("cases" -> pro)
    // convert json object to json string
    val createIndexRequestBody = compact(render(json))
    // create HTTP request
    val create_index_request = Http(HTTP + esServer + "/legal_idx").method("PUT").header("Content-Type", MIME_JSON).put(createIndexRequestBody)
    // send HTTP request and get response status
    val status = create_index_request.execute().statusLine
    println(status)
  }

  // update record to elasticsearch
  def updateDocument(record: scala.collection.mutable.Map[String, Any]): Unit = {
    implicit val formats = org.json4s.DefaultFormats
    // convert scala object to json
    val postJson = Serialization.write(record)
    // create HTTP request for updating json
    val updateDocumentRequest = Http(HTTP + esServer + "/legal_idx/cases/" + record.apply("filename")).method("PUT").header("Content-Type", MIME_JSON).put(postJson)
    // send HTTP request and get response status
    val status = updateDocumentRequest.execute().statusLine
    println("\nUPDATE " + record.apply("filename") + " :\t" + status)
    println("\tPERSON: " + record.apply("person"))
    println("\tLOCATION: " + record.apply("location"))
    println("\tORGANIZATION: " + record.apply("organization"))
  }
}