import org.apache.spark.{SparkConf, SparkContext}
import org.json4s.JsonDSL._
import org.json4s._
import org.json4s.native.JsonMethods._
import org.json4s.native.Serialization
import scalaj.http._

import scala.xml.XML

object CaseIndex {
  val SPECIFIED_ENTIRY_TYPES = Set("PERSON","ORGANIZATION","LOCATION")
  val HTTP = "http://"
  val esServer = "localhost:9200"
  val nlpServer = "localhost:9000"
  val MIME_JSON = "application/json"

  def main(args: Array[String]) {
    println("V3")
    val inputFile = args(0)
    val conf = new SparkConf().setAppName("CaseIndex").setMaster("local") // Create a Scala Spark Context.
    val sc = new SparkContext(conf)
    val files = sc.wholeTextFiles(inputFile)
    createIndexWithMappings()
    val XMLs = files.map(x=>(x._1, XML.loadString(x._2)))
    val esRecord = XMLs.map(x=>analyseXML(x._1,x._2))
    esRecord.foreach(x=>updateDocument(x))
    println("SUCCESS!!!")
  }

  def analyseViaNLP(s: String): scala.collection.mutable.Set[(String,String)] ={
    val ners = scala.collection.mutable.Set[(String,String)]()
    val urlParams = Map("annotators"->"ner","outputFormat"->"json")
    val requestHeads = Map("Content-Type"->"text/plain; charset=utf-8")
    val requestAnalyse = Http(HTTP+nlpServer).method("POST").headers(requestHeads).postData(s).params(urlParams).timeout(1000,60000)
    val responseJSON = requestAnalyse.execute()
    val jsonPrased = parse(responseJSON.body)\"sentences"\"entitymentions" removeField{
      case ("text",_) => false
      case ("ner",JString(x)) => false
      case _ => true
    }
    val result = jsonPrased.values.asInstanceOf[List[List[Map[String,String]]]]
    for(i<-result){
      for(j<-i){
        if(SPECIFIED_ENTIRY_TYPES.contains(j.apply("ner"))){
          ners.add((j.apply("text"),j.apply("ner")))
        }
      }
    }
    ners
  }

  def analyseXML(fn:String,xm: scala.xml.Elem): scala.collection.mutable.Map[String,Any]={
    val result = scala.collection.mutable.Map[String,Any]()
    val analyse = (x:scala.xml.Node) => analyseViaNLP(x.text)
    result += ("filename"->new java.io.File(fn).getName)
    result += ("name"->(xm\\"name").text)
    result += ("AustLII"->(xm\\"AustLII").text)
    result += ("catchphrases"->(xm\\"catchphrase").map(_.text))
    result += ("sentences"->(xm\\"sentence").map(_.text))

    //    init 初始化person。。。。
    for(k<-SPECIFIED_ENTIRY_TYPES.map(_.toLowerCase)){
      result += (k->List())
    }

    val nlpCatchphrases = (xm\\"catchphrase").map(analyse).reduce(_++_)
    val nlpSentences = (xm\\"sentence").map(analyse).reduce(_++_)
    val finalEntry = (nlpSentences ++ nlpCatchphrases).groupBy[String](_._2)

    for(k<-finalEntry.keys){
      result += (k.toLowerCase->finalEntry.apply(k).map(_._1).toList)
    }
    result
  }

  def createIndexWithMappings(): Unit = {
    val textType = "type"->"text"
    val pro =
      "properties" ->
        ("filename"->textType)~
          ("name"->textType)~
          ("AustLII"->textType)~
          ("catchphrases"->textType)~
          ("sentences"->textType)~
          ("person"->textType)~
          ("location"->textType)~
          ("organization"->textType)

    val json = "mappings" -> ("cases"->pro)
    val createIndexRequestBody = compact(render(json))
    val create_index_request = Http(HTTP+esServer+"/legal_idx").method("PUT").header("Content-Type",MIME_JSON).put(createIndexRequestBody)
    val status = create_index_request.execute().statusLine
    println(status)
  }

  def updateDocument(record: scala.collection.mutable.Map[String,Any]): Unit ={
    implicit val formats = org.json4s.DefaultFormats
    val postJson = Serialization.write(record)
    val updateDocumentRequest = Http(HTTP+esServer+"/legal_idx/cases/"+record.apply("filename")).method("PUT").header("Content-Type",MIME_JSON).put(postJson)
    val status = updateDocumentRequest.execute().statusLine
    println("SUPDATE "+record.apply("filename")+" :\t"+status)
    println(record.apply("person"))
    println(record.apply("location"))
    println(record.apply("organization"))
  }
}