import java.nio.file.Paths
import java.util.concurrent.TimeUnit
import javax.xml.stream.events.XMLEvent

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.{FileIO, Sink}
import com.tradeshift.reaktive.marshal.{Protocol, StringMarshallable}
import com.tradeshift.reaktive.marshal.stream.{AaltoReader, ProtocolReader}
import com.tradeshift.reaktive.xml.XMLProtocol
import com.tradeshift.reaktive.xml.XMLProtocol.{attribute, body, qname, tag}
import com.tradeshift.reaktive.marshal.Protocol._
import com.typesafe.config.ConfigFactory
import org.scalatest.{BeforeAndAfter, WordSpec}

import scala.concurrent.duration._
import scala.concurrent.Await
import scala.concurrent.ExecutionContext.Implicits.global

class ParseSpec extends WordSpec with BeforeAndAfter {

  val config = ConfigFactory.parseString(
    """
      |akka.loglevel = DEBUG
    """.stripMargin)

  implicit val sys = ActorSystem("ParseSpec", config.withFallback(ConfigFactory.load))
  implicit val mat = ActorMaterializer()

  def afterAll() = {
    Await.result(sys.terminate(), 10.seconds)
  }

  "parser" should {

    def nowMillis = Duration(System.currentTimeMillis(), TimeUnit.MILLISECONDS)
    val started = nowMillis

//    val proto = tag(qname("MedlineCitationSet"),
//                    tag(qname("MedlineCitation"),
//                      tag(qname("Article"),
//                        tag(qname("ArticleTitle"), body))))

    //val priceProto = tag(qname("int"), body).having(attribute("name"), "price")

    val proto =
      tag(qname("response"),
        tag(qname("result"),
          tag(qname("doc"),
            option(
              tag(qname("int"), body).having(attribute("name"), "price")
            )
          )
        )
      )

    "should parse" in {

      def parseFrom(num: Int) = {
        FileIO.fromPath(Paths.get("/home/martynas/Downloads/export20170407.xml"), chunkSize = 8192 * 2)
          .via(new AaltoReader)
          .statefulMapConcat(() => {
            var count = 0
            var drop = true
            var startDocSaved = false
            var startDocEmitted = false
            var preamble: List[XMLEvent] = Nil

            (el) => {
              if (!(el.isStartElement && el.asStartElement.getName.toString == "doc")) {
                if (!startDocSaved) {
                  preamble = el :: preamble
                }
              } else {
                startDocSaved = true
              }

              if (!drop) {
                if (startDocEmitted) {
                  el :: Nil
                } else {
                  if (el.isStartElement) {
                    println(s"will stop dropping stuff")
                    startDocEmitted = true
                    preamble.reverse :+ el
                  } else {
                    println("skipping until first start of an element")
                    Nil
                  }
                }
              } else {
                if (el.isEndElement && el.asEndElement().getName.toString == "doc") {
                  count += 1
                }

                if (count >= num) {
                  drop = false
                }

                Nil
              }
            }
          })
      }

      def elementsFrom(n: Int) =
        parseFrom(n).via(ProtocolReader.of(proto))

      def printXmlEvent(e: XMLEvent) = e match {
        case e if e.isStartElement => s"Start: ${e.asStartElement.getName}"
        case e if e.isEndElement => s"End: ${e.asEndElement.getName}"
        case e => e.toString
      }

      val res =
        //elementsFrom(0).take(100000)
        //.merge(elementsFrom(100000).take(100000))
        //.merge(elementsFrom(200000).take(100000))
        FileIO.fromPath(Paths.get("/home/martynas/Downloads/export20170407.xml"), chunkSize = 8192 * 2)
        .via(AaltoReader.instance)
        //parseFrom(0)
        //.log("Element", printXmlEvent)
        .via(ProtocolReader.of(proto))
        //.take(10)
        //.log("Price")
        .runWith(Sink.fold(0) { (c, el) =>
          if (c % 50000 == 0) println(c, nowMillis - started)
          c + 1
        })

      res.onComplete { count =>
        println(count)
        //println(count.failed.get.printStackTrace())
        println(nowMillis - started)
      }

      scala.io.StdIn.readLine()
    }
  }

}
