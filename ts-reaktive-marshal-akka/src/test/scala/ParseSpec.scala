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

  import ParseSpec._

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

    def printSink[T]() = Sink.fold[T](0) { (c, el) =>
      if (c % 50000 == 0) println(c, nowMillis - started)
      c + 1
    }

    "should parse" in {

      def printXmlEvent(e: XMLEvent) = e match {
        case e if e.isStartElement => s"Start: ${e.asStartElement.getName}"
        case e if e.isEndElement => s"End: ${e.asEndElement.getName}"
        case e => e.toString
      }

      val res =
        FileIO.fromPath(Paths.get("/home/martynas/Downloads/export20170407.xml"), chunkSize = 8192 * 2)
        .via(AaltoReader.instance)
        .via(ProtocolReader.of(proto))
        .mapConcat(_.toList)
        .runWith(printSink)

      res.onComplete { count =>
        println(count)
        println(nowMillis - started)
      }

      scala.io.StdIn.readLine()
    }
  }

}

object ParseSpec {
  case class Entity(price: Int)
}
