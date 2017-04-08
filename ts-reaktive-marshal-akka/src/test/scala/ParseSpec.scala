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
import org.scalatest.{BeforeAndAfter, WordSpec}

import scala.concurrent.duration._
import scala.concurrent.Await

class ParseSpec extends WordSpec with BeforeAndAfter {

  implicit val sys = ActorSystem("ParseSpec")
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

      val res = FileIO.fromPath(Paths.get("/home/martynas/Downloads/export20170407.xml"), chunkSize = 8192)
        .via(AaltoReader.instance)
        .via(ProtocolReader.of(proto))
        .runWith(Sink.fold(0) { (c, el) =>
          if (c % 50000 == 0) println(c, nowMillis - started)
          c + 1
        })

      val count = Await.result(res, Duration.Inf)
      println(count)

      println(nowMillis - started)
    }
  }

}
