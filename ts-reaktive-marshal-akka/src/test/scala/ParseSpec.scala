import java.nio.file.Paths
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicReference
import javax.xml.stream.events.XMLEvent

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.{FileIO, Sink, Source}
import akka.util.ByteString
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

import javaslang.control.{ Option => JOption }

import scala.compat.java8.OptionConverters._

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
            )/*,
            option(
              tag(qname("int"), body).having(attribute("name"), "square_footage")
            )*/
            /*option(
              tag(qname("float"), body).having(attribute("name"), "total_baths")
            ),
            option(
              tag(qname("int"), body).having(attribute("name"), "year_built")
            ),
            option(
              tag(qname("float"), body).having(attribute("name"), "lot_size")
            ),
            option(
              tag(qname("int"), body).having(attribute("name"), "beds")
            ),*/
//            new javaslang.Function6[JOption[String], JOption[String], JOption[String], JOption[String], JOption[String], JOption[String], Option[Entity]] {
//              def apply(price: JOption[String], squareFootage: JOption[String], baths: JOption[String], yearBuilt: JOption[String], lotSize: JOption[String], beds: JOption[String]) = {
//                for {
//                  p <- price.toJavaOptional.asScala.map(_.toInt) if p > 10000
//                  s <- squareFootage.toJavaOptional.asScala.map(_.toFloat)
//                  b <- baths.toJavaOptional.asScala.map(_.toFloat)
//                  y <- yearBuilt.toJavaOptional.asScala.map(_.toInt)
//                  l <- lotSize.toJavaOptional.asScala.map(_.toFloat)
//                  beds <- beds.toJavaOptional.asScala.map(_.toInt)
//                } yield Entity(p, s, b, y, l, beds)
//              }
//            }
//            new javaslang.Function2[JOption[String], JOption[String], Option[Entity]] {
//              def apply(price: JOption[String], squareFootage: JOption[String]) = {
//                for {
//                  p <- price.toJavaOptional.asScala.map(_.toInt) if p > 10000
//                  s <- squareFootage.toJavaOptional.asScala.map(_.toFloat)
//                } yield Entity(p, s, 0, 0, 0, 0)
//              }
//            }
            /*new javaslang.Function1[JOption[String], Option[Entity]] {
              def apply(price: JOption[String]) = {
                for {
                  p <- price.toJavaOptional.asScala.map(_.toInt) if p > 10000
                } yield Entity(p, 0, 0, 0, 0, 0)
              }
            }*/
          )
        )
      )

    val footageProto =
      tag(qname("response"),
        tag(qname("result"),
          tag(qname("doc"),
            option(
              tag(qname("int"), body).having(attribute("name"), "square_footage")
            )
          )
        )
      )

    def printSink[T]() = Sink.fold[Int, T](0) { (c, el) =>
      if (c % 50000 == 0) println(c, nowMillis - started)
      c + 1
    }

    "should parse" in {

      def printXmlEvent(e: XMLEvent) = e match {
        case e if e.isStartElement => s"Start: ${e.asStartElement.getName}"
        case e if e.isEndElement => s"End: ${e.asEndElement.getName}"
        case e => e.toString
      }

      var bnd = Boundaries()

      val parsed = FileIO.fromPath(Paths.get("/home/martynas/Downloads/export20170407.xml"), chunkSize = 8192 * 2)
        .via(AaltoReader.instance)
        .via(ProtocolReader.of(proto))
        .mapConcat {
          case e if e._1.isDefined && e._2.isDefined => Entity(e._1.get.toInt, e._2.get.toInt, 0, 0, 0, 0) :: Nil
          case e => Nil
        }
        .map { entity =>
          if (entity.price > bnd.priceMax) {
            bnd = bnd.copy(priceMax = entity.price)
          }
          if (entity.price < bnd.priceMin) {
            bnd = bnd.copy(priceMin = entity.price)
          }
          if (entity.footage > bnd.footageMax) {
            bnd = bnd.copy(footageMax = entity.footage)
          }
          if (entity.footage < bnd.footageMin) {
            bnd = bnd.copy(footageMin = entity.footage)
          }
          if (entity.baths > bnd.bathsMax) {
            bnd = bnd.copy(bathsMax = entity.baths)
          }
          if (entity.baths < bnd.bathsMin) {
            bnd = bnd.copy(bathsMin = entity.baths)
          }
          if (entity.year > bnd.yearMax) {
            bnd = bnd.copy(yearMax = entity.year)
          }
          if (entity.year < bnd.yearMin) {
            bnd = bnd.copy(yearMin = entity.year)
          }
          if (entity.lotSize > bnd.lotSizeMax) {
            bnd = bnd.copy(lotSizeMax = entity.lotSize)
          }
          if (entity.lotSize < bnd.lotSizeMin) {
            bnd = bnd.copy(lotSizeMin = entity.lotSize)
          }
          if (entity.beds > bnd.bedsMax) {
            bnd = bnd.copy(bedsMax = entity.beds)
          }
          if (entity.beds < bnd.bedsMin) {
            bnd = bnd.copy(bedsMin = entity.beds)
          }
          entity
        }
        .take(60000)
        .runWith(Sink.seq)

      val allrows = Await.result(parsed, Duration.Inf)

      val complete = Source(allrows)
        .map { entity =>
          EntityNormal(
            (entity.price - bnd.priceMin) / bnd.priceMax.toFloat,
            (entity.footage - bnd.footageMin) / bnd.footageMax,
            (entity.baths - bnd.bathsMin) / bnd.bathsMax,
            (entity.year - bnd.yearMin) / bnd.yearMax.toFloat,
            (entity.lotSize - bnd.lotSizeMin) / bnd.lotSizeMax,
            (entity.beds - bnd.bedsMin) / bnd.bedsMax.toFloat
          )
        }
        .map { e =>
          f"${e.price}%f;${e.footage}%f;${e.baths}%f;${e.year}%f;${e.lotSize}%f;${e.beds}%f\n"
        }
        .map(ByteString.apply)
        .runWith(FileIO.toPath(Paths.get("/home/martynas/Downloads/houses_normal.csv")))

      complete.onComplete { res =>
        println(res)
        println(nowMillis - started)
      }

      scala.io.StdIn.readLine()
    }
  }

}

object ParseSpec {
  case class Entity(price: Int, footage: Float, baths: Float, year: Int, lotSize: Float, beds: Int)
  case class EntityNormal(price: Float, footage: Float, baths: Float, year: Float, lotSize: Float, beds: Float)

  case class Boundaries(
    priceMin: Int = Int.MaxValue, priceMax: Int = Int.MinValue,
    footageMin: Float = Float.MaxValue, footageMax: Float = Float.MinValue,
    bathsMin: Float = Float.MaxValue, bathsMax: Float = Float.MinValue,
    yearMin: Int = Int.MaxValue, yearMax: Int = Int.MinValue,
    lotSizeMin: Float = Float.MaxValue, lotSizeMax: Float = Float.MinValue,
    bedsMin: Int = Int.MaxValue, bedsMax: Int = Int.MinValue
  )
}
