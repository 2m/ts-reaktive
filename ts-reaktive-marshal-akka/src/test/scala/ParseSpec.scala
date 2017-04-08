import java.nio.file.Paths
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicReference
import javax.xml.stream.events.XMLEvent

import akka.actor.ActorSystem
import akka.stream.{ActorMaterializer, ClosedShape, FlowShape}
import akka.stream.scaladsl.{Broadcast, FileIO, Flow, GraphDSL, Merge, RunnableGraph, Sink, Source, Zip, ZipN}
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
import javaslang.control.{Option => JOption}

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

    val priceProto =
      tag(qname("response"),
        tag(qname("result"),
          tag(qname("doc"),
            option(
              tag(qname("int"), body).having(attribute("name"), "price")
            )
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

    val bathsProto =
      tag(qname("response"),
        tag(qname("result"),
          tag(qname("doc"),
            option(
              tag(qname("float"), body).having(attribute("name"), "total_baths")
            )
          )
        )
      )

    val yearProto =
      tag(qname("response"),
        tag(qname("result"),
          tag(qname("doc"),
            option(
              tag(qname("int"), body).having(attribute("name"), "year_built")
            )
          )
        )
      )

    val lotSizeProto =
      tag(qname("response"),
        tag(qname("result"),
          tag(qname("doc"),
            option(
              tag(qname("float"), body).having(attribute("name"), "lot_size")
            )
          )
        )
      )

    val bedsProto =
      tag(qname("response"),
        tag(qname("result"),
          tag(qname("doc"),
            option(
              tag(qname("int"), body).having(attribute("name"), "beds")
            )
          )
        )
      )

    val stateProto =
      tag(qname("response"),
        tag(qname("result"),
          tag(qname("doc"),
            option(
              tag(qname("str"), body).having(attribute("name"), "state")
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

      val parserFlow = Flow.fromGraph(GraphDSL.create() { implicit b =>
        import GraphDSL.Implicits._

        // prepare graph elements
        val broadcast = b.add(Broadcast[XMLEvent](7))
        val zip = b.add(ZipN[JOption[String]](7))

        // connect the graph
        broadcast.out(0) ~> ProtocolReader.of(priceProto) ~> zip.in(0)
        broadcast.out(1) ~> ProtocolReader.of(footageProto) ~> zip.in(1)
        broadcast.out(2) ~> ProtocolReader.of(bathsProto) ~> zip.in(2)
        broadcast.out(3) ~> ProtocolReader.of(yearProto) ~> zip.in(3)
        broadcast.out(4) ~> ProtocolReader.of(lotSizeProto) ~> zip.in(4)
        broadcast.out(5) ~> ProtocolReader.of(bedsProto) ~> zip.in(5)
        broadcast.out(6) ~> ProtocolReader.of(stateProto) ~> zip.in(6)

        // expose ports
        FlowShape(broadcast.in, zip.out)
      })

      val parsed = FileIO.fromPath(Paths.get("/home/martynas/Downloads/export20170407.xml"), chunkSize = 8192 * 2)
        .via(AaltoReader.instance)
        .via(parserFlow)
        .mapConcat {
          case e if e.forall(_.isDefined) =>
            Entity(e(0).get.toInt, e(1).get.toInt, e(2).get.toFloat, e(3).get.toInt, e(4).get.toFloat, e(5).get.toInt, e(6).get) :: Nil
          case e => Nil
        }
        .groupBy(100, _.state)
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
        .mergeSubstreams
        //.take(60000)
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
            (entity.beds - bnd.bedsMin) / bnd.bedsMax.toFloat,
            entity.state
          )
        }
        .groupBy(100, _.state)
        .map { e =>
          (f"${e.price}%f;${e.footage}%f;${e.baths}%f;${e.year}%f;${e.lotSize}%f;${e.beds}%f", e.state)
        }
        .fold(("", List.empty[String])) {
          case ((_, list), (row, state)) => (state, row :: list)
        }
        .mapAsync(parallelism = 5) {
          case (state, rows) =>
            Source.single("price;footage;baths;year;lot_size;beds")
              .concat(Source(rows))
              .map(line => ByteString(line + "\n"))
              .runWith(FileIO.toPath(Paths.get(s"/home/martynas/Downloads/states/houses_normal_$state.csv")))
        }
        .mergeSubstreams
        .runWith(Sink.foreach(println))

      complete.onComplete { res =>
        println(res)
        println(nowMillis - started)
      }

      scala.io.StdIn.readLine()
    }
  }

}

object ParseSpec {
  case class Entity(price: Int, footage: Int, baths: Float, year: Int, lotSize: Float, beds: Int, state: String)
  case class EntityNormal(price: Float, footage: Float, baths: Float, year: Float, lotSize: Float, beds: Float, state: String)

  case class Boundaries(
    priceMin: Int = Int.MaxValue, priceMax: Int = Int.MinValue,
    footageMin: Float = Float.MaxValue, footageMax: Float = Float.MinValue,
    bathsMin: Float = Float.MaxValue, bathsMax: Float = Float.MinValue,
    yearMin: Int = Int.MaxValue, yearMax: Int = Int.MinValue,
    lotSizeMin: Float = Float.MaxValue, lotSizeMax: Float = Float.MinValue,
    bedsMin: Int = Int.MaxValue, bedsMax: Int = Int.MinValue
  )
}
