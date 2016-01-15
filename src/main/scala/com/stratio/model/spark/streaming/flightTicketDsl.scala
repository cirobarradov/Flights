package com.stratio.model.spark.streaming

import com.stratio.model._
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.Duration
import org.apache.spark.streaming.dstream.DStream

import scala.language.implicitConversions

class FlightTicketFunctions(self: DStream[FlightTicket]){

  /**
   *
   * Obtener la edad media de los pasajeros cada "windowSeconds" segundos moviendo la ventana cada "slideSeconds"
   * segundos
   *
   * Tip: Usar avgFunc
   * @param windowSeconds Segundos de ventana
   * @param slideSeconds Segundos de sliding
   */
  def avgAgeByWindow(windowSeconds: Int, slideSeconds: Int): DStream[(Float, Float)]= {

    val avgFunc: ((Float, Float), (Float, Float)) => (Float, Float) =
      (sumCounter1, sumCounter2) =>
      (((sumCounter1._1 * sumCounter1._2) + (sumCounter2._1 * sumCounter2._2)) / (sumCounter1._2 + sumCounter2._2),
        sumCounter1._2 + sumCounter2._2)

    self.map(x=>(x.passenger.age.toFloat,1F)).
      reduceByWindow(avgFunc,Duration(windowSeconds*1000), Duration(slideSeconds*1000))

  }

  /**
   *
   * Extraer el nombre del aeropuerto del ticket correspondiente a partir de la información de los vuelos
   *
   * @param flights RDD con los vuelos a los que pertenecen los tickets
   * @return
   *         DStream con la asocicacion del nombre del aerpuerto correspondinte a la sálida de cada ticket de vuelo y
   *         el propio ticket
   *
   * Tip: Cruzar por el fligthNum
   *
   * Tip: Usar la operación transform para usar la información de los vuelos.
   */
  def byAirport(flights: RDD[Flight]): DStream[(String, FlightTicket)] = {
    self.transform(rdd=>{
      flights.map(x=>(x.flightNum,x.origin)).join(rdd.map(x=>(x.flightNumber,
        FlightTicket(x.flightNumber,x.passenger,x.payer))))
        .map(x=>(x._2._1,x._2._2))
    }
    )
  }

  /**
   * Obtener para cada ventana de tiempo definida por "windowSeconds" y "slideSeconds" cual es el aeropuerto con
   * mayor número de tickets.
   *
   * @param flights RDD con los vuelos a los que pertenecen los tickets
   * @param windowSeconds Segundos de ventana
   * @param slideSeconds Segundos de sliding
   * @return
   *         DStream con el nombre y número de tickets obtenidos en esta ventana del aeropuerto con mayor número de
   *         tickets.
   *
   * Tip: Usar la función anterior "byAirport" para poder analizar la información por aeropuerto en cada ventana.
   *
   * * Tip: Si queremos hacer un reduceByKey con los datos de una ventana de tiempo, deberemos usar la operación
   * reduceByKeyAndWindow (usar la operacion reduceFunc)
   *
   * Tip: Si tenemos varios resultados en un DStream y queremos devolver un solo resultado en una venta tendremos que
   * usar la operacion reduce.
   */
  def airportMaxFlightsByWindow(flights: RDD[Flight], windowSeconds: Int, slideSeconds: Int): DStream[(String, Int)]= {
    val reduceFunc: (Int, Int) => Int = _ + _
    byAirport(flights).map(x=>(x._1,1)).reduceByKeyAndWindow(reduceFunc,
      Duration(windowSeconds*1000),Duration(slideSeconds*1000)
    ).reduce((x,y)=>if (x._2 > y._2) (x._1,x._2) else (y._1,y._2))
  }

  /**
   *
   * Obtener las estadísticas de cada aeropuerto a partir de la información de los tickets de vuelos.
   *
   * @param flights RDD con los vuelos a los que pertenecen los tickets
   * @return
   *         DStream con el nombre del aeropuerto y las estadisticas reflejadas con el objeto "AirportStatistics"
   *
   * Tip: Usar la función anterior "byAirport" para poder analizar la información por aeropuerto en cada ventana.
   *
   * Tip: Para obterner las estadisticas de cada aeropuerto debemos mantener un estado asociado a ellas para cada
   * micro-batch, usar la operacion state-ful "updateStateByKey"
   *
   * Tip: Usar la función "addFlightTickets" para actulizar la información estadistica de cada aeropuerto en cada
   * micro-batch
   */
  def airportStatistics(flights: RDD[Flight]): DStream[(String, AirportStatistics)]= {
    val fba = byAirport(flights)
    fba.updateStateByKey(x=>(x._))
    AirportStatistics.
    self.map("",AirportStatistics(FlightTicket(1,Person("Maria",'F',50,Some(1250.0)),Company)))
  }
}

/*
  def addFlightTickets(tickets: Seq[FlightTicket]): AirportStatistics = {
    if (tickets.isEmpty) this
    else tickets.aggregate(this)(_.addFlightTicket(_), _.aggregate(_))
  }
 */
trait FlightTicketDsl {

  implicit def ticketFunctions(tickets: DStream[FlightTicket]): FlightTicketFunctions =
    new FlightTicketFunctions(tickets)
}

object FlightTicketDsl extends FlightTicketDsl
