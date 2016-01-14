package com.stratio.model.spark

import java.util.Calendar

import com.stratio.model.{Delays, Cancelled, Flight, FuelPrice}
import org.apache.spark.rdd.RDD
import org.joda.time.LocalDate

import scala.language.implicitConversions

class FlightCsvReader(self: RDD[String]) {

  //val depTime=4
  /**
    *
    * Parsea el RDD[String] de CSV's a un RDD[Flight]
    *
    * Tip: Usar método flatmap.
    *
    */
  def toFlight: RDD[Flight] = {
    val c = self.map(x=>Flight( new LocalDate(
      x.split(",")(0).toInt,x.split(",")(1).toInt+1, x.split(",")(2).toInt).toDate,
      x.split(",")(4).toInt,
      x.split(",")(5).toInt,
      x.split(",")(6).toInt,
      x.split(",")(7).toInt,
      x.split(",")(8).toString,
      x.split(",")(9).toInt,
      x.split(",")(11).toInt,
      x.split(",")(12).toInt,
      x.split(",")(14).toInt,
      x.split(",")(15).toInt,
      x.split(",")(16).toString,
      x.split(",")(17).toString,
      x.split(",")(18).toInt,
      Flight.parseCancelled(x.split(",")(21)),
      x.split(",")(22).toInt,
      Delays(Flight.parseCancelled(x.split(",")(24)),
        Flight.parseCancelled(x.split(",")(25)),
        Flight.parseCancelled(x.split(",")(26)),
        Flight.parseCancelled(x.split(",")(27)),
        Flight.parseCancelled(x.split(",")(28)))))
    c

  }

  /**
    *
    * Obtener todos los errores del CSV asociados a cada línea. OJO puede haber más de un error por línea
    *
    * Tip: Usar método flatmap.
    * Tip: Usar el método extractErrors de Flight
    *
    */
  def toErrors: RDD[(String, String)] = {
    val c = self.map(x=>Flight.extractErrors(x.split(","))).filter(x=>x.length>0).
      flatMap(x=>x).map(x=>(x.hashCode.toString,x))
    c
  }
}

class FlightFunctions(self: RDD[Flight]) {

  /**
    *
    * Obtener la distancia media recorrida por cada aeropuerto.
    *
    *
    */
  def averageDistanceByAirport: RDD[(String, Float)] = {
    val c = self.map(x=>(x.origin,x.distance))

    val result = c.combineByKey(
      (v) => (v, 1),
      (acc: (Int, Int), v) => (acc._1 + v, acc._2 + 1),
      (acc1: (Int, Int), acc2: (Int, Int)) => (acc1._1 + acc2._1, acc1._2 + acc2._2)
    ).map{ case (key, value) => (key, value._1 / value._2.toFloat) }


    result
  }

  /**
    *
    * Obtener el consumo mínimo por aeropuerto, mes y año.
    * @param fuelPrice RDD que contiene el precio del Fuel en un año
    *                  y mes determinado. Ver case class {@see com.stratio.model.FuelPrice}
    *
    *  Tip: Primero agrupar para cada aeropuerto, mes y año y sumar las distancias de los vuelos por el precio de
    *  combustible para ese mes y año y luego ver, para cada aeropuerto cual es el menor de los totales de los meses, año
    *
    *  Tip: Si el RDD es muy pequeño, podeis usar variables compartidas para evitar Joins
    *
    */
  def minFuelConsumptionByMonthAndAirport(fuelPrice: RDD[FuelPrice]): RDD[(String, (Int, Int))] = {
    val c = self.map(x=>(x.origin,(1900+x.date.getYear,x.date.getMonth)))

    val a = self.map(x=>((x.origin,1900+x.date.getYear,x.date.getMonth),x.distance)).reduceByKey(_+_)

    val fuel =fuelPrice.cartesian(self.map(x=>x.origin)).map(x=>((x._2,x._1.year,x._1.month),x._1.price)).distinct()
    val fuels =fuel.join(a).map(x=>(x._1,x._2._1*x._2._2))
    val result = fuels.map(x=>(x._1._1,(x._1._2,x._1._3,x._2))).reduceByKey((x,y)=>if (x._3 < y._3) (x._1,x._2,x._3)
    else (y._1,y._2,y._3)).map(x=>(x._1,(x._2._1,x._2._2)))
    result
  }
}


trait FlightDsl {

  implicit def flightParser(lines: RDD[String]): FlightCsvReader = new FlightCsvReader(lines)

  implicit def flightFunctions(flights: RDD[Flight]): FlightFunctions = new FlightFunctions(flights)
}

object FlightDsl extends FlightDsl