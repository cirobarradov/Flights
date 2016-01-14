package com.stratio.model.spark.sql

import java.sql.Date

import com.stratio.model.{FlightSql, Delays, Flight}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.joda.time.LocalDate

import scala.language.implicitConversions

class FlightCsvReader(self: RDD[String]) {

  /**
    * Creación de contexto para Spark SQL.
    */
  val sc = self.sparkContext
  val sqlContext = new SQLContext(sc)

  /**
    *
    * Parsea el RDD[String] de CSV's a un DataFrame.
    *
    * Tip: Crear un Flight y usar el método to FlightSql.
    *
    */
  def toDataFrame: DataFrame = {
    import sqlContext.implicits._
    val c = self.map(x=>FlightSql(Flight(
      new LocalDate(x.split(",")(0).toInt,x.split(",")(1).toInt+1, x.split(",")(2).toInt).toDate,
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
        Flight.parseCancelled(x.split(",")(28))))))

    c.toDF()
  }
}

class FlightFunctions(self: DataFrame) {

  /**
    *
    * Obtener la distancia media recorrida por cada aeropuerto.
    *
    * Tip: Para usar funciones de aggregación df.agg(sum('distance) as "suma"
    *
    */
  def averageDistanceByAirport: DataFrame = {
    /*self.agg(sum())
    val c = self.map(x=>(x.origin,x.distance))
    self.agg
    val result = c.combineByKey(
      (v) => (v, 1),
      (acc: (Int, Int), v) => (acc._1 + v, acc._2 + 1),
      (acc1: (Int, Int), acc2: (Int, Int)) => (acc1._1 + acc2._1, acc1._2 + acc2._2)
    ).map{ case (key, value) => (key, value._1 / value._2.toFloat)
        assert(averages(0).get(0).equals("SAN") && averages(0).get(1).equals(3926.3333333333335d))
    assert(averages(1).get(0).equals("SFO") && averages(1).get(1).equals(330.0d))}
    result*/
    self.registerTempTable("flight")
    self.sqlContext.sql("SELECT origin, AVG(distance) FROM flight group by origin").toDF()

  }

  /**
    *
    * Obtener el consumo mínimo por aeropuerto, mes y año.
    * @param fuelPrice DataFrame que contiene el precio del Fuel en un año
    *                  y mes determinado. Ver case class {@see com.stratio.model.FuelPrice}
    *
    *  Tip: Se pueden utilizar funciones propias del estándar SQL.
    *  ej: Extraer el año de un campo fecha llamado date:  year('date)
    */
  def minFuelConsumptionByMonthAndAirport(fuelPrice: DataFrame): DataFrame = {
    self.registerTempTable("flight")
    fuelPrice.registerTempTable("fuel")
    val df = self.sqlContext.sql("SELECT flight.origin, year(flight.date), month(flight.date), sum(flight.distance)*fuel.price " +
      "FROM flight JOIN fuel ON year(flight.date)=fuel.year " +
      "AND month(flight.date)=fuel.month GROUP BY flight.origin, year(flight.date), month(flight.date), fuel.price ").toDF()

    df.groupBy("origin").min("_c3").sort("min(_c3)")
  }

}


trait FlightSqlDsl {

  implicit def flightParser(lines: RDD[String]): FlightCsvReader = new FlightCsvReader(lines)

  implicit def flightFunctions(flights: DataFrame): FlightFunctions = new FlightFunctions(flights)
}

object FlightSqlDsl extends FlightSqlDsl