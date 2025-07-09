package fi.hsl.transitlog.domain

import fi.hsl.common.passengercount.proto.PassengerCount
import java.time.Instant
import java.time.LocalDate
import java.time.LocalTime
import java.time.OffsetDateTime
import java.time.ZoneId

data class APCDataRow(val dir: Short,
                      val oper: Short,
                      val veh: Int,
                      val uniqueVehicleId: String,
                      val tst: OffsetDateTime,
                      val tsi: Long,
                      val latitude: Double,
                      val longitude: Double,
                      val oday: LocalDate,
                      val start: LocalTime,
                      val stop: Int?,
                      val route: String,
                      val passengerCountQuality: String,
                      val vehicleLoad: Short,
                      val vehicleLoadRatio: Double,
                      val totalPassengersIn: Short,
                      val totalPassengersOut: Short
) {
    companion object {
        //TODO: handle invalid data
        fun PassengerCount.Data.toAPCDataRow(): APCDataRow {
            val payload = this.payload

            try {
                val allowedClasses = setOf("adult", "child")

                val totalPassengersIn = payload.vehicleCounts.doorCountsList.sumOf { door -> door.countList.filter { count -> count.clazz in allowedClasses }.sumOf { count -> count.in } }
                val totalPassengersOut = payload.vehicleCounts.doorCountsList.sumOf { door -> door.countList.filter { count -> count.clazz in allowedClasses }.sumOf { count -> count.out } }
                return APCDataRow(
                    payload.dir.toShort(),
                    payload.oper.toShort(),
                    payload.veh,
                    "${payload.oper}/${payload.veh}", //TODO: should we use operator + vehicle values from the topic?
                    Instant.ofEpochMilli(payload.tst).atZone(ZoneId.of("Europe/Helsinki")).toOffsetDateTime(), //TODO: don't hardcode timezone
                    payload.tsi,
                    payload.lat,
                    payload.long,
                    LocalDate.parse(payload.oday),
                    LocalTime.parse(payload.start),
                    payload.stop,
                    payload.route,
                    payload.vehicleCounts.countQuality,
                    payload.vehicleCounts.vehicleLoad.toShort(),
                    payload.vehicleCounts.vehicleLoadRatio,
                    totalPassengersIn.toShort(),
                    totalPassengersOut.toShort()
                )
            } catch (e: Exception) {
                throw InvalidAPCException("APC message could not be converted to data row", e)
            }
        }
    }

    class InvalidAPCException(override val message: String, cause: Throwable?) : Exception(message, cause)
}
