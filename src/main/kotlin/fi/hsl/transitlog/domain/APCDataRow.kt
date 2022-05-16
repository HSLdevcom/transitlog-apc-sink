package fi.hsl.transitlog.domain

import fi.hsl.common.passengercount.proto.PassengerCount
import java.time.Instant
import java.time.OffsetDateTime
import java.time.ZoneId

data class APCDataRow(val dir: Int,
                      val oper: Int,
                      val veh: Int,
                      val uniqueVehicleId: String,
                      val tst: OffsetDateTime,
                      val tsi: Long,
                      val latitude: Double,
                      val longitude: Double,
                      val oday: String,
                      val start: String,
                      val stop: Int?,
                      val route: String,
                      val passengerCountQuality: String,
                      val vehicleLoad: Int,
                      val vehicleLoadRatio: Double,
                      val totalPassengersIn: Int,
                      val totalPassengersOut: Int
) {
    companion object {
        //TODO: handle invalid data
        fun PassengerCount.Data.toAPCDataRow(): APCDataRow {
            val payload = this.payload

            val totalPassengersIn = payload.vehicleCounts.doorCountsList.sumOf { door -> door.countList.sumOf { count -> count.`in` } }
            val totalPassengersOut = payload.vehicleCounts.doorCountsList.sumOf { door -> door.countList.sumOf { count -> count.out } }

            return APCDataRow(
                payload.dir.toInt(),
                payload.oper,
                payload.veh,
                "${payload.oper}/${payload.veh}", //TODO: should we use operator + vehicle values from the topic?
                Instant.ofEpochMilli(payload.tst).atZone(ZoneId.of("Europe/Helsinki")).toOffsetDateTime(), //TODO: don't hardcode timezone
                payload.tsi,
                payload.lat,
                payload.long,
                payload.oday,
                payload.start,
                payload.stop,
                payload.route,
                payload.vehicleCounts.countQuality,
                payload.vehicleCounts.vehicleLoad,
                payload.vehicleCounts.vehicleLoadRatio,
                totalPassengersIn,
                totalPassengersOut
            )
        }
    }
}
