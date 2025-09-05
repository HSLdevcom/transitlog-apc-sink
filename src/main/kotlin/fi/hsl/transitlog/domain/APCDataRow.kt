package fi.hsl.transitlog.domain

import fi.hsl.common.passengercount.proto.PassengerCount
import java.time.Instant
import java.time.LocalDate
import java.time.LocalTime
import java.time.OffsetDateTime
import java.time.ZoneId

data class APCDataRow(
    val dir: Short,
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
    val totalPassengersOut: Short,
    val bikesIn: Short,
    val bikesOut: Short,
    val wheelchairsIn: Short,
    val wheelchairsOut: Short,
    val pramsIn: Short,
    val pramsOut: Short,
    val otherIn: Short,
    val otherOut: Short,
) {
    companion object {
        // TODO: handle invalid data
        fun PassengerCount.Data.toAPCDataRow(): APCDataRow {
            val payload = this.payload

            try {
                val allowedClasses = setOf("adult", "child")

                val totalPassengersIn =
                    payload.vehicleCounts.doorCountsList.sumOf { door ->
                        door.countList
                            .filter { count ->
                                count.clazz != null && count.clazz in allowedClasses
                            }
                            .sumOf { count -> count.`in`.toInt() }
                    }
                val totalPassengersOut =
                    payload.vehicleCounts.doorCountsList.sumOf { door ->
                        door.countList
                            .filter { count ->
                                count.clazz != null && count.clazz in allowedClasses
                            }
                            .sumOf { count -> count.out.toInt() }
                    }

                val bikesIn =
                    payload.vehicleCounts.doorCountsList.sumOf { door ->
                        door.countList
                            .filter { count -> count.clazz != null && count.clazz == "bike" }
                            .sumOf { count -> count.`in`.toInt() }
                    }
                val bikesOut =
                    payload.vehicleCounts.doorCountsList.sumOf { door ->
                        door.countList
                            .filter { count -> count.clazz != null && count.clazz == "bike" }
                            .sumOf { count -> count.out.toInt() }
                    }

                val wheelchairsIn =
                    payload.vehicleCounts.doorCountsList.sumOf { door ->
                        door.countList
                            .filter { count -> count.clazz != null && count.clazz == "wheelchair" }
                            .sumOf { count -> count.`in`.toInt() }
                    }
                val wheelchairsOut =
                    payload.vehicleCounts.doorCountsList.sumOf { door ->
                        door.countList
                            .filter { count -> count.clazz != null && count.clazz == "wheelchair" }
                            .sumOf { count -> count.out.toInt() }
                    }

                val pramsIn =
                    payload.vehicleCounts.doorCountsList.sumOf { door ->
                        door.countList
                            .filter { count -> count.clazz != null && count.clazz == "pram" }
                            .sumOf { count -> count.`in`.toInt() }
                    }
                val pramsOut =
                    payload.vehicleCounts.doorCountsList.sumOf { door ->
                        door.countList
                            .filter { count -> count.clazz != null && count.clazz == "pram" }
                            .sumOf { count -> count.out.toInt() }
                    }

                val otherIn =
                    payload.vehicleCounts.doorCountsList.sumOf { door ->
                        door.countList
                            .filter { count -> count.clazz != null && count.clazz == "other" }
                            .sumOf { count -> count.`in`.toInt() }
                    }
                val otherOut =
                    payload.vehicleCounts.doorCountsList.sumOf { door ->
                        door.countList
                            .filter { count -> count.clazz != null && count.clazz == "other" }
                            .sumOf { count -> count.out.toInt() }
                    }

                return APCDataRow(
                    payload.dir.toShort(),
                    payload.oper.toShort(),
                    payload.veh,
                    "${payload.oper}/${payload.veh}", // TODO: should we use operator + vehicle
                    // values from the topic?
                    Instant.ofEpochMilli(payload.tst)
                        .atZone(ZoneId.of("Europe/Helsinki"))
                        .toOffsetDateTime(), // TODO: don't hardcode timezone
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
                    totalPassengersOut.toShort(),
                    bikesIn.toShort(),
                    bikesOut.toShort(),
                    wheelchairsIn.toShort(),
                    wheelchairsOut.toShort(),
                    pramsIn.toShort(),
                    pramsOut.toShort(),
                    otherIn.toShort(),
                    otherOut.toShort()
                )
            } catch (e: Exception) {
                throw InvalidAPCException("APC message could not be converted to data row", e)
            }
        }
    }

    class InvalidAPCException(override val message: String, cause: Throwable?) :
        Exception(message, cause)
}
