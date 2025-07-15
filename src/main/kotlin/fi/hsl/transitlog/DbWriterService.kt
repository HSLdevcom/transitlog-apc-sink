package fi.hsl.transitlog

import fi.hsl.transitlog.domain.APCDataRow
import mu.KotlinLogging
import org.apache.pulsar.client.api.MessageId
import java.sql.BatchUpdateException
import java.sql.Connection
import java.sql.PreparedStatement
import java.sql.Types
import java.util.concurrent.Executors
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.TimeUnit
import kotlin.math.min
import kotlin.time.ExperimentalTime
import kotlin.time.measureTime

@ExperimentalTime
class DbWriterService(connection: Connection, private val messageAcknowledger: (MessageId) -> Unit, private val dbMaxWriteBatchSize: Int, dbWriteIntervalSeconds: Long) : AutoCloseable {
    private val log = KotlinLogging.logger {}

    companion object {
        private const val DB_INSERT_QUERY = """
            INSERT INTO passengercount (
              dir, oper, veh, unique_vehicle_id, 
              tst, tsi, latitude, longitude, oday, 
              start, stop, route, passenger_count_quality, 
              vehicle_load, vehicle_load_ratio, 
              total_passengers_in, total_passengers_out,
              bikes_in, bikes_out, wheelchairs_in, wheelchairs_out,
              prams_in, prams_out, other_in, others_out
            ) 
            VALUES 
              (
                ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 
                ?, ?, ?, ?, ?, ?, ?, ?, ?
              )
        """
    }

    private val dbWriterExecutor = Executors.newSingleThreadScheduledExecutor { runnable ->
        val thread = Thread(runnable)
        thread.name = "DbWriterThread"
        thread.isDaemon = true
        return@newSingleThreadScheduledExecutor thread
    }

    private val writeQueue = LinkedBlockingQueue<Pair<APCDataRow, MessageId>>()

    init {
        val statement = connection.prepareStatement(DB_INSERT_QUERY)
        statement.queryTimeout = 60 //60s timeout for the query

        dbWriterExecutor.scheduleWithFixedDelay({
            try {
                writeBatch(statement)
            } catch (e: Exception) {
                if (e is BatchUpdateException) {
                    log.error(e) { "Batch update exception when writing APC data to DB (SQL state: ${e.sqlState}, error code: ${e.errorCode}, next exception: ${e.nextException?.toString()})" }
                } else {
                    log.error(e) { "Unknown exception when writing APC data to DB" }
                }
                throw RuntimeException(e)
            }
        }, dbWriteIntervalSeconds, dbWriteIntervalSeconds, TimeUnit.SECONDS)
    }

    override fun close() {
        dbWriterExecutor.shutdownNow()
    }

    private fun writeBatch(statement: PreparedStatement) {
        if (dbWriterExecutor.isShutdown) {
            throw IllegalStateException("DbWriterService is closed")
        }

        val rows = ArrayList<Pair<APCDataRow, MessageId>>(min(dbMaxWriteBatchSize, writeQueue.size))

        for (i in 1..dbMaxWriteBatchSize) {
            val row = writeQueue.poll()
            if (row == null) {
                break
            } else {
                rows += row
            }
        }

        if (rows.isEmpty()) {
            log.info { "No data to write" }
            return
        }

        log.debug { "Writing ${rows.size} APC data rows to DB" }

        val duration = measureTime {
            for (row in rows) {
                val apcData = row.first
                statement.setShort(1, apcData.dir)
                statement.setShort(2, apcData.oper)
                statement.setInt(3, apcData.veh)
                statement.setString(4, apcData.uniqueVehicleId)
                statement.setObject(5, apcData.tst, Types.TIMESTAMP_WITH_TIMEZONE)
                statement.setLong(6, apcData.tsi)
                statement.setDouble(7, apcData.latitude)
                statement.setDouble(8, apcData.longitude)
                statement.setObject(9, apcData.oday, Types.DATE)
                statement.setObject(10, apcData.start, Types.TIME)
                if (apcData.stop != null) {
                    statement.setInt(11, apcData.stop)
                } else {
                    statement.setNull(11, Types.INTEGER)
                }
                statement.setString(12, apcData.route)
                statement.setString(13, apcData.passengerCountQuality)
                statement.setShort(14, apcData.vehicleLoad)
                statement.setDouble(15, apcData.vehicleLoadRatio)
                statement.setShort(16, apcData.totalPassengersIn)
                statement.setShort(17, apcData.totalPassengersOut)
                statement.setShort(18, apcData.bikesIn)
                statement.setShort(19, apcData.bikesOut)
                statement.setShort(20, apcData.wheelchairsIn)
                statement.setShort(21, apcData.wheelchairsOut)
                statement.setShort(22, apcData.pramsIn)
                statement.setShort(23, apcData.pramsOut)
                statement.setShort(24, apcData.otherIn)
                statement.setShort(25, apcData.otherOut)
                statement.addBatch()
            }

            statement.executeBatch()
        }

        log.info { "Wrote ${rows.size} APC data rows to DB in ${duration.inWholeMilliseconds}ms" }

        rows.map { it.second }.forEach(messageAcknowledger)
    }

    fun addToWriteQueue(apcDataRow: APCDataRow, messageId: MessageId): Unit = writeQueue.put(apcDataRow to messageId)
}