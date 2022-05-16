package fi.hsl.transitlog

import fi.hsl.transitlog.domain.APCDataRow
import mu.KotlinLogging
import org.apache.pulsar.client.api.MessageId
import java.sql.Connection
import java.sql.PreparedStatement
import java.sql.Types
import java.util.concurrent.Executors
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.TimeUnit
import kotlin.time.ExperimentalTime
import kotlin.time.measureTime

@ExperimentalTime
class DbWriterService(connection: Connection, private val messageAcknowledger: (MessageId) -> Unit, dbWriteIntervalSeconds: Int = 10) {
    private val log = KotlinLogging.logger {}

    companion object {
        private const val MAX_WRITE_BATCH_SIZE = 10000

        private const val DB_INSERT_QUERY = """
            INSERT INTO passengercount (dir, oper, veh, tst, tsi, latitude, longitude, oday, start, stop, route, passengerCountQuality, vehicleLoad, vehicleLoadRatio, totalPassengersIn, totalPassengersOut) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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

        dbWriterExecutor.scheduleWithFixedDelay({
            writeBatch(statement)
        }, dbWriteIntervalSeconds.toLong(), dbWriteIntervalSeconds.toLong(), TimeUnit.SECONDS)
    }

    private fun writeBatch(statement: PreparedStatement) {
        val rows = ArrayList<Pair<APCDataRow, MessageId>>(writeQueue.size)

        for (i in 1..MAX_WRITE_BATCH_SIZE) {
            val row = writeQueue.poll()
            if (row == null) {
                break
            } else {
                rows += row
            }
        }

        log.info { "Writing ${rows.size} APC data rows to DB" }

        val duration = measureTime {
            for (row in rows) {
                val apcData = row.first
                statement.setInt(1, apcData.dir)
                statement.setInt(2, apcData.oper)
                statement.setInt(3, apcData.veh)
                statement.setObject(4, apcData.tst, Types.TIMESTAMP_WITH_TIMEZONE)
                statement.setLong(5, apcData.tsi)
                statement.setDouble(6, apcData.latitude)
                statement.setDouble(7, apcData.longitude)
                statement.setString(8, apcData.oday)
                statement.setString(9, apcData.start)
                if (apcData.stop != null) {
                    statement.setInt(10, apcData.stop)
                } else {
                    statement.setNull(10, Types.INTEGER)
                }
                statement.setString(11, apcData.route)
                statement.setString(12, apcData.passengerCountQuality)
                statement.setInt(13, apcData.vehicleLoad)
                statement.setDouble(14, apcData.vehicleLoadRatio)
                statement.setInt(15, apcData.totalPassengersIn)
                statement.setInt(16, apcData.totalPassengersOut)

                statement.addBatch()
            }

            statement.executeBatch()
        }

        log.info { "Wrote ${rows.size} APC data rows to DB in ${duration.inWholeMilliseconds}ms" }

        rows.map { it.second }.forEach(messageAcknowledger)
    }

    fun addToWriteQueue(apcDataRow: APCDataRow, messageId: MessageId) = writeQueue.add(apcDataRow to messageId)
}