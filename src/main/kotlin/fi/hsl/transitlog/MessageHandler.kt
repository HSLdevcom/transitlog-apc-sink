package fi.hsl.transitlog

import fi.hsl.common.passengercount.proto.PassengerCount
import fi.hsl.common.pulsar.IMessageHandler
import fi.hsl.common.pulsar.PulsarApplicationContext
import fi.hsl.common.transitdata.TransitdataProperties
import fi.hsl.common.transitdata.TransitdataSchema
import fi.hsl.transitlog.domain.APCDataRow
import fi.hsl.transitlog.domain.APCDataRow.Companion.toAPCDataRow
import mu.KotlinLogging
import org.apache.pulsar.client.api.Message
import org.apache.pulsar.client.api.MessageId
import java.sql.Connection
import java.sql.DriverManager
import java.time.Duration
import java.time.Instant
import java.time.format.DateTimeFormatter
import kotlin.time.ExperimentalTime

@ExperimentalTime
class MessageHandler(private val pulsarApplicationContext: PulsarApplicationContext) : IMessageHandler {
    private val log = KotlinLogging.logger {}

    private val dbWriterService = DbWriterService(createDbConnection(), ::ack)

    private val tstMaxFuture = pulsarApplicationContext.config!!.getDuration("application.apcTstMaxFuture")
    private val tstMaxPast = pulsarApplicationContext.config!!.getDuration("application.apcTstMaxPast")

    private var lastAcknowledgedMessageTime = System.nanoTime()

    fun isHealthy(): Boolean {
        val timeSinceLastAck = Duration.ofNanos(System.nanoTime() - lastAcknowledgedMessageTime)
        val healthy = timeSinceLastAck < pulsarApplicationContext.config!!.getDuration("application.unhealthyIfNoAck")

        if (!healthy) {
            log.warn { "Service unhealthy, last message was acknowledged ${timeSinceLastAck.seconds} seconds ago" }
        }

        return healthy
    }

    private fun createDbConnection(): Connection {
        val dbAddress = pulsarApplicationContext.config!!.getString("db.address")

        val dbUsername = System.getProperty("db.username")
        if (dbUsername == null) {
            log.warn { "Missing DB username" }
        }
        val dbPassword = System.getProperty("db.password")
        if (dbPassword == null) {
            log.warn { "Missing DB password" }
        }

        val connectionString = "jdbc:postgresql://$dbAddress/citus?user=$dbUsername&sslmode=require&reWriteBatchedInserts=true&password=$dbPassword";

        return DriverManager.getConnection(connectionString)
    }

    private fun hasValidTst(apcData: PassengerCount.Data): Boolean {
        val tst = Instant.ofEpochMilli(apcData.payload.tst)
        val receivedAt = Instant.ofEpochMilli(apcData.receivedAt)

        return Duration.between(tst, receivedAt) < tstMaxPast && Duration.between(receivedAt, tst) < tstMaxFuture
    }

    private fun formatTimestampForLog(timestamp: Long): String = "$timestamp (${DateTimeFormatter.ISO_INSTANT.format(Instant.ofEpochMilli(timestamp))})"

    override fun handleMessage(msg: Message<Any>) {
        if (TransitdataSchema.hasProtobufSchema(msg, TransitdataProperties.ProtobufSchema.PassengerCount)) {
            try {
                val apcData = PassengerCount.Data.parseFrom(msg.data)

                if (hasValidTst(apcData)) {
                    dbWriterService.addToWriteQueue(apcData.toAPCDataRow(), msg.messageId)
                } else {
                    log.warn {
                        "Timestamp (tst) of APC message from vehicle ${apcData.payload.oper}/${apcData.payload.veh} was outside of accepted range. Tst: ${formatTimestampForLog(apcData.payload.tst)}, received at: ${formatTimestampForLog(apcData.receivedAt)}"
                    }
                    //Ack message with invalid timestamp so that we don't receive it again
                    ack(msg.messageId)
                }
            } catch (e: Exception) {
                log.warn(e) { "Failed to handle message" }

                //Acknowledge messages that can't be written to the DBso that we don't receive them again
                if (e is APCDataRow.InvalidAPCException) {
                    ack(msg.messageId)
                }
            }
        } else {
            log.warn {
                "Received invalid protobuf schema, expected PassengerCount but received ${TransitdataSchema.parseFromPulsarMessage(msg).orElse(null)}"
            }
        }
    }

    private fun ack(messageId: MessageId) {
        pulsarApplicationContext.consumer!!.acknowledgeAsync(messageId)
            .exceptionally { throwable ->
                //TODO: should we stop the application when ack fails?
                log.error("Failed to ack Pulsar message", throwable)
                null
            }
            .thenRun { lastAcknowledgedMessageTime = System.nanoTime() }
    }
}
