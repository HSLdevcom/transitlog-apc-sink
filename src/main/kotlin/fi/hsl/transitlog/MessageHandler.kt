package fi.hsl.transitlog

import fi.hsl.common.passengercount.proto.PassengerCount
import fi.hsl.common.pulsar.IMessageHandler
import fi.hsl.common.pulsar.PulsarApplicationContext
import fi.hsl.common.transitdata.TransitdataProperties
import fi.hsl.common.transitdata.TransitdataSchema
import fi.hsl.transitlog.domain.APCDataRow.Companion.toAPCDataRow
import mu.KotlinLogging
import org.apache.pulsar.client.api.Message
import org.apache.pulsar.client.api.MessageId
import java.sql.Connection
import java.sql.DriverManager
import java.time.Duration
import kotlin.time.ExperimentalTime

@ExperimentalTime
class MessageHandler(private val pulsarApplicationContext: PulsarApplicationContext) : IMessageHandler {
    private val log = KotlinLogging.logger {}

    private val dbWriterService = DbWriterService(createDbConnection(), ::ack)

    private var lastAcknowledgedMessageTime = System.nanoTime()

    val isHealthy: Boolean
        get() = Duration.ofNanos(System.nanoTime() - lastAcknowledgedMessageTime)  < pulsarApplicationContext.config!!.getDuration("application.unhealthyIfNoAck")

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

    override fun handleMessage(msg: Message<Any>) {
        if (TransitdataSchema.hasProtobufSchema(msg, TransitdataProperties.ProtobufSchema.PassengerCount)) {
            try {
                val apcData = PassengerCount.Data.parseFrom(msg.data)

                dbWriterService.addToWriteQueue(apcData.toAPCDataRow(), msg.messageId)
            } catch (e: Exception) {
                log.warn(e) { "Failed to handle message" }
                e.printStackTrace()
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
