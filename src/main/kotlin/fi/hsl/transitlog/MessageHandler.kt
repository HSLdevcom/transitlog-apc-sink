package fi.hsl.transitlog

import fi.hsl.common.hfp.proto.Hfp
import fi.hsl.common.passengercount.proto.PassengerCount
import fi.hsl.common.pulsar.IMessageHandler
import fi.hsl.common.pulsar.PulsarApplicationContext
import fi.hsl.common.transitdata.TransitdataProperties
import fi.hsl.common.transitdata.TransitdataSchema
import fi.hsl.transitlog.domain.APCDataRow.Companion.toAPCDataRow
import mu.KotlinLogging
import org.apache.pulsar.client.api.Message
import org.apache.pulsar.client.api.MessageId
import java.sql.DriverManager

class MessageHandler(private val pulsarApplicationContext: PulsarApplicationContext) : IMessageHandler {
    private val log = KotlinLogging.logger {}

    private val dbWriterService = DbWriterService(DriverManager.getConnection(pulsarApplicationContext.config!!.getString("application.dbConnectionString")), ::ack)

    private var lastAcknowledgedMessageTime = System.nanoTime()

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
