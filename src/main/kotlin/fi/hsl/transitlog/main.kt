package fi.hsl.transitlog

import fi.hsl.common.config.ConfigParser
import fi.hsl.common.pulsar.PulsarApplication
import mu.KotlinLogging
import kotlin.time.ExperimentalTime

private val log = KotlinLogging.logger {}

@ExperimentalTime
fun main() {
    log.info { "Starting application" }

    val config = ConfigParser.createConfig()

    try {
        val app = PulsarApplication.newInstance(config)

        val messageHandler = MessageHandler(app.context)
        app.launchWithHandler(messageHandler)

        log.info { "Started handling messages" }
    } catch (e: Exception) {
        log.error(e) { "Exception at main" }
    }
}