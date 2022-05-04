package fi.hsl.transitlog

import fi.hsl.common.config.ConfigParser
import fi.hsl.common.pulsar.PulsarApplication
import mu.KotlinLogging

private val log = KotlinLogging.logger {}

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