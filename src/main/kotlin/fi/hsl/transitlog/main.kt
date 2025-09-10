package fi.hsl.transitlog

import fi.hsl.common.config.ConfigParser
import fi.hsl.common.pulsar.PulsarApplication
import kotlin.time.ExperimentalTime
import mu.KotlinLogging

private val log = KotlinLogging.logger {}

@ExperimentalTime
fun main() {
    log.info { "Starting application" }

    val config = ConfigParser.createConfig()

    try {
        val app = PulsarApplication.newInstance(config)

        val messageHandler = MessageHandler(app.context)

        if (app.context.healthServer != null) {
            log.info { "Healthcheck enabled" }
            app.context.healthServer!!.addCheck(messageHandler::isHealthy)
        }

        app.launchWithHandler(messageHandler)
    } catch (e: Exception) {
        log.error(e) { "Exception at main" }
    }
}
