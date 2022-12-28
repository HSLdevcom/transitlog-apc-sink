package fi.hsl.transitlog

import fi.hsl.transitlog.domain.APCDataRow
import org.apache.pulsar.client.api.MessageId
import org.mockito.kotlin.any
import org.mockito.kotlin.mock
import org.mockito.kotlin.times
import org.mockito.kotlin.verify
import org.testcontainers.containers.GenericContainer
import org.testcontainers.junit.jupiter.Container
import org.testcontainers.junit.jupiter.Testcontainers
import org.testcontainers.utility.DockerImageName
import java.sql.Connection
import java.sql.DriverManager
import java.time.Instant
import java.time.LocalDate
import java.time.LocalTime
import java.time.OffsetDateTime
import kotlin.test.AfterTest
import kotlin.test.BeforeTest
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.time.ExperimentalTime

@OptIn(ExperimentalTime::class)
@Testcontainers
class DbWriterServiceTest {
    companion object {
        const val DB_PASSWORD = "test_password"
        const val WRITE_INTERVAL_SECS = 1
    }

    @Container
    val postgres = GenericContainer(DockerImageName.parse("postgres:15-alpine"))
        .withEnv("POSTGRES_PASSWORD", DB_PASSWORD)
        .withExposedPorts(5432)

    lateinit var connection: Connection
    lateinit var dbWriterService: DbWriterService

    lateinit var messageAcknowledger: (MessageId) -> Unit

    @BeforeTest
    fun setup() {
        connection = DriverManager.getConnection("jdbc:postgresql://${postgres.host}:${postgres.firstMappedPort}/?user=postgres&reWriteBatchedInserts=true&password=$DB_PASSWORD")

        connection.prepareStatement("CREATE TABLE passengercount (dir SMALLINT, oper SMALLINT, veh INT, unique_vehicle_id TEXT, tst TIMESTAMP WITH TIME ZONE, tsi BIGINT, latitude REAL, longitude REAL, oday DATE, start TIME, stop INT, route TEXT, passenger_count_quality TEXT, vehicle_load SMALLINT, vehicle_load_ratio REAL, total_passengers_in SMALLINT, total_passengers_out SMALLINT)").execute()

        messageAcknowledger = mock { }

        dbWriterService = DbWriterService(connection, messageAcknowledger, WRITE_INTERVAL_SECS)
    }

    @AfterTest
    fun teardown() {
        dbWriterService.close()

        connection.close()
    }

    @Test
    fun `Test writing passenger count data to database`() {
        val messageId = mock<MessageId> {  }

        dbWriterService.addToWriteQueue(
            APCDataRow(1, 1, 1, "1/1", OffsetDateTime.now(), Instant.now().epochSecond, 0.0, 0.0, LocalDate.now(), LocalTime.now(), 1, "1", "normal", 50, 0.5, 7, 5),
            messageId
        )

        Thread.sleep((WRITE_INTERVAL_SECS * 1000 * 2).toLong())

        val results = connection.prepareStatement("SELECT * FROM passengercount").executeQuery()
        results.next()

        val passengersIn = results.getInt("total_passengers_in")
        val passengersOut = results.getInt("total_passengers_out")

        assertEquals(7, passengersIn)
        assertEquals(5, passengersOut)

        verify(messageAcknowledger, times(1)).invoke(any())
    }
}