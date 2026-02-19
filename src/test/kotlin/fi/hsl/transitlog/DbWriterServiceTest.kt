package fi.hsl.transitlog

import fi.hsl.transitlog.domain.APCDataRow
import java.sql.Connection
import java.sql.DriverManager
import java.time.Instant
import java.time.LocalDate
import java.time.LocalTime
import java.time.OffsetDateTime
import kotlin.time.ExperimentalTime
import org.apache.pulsar.client.api.MessageId
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.mockito.kotlin.mock
import org.mockito.kotlin.times
import org.mockito.kotlin.verify
import org.testcontainers.containers.PostgreSQLContainer
import org.testcontainers.junit.jupiter.Container
import org.testcontainers.junit.jupiter.Testcontainers
import org.testcontainers.utility.DockerImageName

@OptIn(ExperimentalTime::class)
@Testcontainers
class DbWriterServiceTest {
    companion object {
        const val DB_PASSWORD = "test_password"
        const val DB_USER = "postgres"
        const val WRITE_INTERVAL_SECS = 1L

        @Container
        @JvmField
        val postgres =
            PostgreSQLContainer(DockerImageName.parse("postgres:15-alpine"))
                .withDatabaseName(DB_USER)
                .withExposedPorts(5432)
                .withUsername(DB_USER)
                .withPassword(DB_PASSWORD)
    }

    lateinit var connection: Connection
    lateinit var dbWriterService: DbWriterService
    lateinit var messageAcknowledger: (MessageId) -> Unit

    @BeforeEach
    fun setup() {
        connection =
            DriverManager.getConnection(
                postgres.jdbcUrl + "&reWriteBatchedInserts=true",
                postgres.username,
                postgres.password
            )

        connection
            .prepareStatement(
                "CREATE TABLE passengercount (dir SMALLINT, oper SMALLINT, veh INT, unique_vehicle_id TEXT, tst TIMESTAMP WITH TIME ZONE, tsi BIGINT, latitude REAL, longitude REAL, oday DATE, start TIME, stop INT, route TEXT, passenger_count_quality TEXT, vehicle_load SMALLINT, vehicle_load_ratio REAL, total_passengers_in SMALLINT, total_passengers_out SMALLINT, bikes_in SMALLINT, bikes_out SMALLINT, wheelchairs_in SMALLINT, wheelchairs_out SMALLINT, prams_in SMALLINT, prams_out SMALLINT, other_in SMALLINT, other_out SMALLINT)"
            )
            .execute()

        messageAcknowledger = mock {}
        dbWriterService =
            DbWriterService(connection, messageAcknowledger, 10000, WRITE_INTERVAL_SECS)

        dbWriterService =
            DbWriterService(connection, messageAcknowledger, 10000, WRITE_INTERVAL_SECS)
    }

    @AfterEach
    fun teardown() {
        dbWriterService.close()
        connection.close()
    }

    @Test
    fun `Test writing passenger count data to database`() {
        val messageId = mock<MessageId> {}

        dbWriterService.addToWriteQueue(
            APCDataRow(
                1,
                1,
                1,
                "1/1",
                OffsetDateTime.now(),
                Instant.now().epochSecond,
                0.0,
                0.0,
                LocalDate.now(),
                LocalTime.now(),
                1,
                "1",
                "normal",
                50,
                0.5,
                7,
                5,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0
            ),
            messageId
        )

        Thread.sleep((WRITE_INTERVAL_SECS * 1000 * 2).toLong())

        val results = connection.prepareStatement("SELECT * FROM passengercount").executeQuery()
        results.next()

        val passengersIn = results.getInt("total_passengers_in")
        val passengersOut = results.getInt("total_passengers_out")

        assertEquals(7, passengersIn)
        assertEquals(5, passengersOut)

        verify(messageAcknowledger, times(1)).invoke(messageId)
    }
}
