package ru.quipy.payments.logic

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import okhttp3.OkHttpClient
import okhttp3.Request
import okhttp3.RequestBody
import org.slf4j.LoggerFactory
import ru.quipy.common.utils.SlidingWindowRateLimiter
import ru.quipy.core.EventSourcingService
import ru.quipy.payments.api.PaymentAggregate
import java.net.SocketTimeoutException
import java.time.Duration
import java.util.*
import java.util.concurrent.*
import kotlin.math.min

class PaymentExternalSystemAdapterImpl(
    private val properties: PaymentAccountProperties,
    private val paymentESService: EventSourcingService<UUID, PaymentAggregate, PaymentAggregateState>
) : PaymentExternalSystemAdapter {

    companion object {
        val logger = LoggerFactory.getLogger(PaymentExternalSystemAdapter::class.java)
        val emptyBody = RequestBody.create(null, ByteArray(0))
        val mapper = ObjectMapper().registerKotlinModule()
    }

    private val serviceName = properties.serviceName
    private val accountName = properties.accountName
    private val rateLimitPerSec = properties.rateLimitPerSec.toLong()
    private val parallelRequests = properties.parallelRequests
    private val requestTimeoutMs = 50000L
    private val maxRetries = 4

    private val client = OkHttpClient.Builder()
        .connectTimeout(1, TimeUnit.SECONDS)
        .readTimeout(40, TimeUnit.SECONDS)
        .writeTimeout(1, TimeUnit.SECONDS)
        .build()

    private val rateLimiter = SlidingWindowRateLimiter(rateLimitPerSec, Duration.ofSeconds(1))
    private val executor = Executors.newFixedThreadPool(parallelRequests)

    override fun performPaymentAsync(paymentId: UUID, amount: Int, paymentStartedAt: Long, deadline: Long) {
        CompletableFuture.runAsync({
            processPayment(paymentId, amount, paymentStartedAt, deadline)
        }, executor)
    }

    private fun processPayment(paymentId: UUID, amount: Int, paymentStartedAt: Long, deadline: Long) {
        val transactionId = UUID.randomUUID()
        val request = Request.Builder()
            .url("http://localhost:1234/external/process?serviceName=$serviceName&accountName=$accountName&transactionId=$transactionId&paymentId=$paymentId&amount=$amount")
            .post(emptyBody)
            .addHeader("deadline", (now() + 49500).toString())
            .build()

        rateLimiter.tickBlocking()

        retryWithDelay(maxRetries) { attempt ->
            try {
                client.newCall(request).execute().use { response ->
                    val body = runCatching {
                        mapper.readValue(response.body?.string(), ExternalSysResponse::class.java)
                    }.getOrElse {
                        logger.error("[$accountName] [ERROR] error processing txId: $transactionId, payment: $paymentId", it)
                        ExternalSysResponse(transactionId.toString(), paymentId.toString(), false, it.message)
                    }

                    logger.warn("[$accountName] payment processed txId: $transactionId, success: ${body.result}, message: ${body.message}, httpCode: ${response.code}")

                    paymentESService.update(paymentId) {
                        it.logProcessing(body.result, now(), transactionId, reason = body.message)
                    }

                    body.result
                }
            } catch (e: SocketTimeoutException) {
                logger.error("[$accountName] timeout for txId: $transactionId, payment: $paymentId", e)
                paymentESService.update(paymentId) {
                    it.logProcessing(false, now(), transactionId, reason = "Request timeout.")
                }
                false
            } catch (e: Exception) {
                logger.error("[$accountName] payment error txId: $transactionId, payment: $paymentId", e)
                paymentESService.update(paymentId) {
                    it.logProcessing(false, now(), transactionId, reason = e.message)
                }
                false
            }
        }
    }

    private fun retryWithDelay(retries: Int, block: (Int) -> Boolean) {
        var attempt = 0
        var delayMs = 500L
        while (attempt < retries) {
            if (block(attempt)) return
            attempt++
            logger.warn("[$accountName] Retry attempt #$attempt in $delayMs ms")
            CompletableFuture.delayedExecutor(delayMs, TimeUnit.MILLISECONDS).execute {}
            delayMs = (delayMs * 1.5).toLong().coerceAtMost(10000L)
        }
    }

    override fun price() = properties.price
    override fun isEnabled() = properties.enabled
    override fun name() = properties.accountName
}

fun now() = System.currentTimeMillis()
