package ru.quipy.payments.logic

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import com.squareup.okhttp.ConnectionPool
import com.squareup.okhttp.*
import org.slf4j.LoggerFactory
import ru.quipy.common.utils.SlidingWindowRateLimiter
import ru.quipy.core.EventSourcingService
import ru.quipy.payments.api.PaymentAggregate
import java.io.IOException
import java.net.SocketTimeoutException
import java.time.Duration
import java.util.*
import java.util.concurrent.*

class PaymentExternalSystemAdapterImpl(
    private val properties: PaymentAccountProperties,
    private val paymentESService: EventSourcingService<UUID, PaymentAggregate, PaymentAggregateState>
) : PaymentExternalSystemAdapter {

    companion object {
        val logger = LoggerFactory.getLogger(PaymentExternalSystemAdapter::class.java)
        val emptyBody = RequestBody.create(MediaType.parse("application/json"), ByteArray(0))
        val mapper = ObjectMapper().registerKotlinModule()
    }

    private val serviceName = properties.serviceName
    private val accountName = properties.accountName
    private val rateLimitPerSec = properties.rateLimitPerSec.toLong()
    private val parallelRequests = properties.parallelRequests
    private val requestTimeoutMs = 50000L
    private val maxRetries = 4

    private val client = OkHttpClient().apply {
        setConnectTimeout(1, TimeUnit.SECONDS)
        setReadTimeout(40, TimeUnit.SECONDS)
        setWriteTimeout(1, TimeUnit.SECONDS)
        dispatcher = Dispatcher().apply {
            maxRequests = 10000
            maxRequestsPerHost = 10000
        }
        connectionPool = ConnectionPool(
            10000,
            10,
            TimeUnit.MINUTES,
        )
    }


    private val rateLimiter = SlidingWindowRateLimiter(rateLimitPerSec, Duration.ofSeconds(1))
    private val executor: ScheduledExecutorService = Executors.newScheduledThreadPool(parallelRequests)

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
            .addHeader("deadline", (now() + 99500).toString())
            .build()

        rateLimiter.tickBlocking()

        sendAsyncWithRetry(request, transactionId, paymentId, 0)
    }

    private fun sendAsyncWithRetry(
        request: Request,
        transactionId: UUID,
        paymentId: UUID,
        attempt: Int
    ) {
        if (attempt >= maxRetries) {
            logger.warn("[$accountName] Max retries reached for txId: $transactionId")
            return
        }

        client.newCall(request).enqueue(object : Callback {
            override fun onFailure(request: Request?, e: IOException?) {
                logger.error("[$accountName] Async failure for txId: $transactionId, payment: $paymentId, attempt: $attempt", e)

                paymentESService.update(paymentId) {
                    it.logProcessing(false, now(), transactionId, reason = e?.message ?: "Unknown error")
                }

                val delayMs = (500L * Math.pow(1.5, attempt.toDouble())).toLong().coerceAtMost(10000L)
                logger.warn("[$accountName] Retry attempt #${attempt + 1} in $delayMs ms")

                executor.schedule({
                    if (request != null) {
                        sendAsyncWithRetry(request, transactionId, paymentId, attempt + 1)
                    }
                }, delayMs, TimeUnit.MILLISECONDS)
            }

            override fun onResponse(response: Response?) {
                val httpCode = response?.code() ?: -1
                val responseBodyStr = response?.body()?.string()

                val body = runCatching {
                    mapper.readValue(responseBodyStr, ExternalSysResponse::class.java)
                }.getOrElse {
                    logger.error("[$accountName] [ERROR] error processing txId: $transactionId, payment: $paymentId", it)
                    ExternalSysResponse(transactionId.toString(), paymentId.toString(), false, it.message)
                }

                logger.warn("[$accountName] payment processed txId: $transactionId, success: ${body.result}, message: ${body.message}, httpCode: $httpCode")

                paymentESService.update(paymentId) {
                    it.logProcessing(body.result, now(), transactionId, reason = body.message)
                }

                response?.body()?.close()
            }
        })
    }



    override fun price() = properties.price
    override fun isEnabled() = properties.enabled
    override fun name() = properties.accountName
}

fun now() = System.currentTimeMillis()