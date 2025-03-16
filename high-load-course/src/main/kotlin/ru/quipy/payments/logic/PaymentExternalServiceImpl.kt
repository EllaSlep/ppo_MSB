package ru.quipy.payments.logic

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import kotlinx.coroutines.*
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
import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.Semaphore

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
    private val rateLimitPerSec = 7L
    private val parallelRequests = 50
    private val processingTimeMs = 700L
    private val requestTimeoutMs = 3500L
    private val maxQueueProcessingPerSec = 10

    private val client = OkHttpClient.Builder().build()
    private val rateLimiter = SlidingWindowRateLimiter(rateLimitPerSec, Duration.ofSeconds(1))
    private val curRequests = Semaphore(parallelRequests)
    private val requestsQueue: Queue<Pair<Request, Long>> = ConcurrentLinkedQueue()

    override fun performPaymentAsync(paymentId: UUID, amount: Int, paymentStartedAt: Long, deadline: Long) {
        CoroutineScope(Dispatchers.IO).launch {
            processPayment(paymentId, amount, paymentStartedAt, deadline)
        }
    }

    private suspend fun processPayment(paymentId: UUID, amount: Int, paymentStartedAt: Long, deadline: Long) {
        val transactionId = UUID.randomUUID()
        val request = Request.Builder()
            .url("http://localhost:1234/external/process?serviceName=${serviceName}&accountName=${accountName}&transactionId=$transactionId&paymentId=$paymentId&amount=$amount")
            .post(emptyBody)
            .build()
        val startTime = System.currentTimeMillis()
        requestsQueue.add(Pair(request, startTime))
        logger.warn("[$accountName] adding request to queue: ${requestsQueue.last().first} ; ${requestsQueue.last().second}")

        while (requestsQueue.isNotEmpty() && System.currentTimeMillis() - requestsQueue.peek().second > requestTimeoutMs) {
            val curReq = requestsQueue.poll()
            logger.info("[$accountName] removing expired request from queue: $curReq")
        }

        rateLimiter.tickBlocking()
        curRequests.acquire()

        try {
            delay(processingTimeMs)
            val availableRequests = minOf(maxQueueProcessingPerSec, requestsQueue.size)
            repeat(availableRequests) {
                val requestPair = requestsQueue.poll() ?: return@repeat
                val requestStartTime = requestPair.second
                val currentTime = System.currentTimeMillis()

                logger.info("[$accountName] deadline for processing request: ${currentTime - requestStartTime} ms")
                client.newCall(requestPair.first).execute().use { response ->
                    val body = try {
                        mapper.readValue(response.body?.string(), ExternalSysResponse::class.java)
                    } catch (e: Exception) {
                        logger.error("[$accountName] [ERROR] error processing txId: $transactionId, payment: $paymentId", e)
                        ExternalSysResponse(transactionId.toString(), paymentId.toString(), false, e.message)
                    }

                    logger.warn(
                        "[$accountName] payment processed txId: $transactionId, success: ${body.result}, " +
                                "message: ${body.message}, httpCode: ${response.code}, response: ${body.message}"
                    )

                    paymentESService.update(paymentId) {
                        it.logProcessing(body.result, now(), transactionId, reason = body.message)
                    }
                }
            }
        } catch (e: Exception) {
            when (e) {
                is SocketTimeoutException -> {
                    logger.error("[$accountName] timeout for txId: $transactionId, payment: $paymentId", e)
                    paymentESService.update(paymentId) {
                        it.logProcessing(false, now(), transactionId, reason = "Request timeout.")
                    }
                }
                else -> {
                    logger.error("[$accountName] payment error txId: $transactionId, payment: $paymentId", e)
                    paymentESService.update(paymentId) {
                        it.logProcessing(false, now(), transactionId, reason = e.message)
                    }
                }
            }
        } finally {
            curRequests.release()
        }
    }

    override fun price() = properties.price
    override fun isEnabled() = properties.enabled
    override fun name() = properties.accountName
}

public fun now() = System.currentTimeMillis()
