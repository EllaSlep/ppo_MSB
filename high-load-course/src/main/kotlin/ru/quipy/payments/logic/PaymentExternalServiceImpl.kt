package ru.quipy.payments.logic

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import okhttp3.OkHttpClient
import okhttp3.Request
import okhttp3.RequestBody
import org.slf4j.LoggerFactory
import ru.quipy.common.utils.LeakingBucketRateLimiter
import ru.quipy.common.utils.SlidingWindowRateLimiter
import ru.quipy.core.EventSourcingService
import ru.quipy.payments.api.PaymentAggregate
import java.net.SocketTimeoutException
import java.time.Duration
import java.time.Instant
import java.util.*
import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.Semaphore


// Advice: always treat time as a Duration
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
    private val requestAverageProcessingTime = properties.averageProcessingTime
    private val rateLimitPerSec = properties.rateLimitPerSec
    private val parallelRequests = properties.parallelRequests

    private val client = OkHttpClient.Builder().build()

    private val rateLimiter = SlidingWindowRateLimiter(
        rateLimitPerSec.toLong(),
        Duration.ofSeconds(1)
    )

    private val curRequests = Semaphore(parallelRequests)

    private val requestsQueue : Queue<Pair<Request,Long>> = LinkedList()
    // TODO: очередь запросов, те у которых в данный момент < (60-4,9)сек осталось удалять из очереди

    override fun performPaymentAsync(paymentId: UUID, amount: Int, paymentStartedAt: Long, deadline: Long) {
        val transactionId = UUID.randomUUID()
        val request = Request.Builder()
            .url("http://localhost:1234/external/process?serviceName=${serviceName}&accountName=${accountName}&transactionId=$transactionId&paymentId=$paymentId&amount=$amount")
            .post(emptyBody)
            .build()
        val startTime = System.currentTimeMillis()
        requestsQueue.add(Pair(request,startTime))
        logger.warn("[$accountName] adding pair of request and time in queue: ${requestsQueue.last().first} ; ${requestsQueue.last().second}")
        while(requestsQueue.isNotEmpty() && System.currentTimeMillis() -  requestsQueue.peek().second > 55100) {
            val curReq = requestsQueue.poll()
            logger.info("[$accountName] polling request out of queue: $curReq")
        }
        rateLimiter.tickBlocking()
        curRequests.acquire()
        try {
            logger.warn("[$accountName] Sending payment request $paymentId")

            logger.info("[$accountName] sending for $paymentId, txId: $transactionId")

            // Вне зависимости от исхода оплаты важно отметить, что она была отправлена.
            // Это необходимо сделать ВО ВСЕХ СЛУЧАЯХ, поскольку эта информация используется сервисом тестирования.
            paymentESService.update(paymentId) {
                it.logSubmission(success = true, transactionId, now(), Duration.ofMillis(now() - paymentStartedAt))
            }

            if (requestsQueue.find{it.first == request} == null) {
                throw SocketTimeoutException()
            }

            try {
                requestsQueue.remove(requestsQueue.find{it.first == request})
                client.newCall(request).execute().use { response ->
                    val body = try {
                        mapper.readValue(response.body?.string(), ExternalSysResponse::class.java)
                    } catch (e: Exception) {
                        logger.error("[$accountName] [ERROR] error processing txId: $transactionId, payment: $paymentId", e)
                        ExternalSysResponse(transactionId.toString(), paymentId.toString(), false, e.message)
                    }
                    logger.warn("[$accountName] payment processed txId: $transactionId, success: ${body.result}, message: ${body.message}")
                    // Здесь мы обновляем состояние оплаты в зависимости от результата в базе данных оплат.
                    // Это требуется сделать ВО ВСЕХ ИСХОДАХ (успешная оплата / неуспешная / ошибочная ситуация)
                    paymentESService.update(paymentId) {
                        it.logProcessing(body.result, now(), transactionId, reason = body.message)
                    }
                }
            } catch (e: Exception) {
                when (e) {
                    is SocketTimeoutException -> {
                        logger.error("[$accountName] timeout for txId: $transactionId, payment: $paymentId", e)
                        paymentESService.update(paymentId) {
                            it.logProcessing(false, now(), transactionId, reason = "Request timeout.")}
                    }
                    else -> {
                        logger.error("[$accountName] payment error txId: $transactionId, payment: $paymentId", e)
                        paymentESService.update(paymentId) {
                            it.logProcessing(false, now(), transactionId, reason = e.message)
                        }
                    }
                }
            }
        }catch (e : Exception) {
            when (e) {
                is SocketTimeoutException -> {
                    logger.error("[$accountName] timeout for txId: $transactionId, payment: $paymentId", e)
                    paymentESService.update(paymentId) {
                        it.logProcessing(false, now(), transactionId, reason = "Request timeout.")}
                }
            }
        }finally {
            curRequests.release()
        }
    }

    override fun price() = properties.price

    override fun isEnabled() = properties.enabled

    override fun name() = properties.accountName

}

public fun now() = System.currentTimeMillis()