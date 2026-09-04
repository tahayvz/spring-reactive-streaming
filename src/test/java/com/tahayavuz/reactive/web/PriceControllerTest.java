package com.tahayavuz.reactive.web;

import com.tahayavuz.reactive.AbstractPostgresTest;
import com.tahayavuz.reactive.domain.PriceTick;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.test.web.reactive.server.WebTestClient;
import reactor.core.publisher.Flux;
import reactor.test.StepVerifier;

import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;

@DisplayName("Fiyat API'si")
class PriceControllerTest extends AbstractPostgresTest {

    @Autowired
    private WebTestClient client;

    /**
     * SSE ucu sonsuz bir akıştır; istemci bağlantıyı kapatana kadar biter.
     * {@code take(3)} tam da bunu yapar: üç değer alınır, sonra abonelik iptal edilir ve
     * sunucu tarafındaki üretici durur.
     */
    @Test
    @DisplayName("canlı akış SSE olarak değer üretir")
    void streamShouldEmitTicks() {
        Flux<PriceTick> stream = client.get()
                .uri("/api/v1/prices/BTCUSD/stream?intervalMillis=50")
                .accept(MediaType.TEXT_EVENT_STREAM)
                .exchange()
                .expectStatus().isOk()
                .expectHeader().contentTypeCompatibleWith(MediaType.TEXT_EVENT_STREAM)
                .returnResult(PriceTick.class)
                .getResponseBody()
                .take(3);

        StepVerifier.create(stream)
                .assertNext(tick -> assertThat(tick.symbol()).isEqualTo("BTCUSD"))
                .expectNextCount(2)
                .expectComplete()
                .verify(Duration.ofSeconds(20));
    }

    @Test
    @DisplayName("sembol büyük harfe çevrilir")
    void streamShouldNormaliseSymbol() {
        Flux<PriceTick> stream = client.get()
                .uri("/api/v1/prices/btcusd/stream?intervalMillis=50")
                .accept(MediaType.TEXT_EVENT_STREAM)
                .exchange()
                .expectStatus().isOk()
                .returnResult(PriceTick.class)
                .getResponseBody()
                .take(1);

        StepVerifier.create(stream)
                .assertNext(tick -> assertThat(tick.symbol()).isEqualTo("BTCUSD"))
                .expectComplete()
                .verify(Duration.ofSeconds(20));
    }

    @Test
    @DisplayName("kaydı olmayan sembol boş liste döner")
    void recentShouldReturnEmptyListForUnknownSymbol() {
        client.get()
                .uri("/api/v1/prices/YOKSYM/recent")
                .exchange()
                .expectStatus().isOk()
                .expectBodyList(PriceTick.class)
                .hasSize(0);
    }

    @Test
    @DisplayName("sağlık ucu çalışır")
    void healthShouldBeUp() {
        client.get().uri("/actuator/health")
                .exchange()
                .expectStatus().isOk()
                .expectBody()
                .jsonPath("$.status").isEqualTo("UP");
    }
}
