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

    /**
     * İstek parametrelerinin sınırları.
     *
     * <p>Bu testler bir review'dan sonra yazıldı. İki parametre de doğrulanmıyordu:
     *
     * <ul>
     *   <li>{@code intervalMillis} alt sınırsızdı. Sıfır veya negatif değer
     *       {@code Flux.interval}'in alttaki zamanlayıcısında hata veriyordu; daha
     *       sinsisi 1 ms gibi geçerli ama çok küçük değerlerdi — iki kaynakla saniyede
     *       2000 tick, ve Reactor'ın paralel havuzu çekirdek sayısı kadar. Birkaç
     *       bağlantı havuzu tüketip uygulamanın tamamını durdurabilirdi.</li>
     *   <li>{@code limit} yalnızca üstten sınırlıydı. Negatif değer sorguya
     *       {@code LIMIT -1} olarak giriyor ve veritabanı hatasıyla 500 dönüyordu.</li>
     * </ul>
     */
    @Test
    @DisplayName("sifir aralik reddedilir: zamanlayiciyi yakan istek")
    void zeroIntervalIsRejected() {
        client.get().uri("/api/v1/prices/BTCUSD/stream?intervalMillis=0")
                .exchange()
                .expectStatus().isBadRequest();
    }

    @Test
    @DisplayName("negatif aralik reddedilir")
    void negativeIntervalIsRejected() {
        client.get().uri("/api/v1/prices/BTCUSD/stream?intervalMillis=-1")
                .exchange()
                .expectStatus().isBadRequest();
    }

    @Test
    @DisplayName("cok kucuk aralik reddedilir: gecerli gorunur ama CPU yakar")
    void tooSmallIntervalIsRejected() {
        client.get().uri("/api/v1/prices/BTCUSD/stream?intervalMillis=1")
                .exchange()
                .expectStatus().isBadRequest();
    }

    @Test
    @DisplayName("negatif limit 500 DEGIL 400 doner")
    void negativeLimitIsRejected() {
        // Once bu deger sorguya LIMIT -1 olarak giriyordu ve PostgreSQL hatasi
        // 500 olarak disari cikiyordu: istemci hatasi sunucu hatasi gibi gorunuyordu.
        client.get().uri("/api/v1/prices/BTCUSD/recent?limit=-1")
                .exchange()
                .expectStatus().isBadRequest();
    }

    @Test
    @DisplayName("cok buyuk limit reddedilir")
    void tooLargeLimitIsRejected() {
        client.get().uri("/api/v1/prices/BTCUSD/recent?limit=5000")
                .exchange()
                .expectStatus().isBadRequest();
    }

    @Test
    @DisplayName("sinirlar icindeki degerler calisir")
    void valuesWithinBoundsAreAccepted() {
        client.get().uri("/api/v1/prices/BTCUSD/recent?limit=5")
                .exchange()
                .expectStatus().isOk();
    }
}
