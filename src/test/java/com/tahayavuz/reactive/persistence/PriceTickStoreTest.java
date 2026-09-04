package com.tahayavuz.reactive.persistence;

import com.tahayavuz.reactive.AbstractPostgresTest;
import com.tahayavuz.reactive.domain.PriceTick;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import reactor.core.publisher.Flux;
import reactor.test.StepVerifier;

import java.math.BigDecimal;
import java.time.Duration;
import java.time.Instant;
import java.util.UUID;

@DisplayName("PriceTickStore — gerçek PostgreSQL")
class PriceTickStoreTest extends AbstractPostgresTest {

    private static final Duration TIMEOUT = Duration.ofSeconds(10);

    @Autowired
    private PriceTickStore store;

    private PriceTick tick(String symbol, String price, Instant at) {
        return new PriceTick(symbol, new BigDecimal(price), "binance", at);
    }

    private String uniqueSymbol() {
        return "S" + UUID.randomUUID().toString().substring(0, 8).toUpperCase();
    }

    @Test
    @DisplayName("kaydedilen fiyat geri okunabilir")
    void shouldPersistAndReadBack() {
        String symbol = uniqueSymbol();

        StepVerifier.create(store.save(tick(symbol, "100.5000", Instant.now())))
                .assertNext(saved -> {
                    org.assertj.core.api.Assertions.assertThat(saved.symbol()).isEqualTo(symbol);
                    org.assertj.core.api.Assertions.assertThat(saved.price())
                            .isEqualByComparingTo("100.5000");
                })
                .verifyComplete();

        StepVerifier.create(store.recent(symbol, 10))
                .expectNextCount(1)
                .expectComplete()
                .verify(TIMEOUT);
    }

    @Test
    @DisplayName("saveAll akıştaki sırayı korur")
    void saveAllShouldPreserveOrder() {
        String symbol = uniqueSymbol();
        Instant base = Instant.now();

        Flux<PriceTick> ticks = Flux.range(0, 5)
                .map(i -> tick(symbol, String.valueOf(100 + i), base.plusMillis(i * 10L)));

        StepVerifier.create(store.saveAll(ticks).map(t -> t.price().intValue()))
                .expectNext(100, 101, 102, 103, 104)
                .expectComplete()
                .verify(TIMEOUT);
    }

    @Test
    @DisplayName("recent, en yeniden eskiye sıralar")
    void recentShouldReturnNewestFirst() {
        String symbol = uniqueSymbol();
        Instant base = Instant.parse("2026-01-15T10:00:00Z");

        Flux<PriceTick> ticks = Flux.just(
                tick(symbol, "100", base),
                tick(symbol, "101", base.plusSeconds(1)),
                tick(symbol, "102", base.plusSeconds(2)));

        StepVerifier.create(store.saveAll(ticks)).expectNextCount(3).verifyComplete();

        StepVerifier.create(store.recent(symbol, 10).map(t -> t.price().intValue()))
                .expectNext(102, 101, 100)
                .expectComplete()
                .verify(TIMEOUT);
    }

    @Test
    @DisplayName("limit uygulanır")
    void recentShouldRespectLimit() {
        String symbol = uniqueSymbol();
        Instant base = Instant.parse("2026-01-15T10:00:00Z");

        StepVerifier.create(store.saveAll(Flux.range(0, 10)
                        .map(i -> tick(symbol, String.valueOf(100 + i), base.plusSeconds(i)))))
                .expectNextCount(10)
                .verifyComplete();

        StepVerifier.create(store.recent(symbol, 3))
                .expectNextCount(3)
                .expectComplete()
                .verify(TIMEOUT);
    }

    @Test
    @DisplayName("bilinmeyen sembol boş akış döner, hata değil")
    void unknownSymbolShouldReturnEmptyStream() {
        StepVerifier.create(store.recent(uniqueSymbol(), 10))
                .expectComplete()
                .verify(TIMEOUT);
    }
}
