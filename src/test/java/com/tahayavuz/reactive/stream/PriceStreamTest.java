package com.tahayavuz.reactive.stream;

import com.tahayavuz.reactive.domain.PriceTick;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.test.StepVerifier;

import java.math.BigDecimal;
import java.time.Duration;
import java.time.Instant;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

@DisplayName("Akış kompozisyonu")
class PriceStreamTest {

    private static final Instant T0 = Instant.parse("2026-01-15T10:00:00Z");

    private PriceTick tick(String symbol, String price, String source) {
        return new PriceTick(symbol, new BigDecimal(price), source, T0);
    }

    @Test
    @DisplayName("merge, kaynakları bitmelerini beklemeden birleştirir")
    void mergedShouldInterleaveSources() {
        Flux<PriceTick> binance = Flux.just(tick("BTCUSD", "100", "binance"));
        Flux<PriceTick> kraken = Flux.just(tick("BTCUSD", "101", "kraken"));

        StepVerifier.create(PriceStream.merged(List.of(binance, kraken)))
                .expectNextCount(2)
                .verifyComplete();
    }

    @Test
    @DisplayName("aynı fiyatın tekrarı elenir")
    void onlyChangesShouldDropRepeatedPrices() {
        Flux<PriceTick> source = Flux.just(
                tick("BTCUSD", "100", "binance"),
                tick("BTCUSD", "100", "kraken"),   // aynı fiyat, farklı kaynak → elenir
                tick("BTCUSD", "101", "binance"),
                tick("BTCUSD", "101", "kraken"),
                tick("BTCUSD", "100", "binance"));

        StepVerifier.create(PriceStream.onlyChanges(source).map(t -> t.price().toString()))
                .expectNext("100", "101", "100")
                .verifyComplete();
    }

    @Test
    @DisplayName("farklı semboller birbirinin tekrarını elemez")
    void onlyChangesShouldTreatSymbolsIndependently() {
        Flux<PriceTick> source = Flux.just(
                tick("BTCUSD", "100", "binance"),
                tick("ETHUSD", "100", "binance"));

        StepVerifier.create(PriceStream.onlyChanges(source))
                .expectNextCount(2)
                .verifyComplete();
    }

    /**
     * Sanal zaman: 3 saniyelik bir davranışı 3 saniye beklemeden doğrular.
     * Gerçek zamanla yazılsaydı bu test hem yavaş hem yüklü makinede kararsız olurdu.
     */
    /**
     * Sanal zaman: 3 saniyelik davranış 3 saniye beklemeden doğrulanır. Gerçek zamanla
     * yazılsaydı test hem yavaş hem yüklü makinede kararsız olurdu.
     * <p>
     * Kaynak 100 ms'de bir üretir ve 3 saniyede 30 değer verir; 1 saniyelik pencerelerle
     * örneklendiğinde geriye 4 değer kalır: 1., 2. ve 3. saniyedeki örnekler, artı
     * Reactor'ın tamamlanma anında geçirdiği son değer. Yani 30 değerin 26'sı elenir.
     */
    @Test
    @DisplayName("sample, 30 değeri 4'e indirir (sanal zaman)")
    void sampledShouldThinOutTheStream() {
        StepVerifier.withVirtualTime(() ->
                        PriceStream.sampled(
                                Flux.interval(Duration.ofMillis(100))
                                        .map(i -> tick("BTCUSD", String.valueOf(100 + i), "binance"))
                                        .take(30),
                                Duration.ofSeconds(1)))
                .thenAwait(Duration.ofSeconds(5))
                .expectNextCount(4)
                .expectComplete()
                .verify(Duration.ofSeconds(5));
    }

    @Test
    @DisplayName("boş kaynak listesi boş akış üretir")
    void mergedShouldHandleNoSources() {
        StepVerifier.create(PriceStream.merged(List.of()))
                .verifyComplete();
    }

    @Test
    @DisplayName("kaynaktaki hata aşağı akışa iletilir, yutulmaz")
    void errorShouldPropagate() {
        Flux<PriceTick> failing = Flux.<PriceTick>error(new IllegalStateException("borsa düştü"));

        StepVerifier.create(PriceStream.merged(List.of(failing)))
                .expectErrorMessage("borsa düştü")
                .verify();
    }
}
