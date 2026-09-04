package com.tahayavuz.reactive.stream;

import com.tahayavuz.reactive.domain.PriceTick;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink;
import reactor.test.StepVerifier;

import java.math.BigDecimal;
import java.time.Duration;
import java.time.Instant;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Backpressure stratejilerinin farklı davrandığını gösterir.
 *
 * <p>Kurgu: üretici tüketicinin talebini yok sayarak 100 değer iter, tampon 8
 * kapasitelidir. Tüketici hiçbir şey istemeden bağlanır; üretim bittikten sonra 8 değer
 * ister. Böylece tamponda <b>ne kaldığı</b> görülür — asıl fark oradadır. Ayrıntı:
 * {@link BufferProbe}.
 *
 * <p><b>Virtual thread'ler bu problemi çözmez.</b> Bloke olmayı ucuzlatırlar ama
 * üreticiye "yavaşla" diyecek bir kanal taşımazlar. Reaktif akışların {@code request(n)}
 * protokolü bu sinyali taşır; buradaki stratejiler de sinyal karşılanamadığında neyin
 * feda edileceğini belirler.
 */
@DisplayName("Backpressure stratejileri")
class BackpressureTest {

    private static final int CAPACITY = 8;
    private static final int PRODUCED = 100;

    private PriceTick tick(int i) {
        return new PriceTick("BTCUSD", BigDecimal.valueOf(i), "binance", Instant.EPOCH);
    }

    /**
     * Talebi <b>yok sayan</b> üretici — gerçek bir borsa beslemesi gibi.
     * <p>
     * {@code Flux.range} burada işe yaramaz: backpressure'a saygı duyar, yalnızca
     * istenen kadar üretir; tampon hiç dolmaz ve stratejiler tetiklenmez.
     */
    private Flux<PriceTick> demandIgnoringSource() {
        return Flux.create(sink -> {
            for (int i = 0; i < PRODUCED; i++) {
                sink.next(tick(i));
            }
            sink.complete();
        }, FluxSink.OverflowStrategy.IGNORE);
    }

    private Flux<Integer> pricesOf(BackpressureStrategy strategy) {
        return PriceStream.withBackpressure(demandIgnoringSource(), strategy, CAPACITY)
                .map(t -> t.price().intValue());
    }

    @Test
    @DisplayName("BUFFER: kapasite aşılınca hata verir — sessizce veri kaybetmez")
    void bufferShouldFailLoudlyOnOverflow() {
        BufferProbe probe = BufferProbe.drain(pricesOf(BackpressureStrategy.BUFFER), CAPACITY);

        assertThat(probe.received()).containsExactly(0, 1, 2, 3, 4, 5, 6, 7);
        assertThat(probe.failedWithOverflow())
                .as("taşma sessizce yutulmamalı, hata olarak bildirilmeli")
                .isTrue();
    }

    @Test
    @DisplayName("DROP_LATEST: en ESKİ değerler korunur, sonra gelenler atılır")
    void dropLatestShouldKeepOldestValues() {
        BufferProbe probe = BufferProbe.drain(pricesOf(BackpressureStrategy.DROP_LATEST), CAPACITY);

        assertThat(probe.received()).containsExactly(0, 1, 2, 3, 4, 5, 6, 7);
        assertThat(probe.completedNormally()).isTrue();
    }

    @Test
    @DisplayName("DROP_OLDEST: en YENİ değerler korunur — gösterge panosu için doğru davranış")
    void dropOldestShouldKeepNewestValues() {
        BufferProbe probe = BufferProbe.drain(pricesOf(BackpressureStrategy.DROP_OLDEST), CAPACITY);

        assertThat(probe.received()).containsExactly(92, 93, 94, 95, 96, 97, 98, 99);
        assertThat(probe.completedNormally()).isTrue();
    }

    @Test
    @DisplayName("iki DROP stratejisi aynı akıştan farklı veri saklar")
    void dropStrategiesShouldDisagree() {
        BufferProbe latest = BufferProbe.drain(pricesOf(BackpressureStrategy.DROP_LATEST), CAPACITY);
        BufferProbe oldest = BufferProbe.drain(pricesOf(BackpressureStrategy.DROP_OLDEST), CAPACITY);

        assertThat(latest.received()).isNotEqualTo(oldest.received());
        assertThat(latest.received().get(0)).isLessThan(oldest.received().get(0));
    }

    @Test
    @DisplayName("tüketici istemedikçe hiçbir değer akmaz")
    void nothingShouldFlowWithoutDemand() {
        BufferProbe probe = BufferProbe.drain(pricesOf(BackpressureStrategy.DROP_OLDEST), 0);

        assertThat(probe.received()).isEmpty();
    }

    @Test
    @DisplayName("tüketici yetişebiliyorsa hiçbir değer kaybolmaz")
    void noValueShouldBeLostWhenConsumerKeepsUp() {
        Flux<PriceTick> stream = PriceStream.withBackpressure(
                demandIgnoringSource(), BackpressureStrategy.DROP_OLDEST, PRODUCED * 2);

        StepVerifier.create(stream)
                .expectNextCount(PRODUCED)
                .expectComplete()
                .verify(Duration.ofSeconds(5));
    }
}
