package com.tahayavuz.reactive.stream;

import com.tahayavuz.reactive.domain.PriceTick;
import reactor.core.publisher.Flux;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.Clock;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Bir borsa beslemesinin yerine geçen üretici.
 * <p>
 * Gerçek bir borsaya bağlanmak bu projenin konusu değil; gösterilmek istenen şey akışın
 * <b>şekillendirilmesi</b> ve tüketici yetişemediğinde ne olduğu. Simülasyon, bu
 * davranışı deterministik biçimde tetiklenebilir kılar: üretim hızı parametredir,
 * dolayısıyla "üretici tüketiciden hızlı" durumu isteyerek oluşturulabilir.
 */
public final class SimulatedPriceSource {

    private SimulatedPriceSource() {
    }

    /**
     * Belirtilen aralıkla fiyat üretir; fiyat rastgele küçük adımlarla yürür.
     *
     * @param interval iki güncelleme arası süre — kısaltmak üreticiyi hızlandırır
     */
    public static Flux<PriceTick> of(String symbol, String source,
                                     BigDecimal startPrice, Duration interval, Clock clock) {
        return Flux.interval(interval)
                .map(tick -> new PriceTick(
                        symbol,
                        nextPrice(startPrice, tick),
                        source,
                        clock.instant()));
    }

    /** Birden çok kaynağı tek akışta birleştirir. */
    public static Flux<PriceTick> merged(String symbol, List<String> sources,
                                         BigDecimal startPrice, Duration interval, Clock clock) {
        List<Flux<PriceTick>> streams = sources.stream()
                .map(source -> of(symbol, source, startPrice, interval, clock))
                .toList();

        return PriceStream.merged(streams);
    }

    private static BigDecimal nextPrice(BigDecimal start, long step) {
        double drift = ThreadLocalRandom.current().nextDouble(-0.5, 0.5);
        return start.add(BigDecimal.valueOf(step * 0.01 + drift))
                .setScale(4, RoundingMode.HALF_UP)
                .max(BigDecimal.ZERO);
    }
}
