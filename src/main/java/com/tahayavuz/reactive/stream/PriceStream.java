package com.tahayavuz.reactive.stream;

import com.tahayavuz.reactive.domain.PriceTick;
import reactor.core.publisher.BufferOverflowStrategy;
import reactor.core.publisher.Flux;

import java.time.Duration;
import java.util.List;

/**
 * Fiyat akışlarını birleştiren ve şekillendiren işlemler.
 *
 * <p>Buradaki metotlar saf akış dönüşümleridir: Spring, veritabanı veya HTTP bilmezler.
 * Bu sayede {@code StepVerifier} ile sanal zamanda test edilebilirler — 30 saniyelik bir
 * davranış, 30 saniye beklemeden doğrulanır.
 */
public final class PriceStream {

    private PriceStream() {
    }

    /**
     * Birden çok kaynağı tek akışta birleştirir.
     * <p>
     * {@code merge} kullanılır, {@code concat} değil: {@code concat} ilk kaynağın
     * bitmesini bekler ve sonsuz akışlarda ikinci kaynağa hiç sıra gelmez. Fiyat
     * kaynakları sonsuzdur, dolayısıyla değerler geldikçe geçmelidir.
     */
    public static Flux<PriceTick> merged(List<Flux<PriceTick>> sources) {
        return Flux.merge(sources);
    }

    /**
     * Aynı fiyatın tekrarını eler; yalnızca değişiklikler geçer.
     * <p>
     * Kaynaklar saniyede birçok kez aynı fiyatı bildirebilir. Tüketiciye değişmeyen
     * veriyi göndermek hem bant genişliği hem ekran güncellemesi israfıdır.
     */
    public static Flux<PriceTick> onlyChanges(Flux<PriceTick> source) {
        return source.distinctUntilChanged(tick -> tick.symbol() + "|" + tick.price());
    }

    /**
     * Her pencerede yalnızca en son değeri geçirir.
     * <p>
     * {@code sample}, {@code buffer}'dan farklı olarak biriktirmez: pencerede ne
     * geldiyse sonuncusunu alır, gerisini atar. Bir gösterge panosu için doğru davranış
     * budur — kullanıcı arada kaçan fiyatları değil, güncel olanı görmek ister.
     */
    public static Flux<PriceTick> sampled(Flux<PriceTick> source, Duration window) {
        return source.sample(window);
    }

    /**
     * Seçilen stratejiyle sınırlı tampon uygular.
     *
     * @param capacity tampon boyutu; aşıldığında {@code strategy} devreye girer
     */
    public static Flux<PriceTick> withBackpressure(Flux<PriceTick> source,
                                                   BackpressureStrategy strategy,
                                                   int capacity) {
        return switch (strategy) {
            case BUFFER -> source.onBackpressureBuffer(capacity);
            case DROP_LATEST -> source.onBackpressureBuffer(
                    capacity, tick -> { }, BufferOverflowStrategy.DROP_LATEST);
            case DROP_OLDEST -> source.onBackpressureBuffer(
                    capacity, tick -> { }, BufferOverflowStrategy.DROP_OLDEST);
        };
    }
}
