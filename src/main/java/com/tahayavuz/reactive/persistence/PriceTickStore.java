package com.tahayavuz.reactive.persistence;

import com.tahayavuz.reactive.domain.PriceTick;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/**
 * Fiyat kayıtlarının okunup yazılması.
 * <p>
 * Uygulamanın geri kalanı {@link PriceTick} ile konuşur; satır tipi bu sınıfın içinde
 * kalır ve dışarı sızmaz.
 */
@Component
public class PriceTickStore {

    private final PriceTickRepository repository;

    PriceTickStore(PriceTickRepository repository) {
        this.repository = repository;
    }

    public Mono<PriceTick> save(PriceTick tick) {
        return repository.save(PriceTickRow.forInsert(
                        tick.symbol(), tick.price(), tick.source(), tick.observedAt()))
                .map(PriceTickStore::toDomain);
    }

    /**
     * Akıştaki her değeri kaydeder ve kaydedileni geçirir.
     * <p>
     * {@code concatMap} kullanılır, {@code flatMap} değil: {@code flatMap} eşzamanlı
     * çalışır ve kayıt sırasını bozar. Fiyat geçmişinde sıra anlamlıdır.
     */
    public Flux<PriceTick> saveAll(Flux<PriceTick> ticks) {
        return ticks.concatMap(this::save);
    }

    public Flux<PriceTick> recent(String symbol, int limit) {
        return repository.findRecent(symbol, limit).map(PriceTickStore::toDomain);
    }

    private static PriceTick toDomain(PriceTickRow row) {
        return new PriceTick(row.symbol(), row.price(), row.source(), row.observedAt());
    }
}
