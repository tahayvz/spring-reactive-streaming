package com.tahayavuz.reactive.persistence;

import org.springframework.data.r2dbc.repository.Query;
import org.springframework.data.repository.reactive.ReactiveCrudRepository;
import reactor.core.publisher.Flux;

interface PriceTickRepository extends ReactiveCrudRepository<PriceTickRow, Long> {

    /**
     * Bir sembolün son kayıtları, yeniden eskiye.
     * <p>
     * {@code Flux} döner: satırlar veritabanından geldikçe akar, hepsi belleğe
     * toplanmaz. Büyük sonuç kümelerinde fark budur — {@code List} dönen bir imza,
     * sonucun tamamını belleğe almak zorundadır.
     */
    @Query("SELECT * FROM price_ticks WHERE symbol = :symbol ORDER BY observed_at DESC LIMIT :limit")
    Flux<PriceTickRow> findRecent(String symbol, int limit);
}
