package com.tahayavuz.reactive.persistence;

import org.springframework.data.annotation.Id;
import org.springframework.data.relational.core.mapping.Column;
import org.springframework.data.relational.core.mapping.Table;

import java.math.BigDecimal;
import java.time.Instant;

/**
 * {@code price_ticks} tablosunun satır karşılığı.
 * <p>
 * Domain'deki {@code PriceTick}'ten ayrıdır: bu tip tabloyu, o tip iş kavramını temsil
 * eder. Ayrım, tablo şeması değiştiğinde akış işlemlerinin etkilenmemesini sağlar.
 */
@Table("price_ticks")
record PriceTickRow(
        @Id Long id,
        String symbol,
        BigDecimal price,
        String source,
        @Column("observed_at") Instant observedAt) {

    static PriceTickRow forInsert(String symbol, BigDecimal price, String source, Instant observedAt) {
        // id null → Spring Data yeni kayıt olarak ekler
        return new PriceTickRow(null, symbol, price, source, observedAt);
    }
}
