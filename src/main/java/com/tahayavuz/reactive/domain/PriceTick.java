package com.tahayavuz.reactive.domain;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.Objects;

/**
 * Tek bir fiyat güncellemesi.
 *
 * @param symbol   enstrüman kodu (örn. "BTCUSD")
 * @param price    o andaki fiyat
 * @param source   fiyatı bildiren kaynak; aynı sembol birden çok borsadan gelebilir
 * @param observedAt fiyatın gözlendiği an
 */
public record PriceTick(String symbol, BigDecimal price, String source, Instant observedAt) {

    public PriceTick {
        Objects.requireNonNull(symbol, "symbol null olamaz");
        Objects.requireNonNull(price, "price null olamaz");
        Objects.requireNonNull(source, "source null olamaz");
        Objects.requireNonNull(observedAt, "observedAt null olamaz");
        if (price.signum() < 0) {
            throw new IllegalArgumentException("Fiyat negatif olamaz: " + price);
        }
    }

    public boolean isSamePriceAs(PriceTick other) {
        return other != null
                && symbol.equals(other.symbol)
                && price.compareTo(other.price) == 0;
    }
}
