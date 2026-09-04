package com.tahayavuz.reactive.web;

import com.tahayavuz.reactive.domain.PriceTick;
import com.tahayavuz.reactive.persistence.PriceTickStore;
import com.tahayavuz.reactive.stream.PriceStream;
import com.tahayavuz.reactive.stream.SimulatedPriceSource;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Flux;

import java.math.BigDecimal;
import java.time.Clock;
import java.time.Duration;
import java.util.List;

@RestController
@RequestMapping("/api/v1/prices")
class PriceController {

    private final PriceTickStore store;
    private final Clock clock;
    private final Duration sampleWindow;

    PriceController(PriceTickStore store, Clock clock,
                    @Value("${app.stream.sample-window-ms:500}") long sampleWindowMillis) {
        this.store = store;
        this.clock = clock;
        this.sampleWindow = Duration.ofMillis(sampleWindowMillis);
    }

    /**
     * Canlı fiyat akışı (Server-Sent Events).
     * <p>
     * Dönüş tipi {@code Flux} olduğu için yanıt açık kalır ve değerler üretildikçe
     * gönderilir. İstemci bağlantıyı kapattığında Reactor akışı iptal eder ve üretici
     * durur — bunun için ayrıca bir temizlik koduna gerek yoktur.
     * <p>
     * Akış üç aşamadan geçer: kaynaklar birleştirilir, aynı fiyatın tekrarı elenir,
     * ardından pencere başına yalnızca son değer geçirilir. Amaç, tüketiciyi
     * görmeyeceği veriyle yormamaktır.
     */
    @GetMapping(value = "/{symbol}/stream", produces = MediaType.TEXT_EVENT_STREAM_VALUE)
    Flux<PriceTick> stream(@PathVariable String symbol,
                           @RequestParam(defaultValue = "100") int intervalMillis) {
        Flux<PriceTick> raw = SimulatedPriceSource.merged(
                symbol.toUpperCase(),
                List.of("binance", "kraken"),
                new BigDecimal("100.00"),
                Duration.ofMillis(intervalMillis),
                clock);

        return PriceStream.sampled(PriceStream.onlyChanges(raw), sampleWindow);
    }

    /** Kaydedilmiş son fiyatlar. */
    @GetMapping("/{symbol}/recent")
    Flux<PriceTick> recent(@PathVariable String symbol,
                           @RequestParam(defaultValue = "20") int limit) {
        return store.recent(symbol.toUpperCase(), Math.min(limit, 200));
    }
}
