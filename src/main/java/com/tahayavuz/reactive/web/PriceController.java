package com.tahayavuz.reactive.web;

import com.tahayavuz.reactive.domain.PriceTick;
import com.tahayavuz.reactive.persistence.PriceTickStore;
import com.tahayavuz.reactive.stream.PriceStream;
import com.tahayavuz.reactive.stream.SimulatedPriceSource;
import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
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

/*
 * @Validated BILEREK YOK.
 *
 * Spring Framework 6.1'den beri controller metot parametrelerindeki kisitlar
 * YERLESIK olarak dogrulanir ve ihlal HandlerMethodValidationException uretir;
 * bu da otomatik olarak 400'e eslenir.
 *
 * Sinifa @Validated eklenince bu yerlesik mekanizma DEVRE DISI kalir ve yerine
 * AOP tabanli eski yol calisir. O yol ConstraintViolationException firlatir,
 * WebFlux bunu bilmez ve 500 doner: istemcinin hatasi sunucu arizasi gibi gorunur.
 * Ilk denemede tam olarak bu oldu.
 */
@RestController
@RequestMapping("/api/v1/prices")
class PriceController {

    /**
     * Üretim aralığının alt sınırı.
     *
     * <p>Sınırsızken {@code ?intervalMillis=0} tek bir istekle
     * {@code Flux.interval(Duration.ZERO)} çağırıyordu; alttaki
     * {@code ScheduledThreadPoolExecutor.scheduleAtFixedRate} periyot sıfır veya
     * negatifken hata atar. Daha sinsisi çok küçük ama geçerli değerlerdi: 1 ms,
     * iki kaynakla saniyede 2000 tick demek. Reactor'ın paralel havuzu çekirdek
     * sayısı kadardır; birkaç böyle bağlantı onu tüketir ve uygulamanın tamamı durur.
     */
    private static final int MIN_INTERVAL_MILLIS = 10;

    /** Üst sınır: bunun ötesi akışı canlı tutmanın anlamını yitirir. */
    private static final int MAX_INTERVAL_MILLIS = 60_000;

    /** Tek istekte dönülecek en fazla kayıt. */
    private static final int MAX_LIMIT = 200;

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
                           @RequestParam(defaultValue = "100")
                           @Min(value = MIN_INTERVAL_MILLIS,
                                message = "intervalMillis en az " + MIN_INTERVAL_MILLIS + " olmali")
                           @Max(value = MAX_INTERVAL_MILLIS,
                                message = "intervalMillis en fazla " + MAX_INTERVAL_MILLIS + " olabilir")
                           int intervalMillis) {
        Flux<PriceTick> raw = SimulatedPriceSource.merged(
                symbol.toUpperCase(),
                List.of("binance", "kraken"),
                new BigDecimal("100.00"),
                Duration.ofMillis(intervalMillis),
                clock);

        return PriceStream.sampled(PriceStream.onlyChanges(raw), sampleWindow);
    }

    /**
     * Kaydedilmiş son fiyatlar.
     *
     * <p>{@code limit} önce yalnızca ÜSTTEN sınırlanıyordu ({@code Math.min(limit, 200)}).
     * Negatif bir değer o kontrolden geçip sorguya {@code LIMIT -1} olarak giriyor ve
     * PostgreSQL isteği reddediyordu — kullanıcı 400 yerine 500 görüyordu. Sınır artık
     * iki taraflı ve doğrulama sorguya varmadan yapılıyor.
     */
    @GetMapping("/{symbol}/recent")
    Flux<PriceTick> recent(@PathVariable String symbol,
                           @RequestParam(defaultValue = "20")
                           @Min(value = 1, message = "limit en az 1 olmali")
                           @Max(value = MAX_LIMIT, message = "limit en fazla " + MAX_LIMIT + " olabilir")
                           int limit) {
        return store.recent(symbol.toUpperCase(), limit);
    }
}
