package com.tahayavuz.reactive.stream;

import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.core.publisher.Flux;

import java.util.ArrayList;
import java.util.List;

/**
 * Tampon dolduktan <b>sonra</b> talep eden abone.
 *
 * <p>Sıralama bu testlerin can alıcı noktası. {@code StepVerifier} ile yazıldığında
 * talep, üretici çalışmadan önce veriliyor; değerler tampona hiç girmiyor ve üç strateji
 * de aynı sonucu veriyor — test hiçbir şey kanıtlamıyor.
 *
 * <p>Burada abone hiçbir şey istemeden bağlanır. Üretici {@code subscribe()} çağrısı
 * içinde senkron olarak tüm değerleri iter; çağrı döndüğünde tamponun son hâli bellidir.
 * Ancak ondan sonra talep edilir. Zamanlamaya değil, çağrı sırasına dayanır — bu yüzden
 * kararsız değildir.
 */
final class BufferProbe {

    private final List<Integer> received = new ArrayList<>();
    private String terminalSignal = "NONE";
    private Subscription subscription;

    private BufferProbe() {
    }

    /**
     * Akışa talepsiz bağlanır, üretimin bitmesini bekler, sonra {@code demand} kadar ister.
     */
    static BufferProbe drain(Flux<Integer> flux, int demand) {
        BufferProbe probe = new BufferProbe();

        flux.subscribe(new Subscriber<>() {
            @Override
            public void onSubscribe(Subscription s) {
                probe.subscription = s;   // talep YOK: üretici tampona itsin
            }

            @Override
            public void onNext(Integer value) {
                probe.received.add(value);
            }

            @Override
            public void onError(Throwable t) {
                probe.terminalSignal = "ERROR:" + t.getClass().getSimpleName();
            }

            @Override
            public void onComplete() {
                probe.terminalSignal = "COMPLETE";
            }
        });

        probe.subscription.request(demand);
        return probe;
    }

    List<Integer> received() {
        return List.copyOf(received);
    }

    String terminalSignal() {
        return terminalSignal;
    }

    boolean completedNormally() {
        return "COMPLETE".equals(terminalSignal);
    }

    boolean failedWithOverflow() {
        return terminalSignal.startsWith("ERROR:");
    }
}
