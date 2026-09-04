package com.tahayavuz.reactive.stream;

/**
 * Üretici tüketiciden hızlı olduğunda ne yapılacağı.
 *
 * <h2>Neden bir seçim gerekiyor?</h2>
 * Sonsuz bir akışta tüketici yetişemiyorsa üç seçenek vardır ve üçü de bir şeyden
 * vazgeçer: bellek, veri ya da hız. Kütüphane bu kararı sizin yerinize veremez, çünkü
 * doğru cevap veriye bağlıdır — bir fiyat akışında en yeni değer önemlidir, bir ödeme
 * akışında hiçbir kayıt atılamaz.
 *
 * <p>Virtual thread'ler bu problemi çözmez. Ucuz bloke olmayı sağlarlar; ama hızlı bir
 * üreticiyi yavaşlatacak bir sinyal taşımazlar. Reaktif akışların {@code request(n)}
 * protokolü tam olarak bunu taşır: tüketici ne kadar isteyebileceğini söyler.
 */
public enum BackpressureStrategy {

    /**
     * Fazlalığı sınırlı bir tamponda biriktir; tampon dolarsa hata ver.
     * Kayıp kabul edilemez ve gecikme kısa süreliyse uygundur.
     */
    BUFFER,

    /** Tampon doluyken gelen YENİ değerleri at. Eski veri korunur. */
    DROP_LATEST,

    /** Tampon doluyken EN ESKİ değeri at. Tüketici hep güncel veriyi görür. */
    DROP_OLDEST
}
