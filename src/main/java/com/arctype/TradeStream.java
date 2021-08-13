package com.arctype;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;

public class TradeStream {
    private final static String TRADE_TOPIC = "ARCTYPE.public.trade";

    public static void build(StreamsBuilder builder) {
        final Serde<TradeModel> tradeModelSerde = SerdeFactory.createSerdeFor(TradeModel.class, true);
        final Serde<String> idSerde = Serdes.serdeFrom(new IdSerializer(), new IdDeserializer());

        KStream<String, TradeModel> tradeModelKStream =
                builder.stream(TRADE_TOPIC, Consumed.with(idSerde, tradeModelSerde));

        tradeModelKStream.peek((key, value) -> {
            System.out.println(key.toString());
            System.out.println(value.toString());
        });
        tradeModelKStream.map((id, trade) -> {
            TradeModel tradeDoubled = new TradeModel();
            tradeDoubled.price = trade.price * 2;
            tradeDoubled.quantity = trade.quantity;
            tradeDoubled.ticker = trade.ticker;
            return new KeyValue<>(id, tradeDoubled);
        }).to("ARCTYPE.doubled-trades", Produced.with(idSerde, tradeModelSerde));
    }
}
