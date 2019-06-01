package io.quarkus.ext.debezium;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Base64;
import java.util.Date;
import java.util.Objects;

import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

/**
 * https://debezium.io/docs/faq/#how_to_retrieve_decimal_field_from_binary_representation
 */
@Disabled
public class MiscTest {

    @Test
    public void test() {
        Object bytes1 = "ABCDEF".getBytes(StandardCharsets.ISO_8859_1);
        Object bytes2 = "ABC1EF".getBytes(StandardCharsets.ISO_8859_1);
        Object bytes3 = "ABCDEF".getBytes(StandardCharsets.ISO_8859_1);
        Object bytes4 = "ABCDEF4".getBytes(StandardCharsets.ISO_8859_1);

        System.out.println("isArray: " + bytes1.getClass().isArray());
        System.out.println("equals: " + Objects.equals(bytes1, bytes2));
        System.out.println("deepEquals: " + Objects.deepEquals(bytes1, bytes3));
        System.out.println("deepEquals4: " + Objects.deepEquals(bytes1, bytes4));

        long timestamp = 1559391186721L;
        System.out.println("timestamp: " + new Date(timestamp));
        LocalDateTime ldt = LocalDateTime.ofInstant(new Date(timestamp).toInstant(), ZoneId.systemDefault());
        System.out.println("LocalDateTime timestamp: " + ldt);

        long tm = 349891365000L; // '1981-02-01 16:02:45'
        System.out.println("tm: " + new Date(tm));
        LocalDateTime localDateTimeTm = LocalDateTime.ofInstant(new Date(tm).toInstant(), ZoneOffset.UTC);
        System.out.println("localDateTime tm: " + localDateTimeTm);

        String iso8601Str = "2019-05-31T02:27:01Z";
        ZonedDateTime zonedDateTime = ZonedDateTime.parse(iso8601Str, DateTimeFormatter.ISO_OFFSET_DATE_TIME);

        LocalDateTime localDateTime = LocalDateTime.ofInstant(zonedDateTime.toInstant(), ZoneId.systemDefault());
        System.out.println("localDateTime: " + localDateTime);

        int date = 8200; // '1992-06-14'
        LocalDate localDate = LocalDate.ofEpochDay(date);
        System.out.println("localDate: " + localDate);

        int date2 = 15139; // '2011-06-14'
        LocalDate localDate2 = LocalDate.ofEpochDay(date2);
        System.out.println("localDate2: " + localDate2);

        String val = "MPI="; // 12.530
        int scale = 3;

        final BigDecimal decoded = new BigDecimal(new BigInteger(Base64.getDecoder().decode(val)), scale);
        System.out.println("decoded: " + decoded);
    }

    //    @Test
    //    public void test2() {
    //        try {
    //            CompletionStage<Message<DebeziumMessage>> obj = CompletableFuture
    //                    .supplyAsync(() -> MiscTest.of(new DebeziumMessage(null)));
    //            //CompletableFuture<Message<DebeziumMessage>> 
    //            System.out.println("obj.getClass(): " + obj.getClass());
    //            String returnType = DescriptorUtils.classToStringRepresentation(obj.getClass());
    //            System.out.println("returnType: " + returnType);
    //        } catch (Throwable e) {
    //            // TODO Auto-generated catch block
    //            e.printStackTrace();
    //        }
    //    }
    //
    //    public static Message<DebeziumMessage> of(DebeziumMessage data) {
    //        System.out.println("data.class: " + data.getClass().getName());
    //        return () -> data;
    //    }
}
