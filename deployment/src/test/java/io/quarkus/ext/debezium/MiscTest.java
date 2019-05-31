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
		byte[] bytes1 = "ABCDEF".getBytes(StandardCharsets.ISO_8859_1);
		byte[] bytes2 = "ABC1EF".getBytes(StandardCharsets.ISO_8859_1);
		byte[] bytes3 = "ABCDEF".getBytes(StandardCharsets.ISO_8859_1);
		byte[] bytes4 = "ABCDEF4".getBytes(StandardCharsets.ISO_8859_1);

		System.out.println("isArray: " + bytes1.getClass().isArray());
		System.out.println("equals: " + Objects.equals(bytes1, bytes2));
		System.out.println("deepEquals: " + Objects.deepEquals(bytes1, bytes3));
		System.out.println("deepEquals4: " + Objects.deepEquals(bytes1, bytes4));

		long tm = 349891365000L; // '1981-02-01 16:02:45'
		System.out.println("tm: " + new Date(tm));
		LocalDateTime localDateTimeTm = LocalDateTime.ofInstant(new Date(tm).toInstant(), ZoneOffset.UTC);
		System.out.println("localDateTimeTm: " + localDateTimeTm);

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

}
