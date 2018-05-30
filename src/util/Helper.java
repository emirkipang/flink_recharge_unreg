package util;

import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.DefaultHttpClient;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.http.HttpResponse;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.DefaultHttpClient;

public class Helper {
	public static String isNull(String text) {
		return (text == null) ? "UNKNOWN" : text;
	}

	public static String isEmpty(String text) {
		text = text.replaceAll("\\s", "");
		return (text.equals("")) ? "0" : text;
	}

	public static String combineFileds(int start, int end, String[] items,
			String delimiter) {
		String result = items[start];
		for (int i = start + 1; i <= end; i++) {
			result = result + delimiter + items[i];
		}

		return result;
	}

	public static String joinRule(String in, int length) {
		int gap = length - in.length();

		if (gap != 0) {
			for (int i = 1; i <= gap; i++) {
				in = "0" + in;
			}
		}

		return in;
	}

	public static void SendAPI(String msisdn) throws Exception {
		DateFormat dateFormat = new SimpleDateFormat("yyyyMMdd_HHmmss");
		Date date = new Date();
		String now = dateFormat.format(date);

		String ADN = "4444";
		String TRX_ID = "recharge_unregister_notification_" + now;
		String MSISDN = msisdn;
		String API = "http://10.251.38.178:7777/SMSGw/Service/SendSMS";
		String MESSAGE = "Jangan tunggu DIBLOKIR, segera registrasikan nomor prabayar Anda. Balas SMS ini dgn ketik ULANG#NIK#NomorKK. Abaikan jika sudah registrasi";

		DefaultHttpClient httpClient = new DefaultHttpClient();
		HttpPost postRequest = new HttpPost(API);
		StringEntity input = new StringEntity(
				""
						+ "<v1:SendSMSRequest xmlns:v1=\"http://www.telkomsel.com/eai/SendSMSNotification/v1.0\">"
						+ "<v1:ADN>" + ADN + "</v1:ADN>" + "<v1:TRX_ID>"
						+ TRX_ID + "</v1:TRX_ID>" + "<v1:MSISDN>" + MSISDN
						+ "</v1:MSISDN>" + "<v1:MESSAGE>" + MESSAGE
						+ "</v1:MESSAGE>" + "</v1:SendSMSRequest>");
		input.setContentType("text/xml");

		postRequest.setEntity(input);
		HttpResponse response = httpClient.execute(postRequest);
	}
}
