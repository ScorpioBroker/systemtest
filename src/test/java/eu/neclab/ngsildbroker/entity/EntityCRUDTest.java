package eu.neclab.ngsildbroker.entity;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.List;
import java.util.Map;

import com.github.jsonldjava.utils.JsonUtils;

import org.apache.commons.compress.utils.Lists;
import org.apache.http.Header;
import org.apache.http.HttpResponse;
import org.apache.http.client.fluent.Content;
import org.apache.http.client.fluent.Request;
import org.apache.http.client.fluent.Response;
import org.apache.log4j.Logger;
import org.junit.Before;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.images.RemoteDockerImage;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.shaded.com.google.common.collect.Maps;
import org.testcontainers.utility.DockerImageName;

import joptsimple.internal.Strings;

@Testcontainers
public class EntityCRUDTest {

	Logger logger = Logger.getLogger(getClass());
	private static final Map<String, String> kafkaEnv = Maps.newHashMap();
	private static final Map<String, String> postgresEnv = Maps.newHashMap();
	private String host;

	{
		kafkaEnv.put("KAFKA_ADVERTISED_HOST_NAME", "kafka");
		kafkaEnv.put("KAFKA_ZOOKEEPER_CONNECT", "zookeeper:2181");
		kafkaEnv.put("KAFKA_ADVERTISED_PORT", "9092");
		kafkaEnv.put("KAFKA_LOG_RETENTION_MS", "10000");
		kafkaEnv.put("KAFKA_LOG_RETENTION_CHECK_INTERVAL_MS", "5000");
		kafkaEnv.put("ALLOW_PLAINTEXT_LISTENER", "yes");
		postgresEnv.put("POSTGRES_USER", "ngb");
		postgresEnv.put("POSTGRES_PASSWORD", "ngb");
		postgresEnv.put("POSTGRES_DB", "ngb");
	}
	@Container
	public GenericContainer zookeeper = new GenericContainer<>(DockerImageName.parse("zookeeper"))
			.withExposedPorts(2181).withNetworkAliases("zookeeper");
	@Container
	public GenericContainer kafka = new GenericContainer<>(DockerImageName.parse("bitnami/kafka"))
			.withExposedPorts(9092).withNetworkAliases("kafka").withEnv(kafkaEnv).dependsOn(zookeeper);
	@Container
	public GenericContainer postgres = new GenericContainer<>(DockerImageName.parse("postgis/postgis"))
			.withExposedPorts(5432).withNetworkAliases("postgres").withEnv(postgresEnv);

	@Container
	public GenericContainer broker = new GenericContainer<>(
			DockerImageName.parse("scorpiobroker/scorpiobroker:aaio-no-eureka_latest")).withExposedPorts(9090)
			.withNetworkAliases("scorpio").dependsOn(kafka, postgres);;

	@Before
	public void setup() {
		host = "http://" + broker.getHost() + ":9090/";
	}

	protected void runTestPart(String file) throws Exception, IOException {
		Map<String, Object> test = (Map<String, Object>) JsonUtils.fromInputStream(new FileInputStream(file));
		boolean success = false;
		List<String> fails = Lists.newArrayList();
		try {
			List<Map<String, Object>> responses = (List<Map<String, Object>>) test.get("responses");
			for (Map<String, Object> response : responses) {
				Map<String, Object> request = (Map<String, Object>) response.get("originalRequest");
				String method = (String) request.get("method");
				String body = null;
				if (request.containsKey("body")) {
					body = ((Map<String, String>) request.get("body")).get("raw");
				}
				String path = Strings.join((List<String>) ((Map<String, Object>) request.get("url")).get("path"), "/");
				List<Map<String, Object>> headers = (List<Map<String, Object>>) request.get("header");
				int expectedStatusCode = (int) response.get("code");
				List<Map<String, Object>> expectedHeaders = (List<Map<String, Object>>) response.get("header");
				Object expectedBody = response.get("body");
				if (expectedBody != null) {
					expectedBody = JsonUtils.fromString((String) expectedBody);
				}
				String url = host + path;

				Request req;
				switch (method) {
					case "POST":
						req = Request.Post(url);
						break;
					case "PATCH":
						req = Request.Patch(url);
						break;
					case "PUT":
						req = Request.Put(url);
						break;
					case "DELETE":
						req = Request.Delete(url);
						break;
					case "GET":
						req = Request.Get(url);
						break;
					default:
						throw new IllegalArgumentException("Unexpected value: " + method);
				}
				for (Map<String, Object> header : headers) {
					req = req.addHeader((String) header.get("key"), (String) header.get("value"));
				}
				if (body != null) {
					req = req.bodyByteArray(body.getBytes());
				}
				Response brokerResponse = req.execute();
				HttpResponse httpResponse = brokerResponse.returnResponse();
				if (httpResponse.getStatusLine().getStatusCode() != expectedStatusCode) {
					fails.add("Expected response code: " + expectedStatusCode + " but got "
							+ httpResponse.getStatusLine().getStatusCode());
				}
				for (Map<String, Object> expectedHeader : expectedHeaders) {
					String key = (String) expectedHeader.get("key");
					String value = (String) expectedHeader.get("value");
					Header[] tmpHeaders = httpResponse.getHeaders(key);
					if (tmpHeaders.length == 0) {
						fails.add("Expected header " + key + " is not present in reply");
					}
					boolean valueFound = false;
					String values = "";
					for (Header tmpHeader : tmpHeaders) {
						if (tmpHeader.getValue().equals(value)) {
							valueFound = true;
							break;
						}
						values += tmpHeader.getValue();
					}
					if (!valueFound) {
						fails.add("Expected header " + key + " to have value " + value
								+ " but these values were present " + values);
					}
				}
				Content content = brokerResponse.returnContent();
				String httpBody = content.asString();
				if (httpBody.isEmpty()) {
					httpBody = null;
				}
				if (expectedBody == null && httpBody != null) {
					fails.add("Body was expected to be empty but was " + httpBody);
					continue;
				}
				if (expectedBody != null && httpBody != null) {
					fails.add("Body was expected to be " + JsonUtils.toPrettyString(httpBody));
					continue;
				}

				Object receivedBody = JsonUtils.fromString(httpBody);
				if (expectedBody.getClass().equals(receivedBody.getClass())) {
					fails.add("Body was expected to be of type " + expectedBody.getClass() + " but was"
							+ receivedBody.getClass());
					continue;
				}
				checkBodies(receivedBody, expectedBody);
			}

		} catch (Exception e) {
			// do nothing
		}
	}

	private void checkBodies(Object receivedBody, Object expectedBody) {
		// TODO Auto-generated method stub

	}

	@Test
	public void TestEntityCreate() {
		//Request.Post(null)
	}

}
