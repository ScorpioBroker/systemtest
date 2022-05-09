package eu.neclab.ngsildbroker.entity;

import static org.junit.jupiter.api.Assertions.fail;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Enumeration;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.stream.Collector;
import java.util.stream.Collectors;

import org.apache.commons.compress.utils.Lists;
import org.apache.http.Header;
import org.apache.http.HttpResponse;
import org.apache.http.client.fluent.Content;
import org.apache.http.client.fluent.Request;
import org.apache.http.client.fluent.Response;
import org.apache.log4j.Logger;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.handler.AbstractHandler;
import org.eclipse.jetty.util.Jetty;
import org.junit.Before;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.DockerComposeContainer;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import com.github.jsonldjava.utils.JsonUtils;
import com.google.common.collect.Iterators;
import com.google.common.collect.MapDifference;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.collect.Sets.SetView;

import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import joptsimple.internal.Strings;

@Testcontainers
public class EntityCRUDTest {

	Server server;
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
			setupDataProviders(test);
			setupDataCallback(test);
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

				String bodyCheck = checkBodies(receivedBody, expectedBody);
				if (bodyCheck != null) {
					fails.add(bodyCheck);
					continue;
				}
				success = true;
				break;
			}
			if (!success) {
				fail(JsonUtils.toPrettyString(fails));
			}

		} catch (Exception e) {
			// do nothing
		}
	}

	

	private void setupDataCallback(Map<String, Object> test) {
		
	}

	private void setupDataProviders(Map<String, Object> test) {
		Object dataProviders = test.get("dataProviders");
		if (dataProviders == null) {
			return;
		}
		Map<String, Object> providerMap = (Map<String, Object>) dataProviders;
		int port= (int) providerMap.get("port");
		
		List<Map<String, Object>> providerList = (List<Map<String, Object>>) providerMap.get("defs");
		MyHandler handler = new MyHandler(providerList);
		if(server != null && server.isRunning()) {
			try {
				server.stop();
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		server = new Server(port);
		server.setHandler(handler);
		try {
			server.start();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	private String checkBodies(Object receivedBody, Object expectedBody) {
		try {
			if (expectedBody.getClass().equals(receivedBody.getClass())) {
				return "Body was expected to be of type " + expectedBody.getClass() + " but was"
						+ receivedBody.getClass();
			}
			if (expectedBody instanceof Map) {

				MapDifference diff = Maps.difference((Map) receivedBody, (Map) expectedBody);
				if (diff.areEqual()) {
					return null;
				}
				Map missingInExpected = diff.entriesOnlyOnLeft();
				Map missingInReceived = diff.entriesOnlyOnRight();
				String result = "";
				if (missingInExpected == null || !missingInExpected.isEmpty()) {
					result = JsonUtils.toPrettyString(missingInExpected) + " was provided but not expected";
				}
				if (missingInReceived == null || !missingInReceived.isEmpty()) {
					result = JsonUtils.toPrettyString(missingInReceived) + " was expected but not received";
				}
				return result;
			} else if (expectedBody instanceof List) {
				List receivedList = (List) receivedBody;
				List expectedList = (List) expectedBody;
				Set receivedSet = new HashSet(receivedList);
				Set expectedSet = new HashSet(expectedList);
				if (receivedList.size() != receivedSet.size()) {
					return "Received result has top level duplicates which is not allowed in NGSI-LD";
				}
				if (expectedList.size() != expectedSet.size()) {
					return "Expected result has top level duplicates which is not allowed in NGSI-LD";
				}
				SetView missingInReceived = Sets.difference(expectedSet, receivedSet);
				SetView missingInExpected = Sets.difference(receivedSet, expectedSet);
				if (missingInExpected.isEmpty() && missingInReceived.isEmpty()) {
					return null;
				}
				String result = "";
				if (!missingInExpected.isEmpty()) {
					result = JsonUtils.toPrettyString(missingInExpected) + " was provided but not expected";
				}
				if (!missingInReceived.isEmpty()) {
					result = JsonUtils.toPrettyString(missingInReceived) + " was expected but not received";
				}
				return result;
			} else {

			}

			return null;
		} catch (Exception e) {
			return e.getMessage();
		}
	}

	@Test
	public void TestEntityCreate() {
		// Request.Post(null)
	}

	private class MyHandler extends AbstractHandler {

		private List<Map<String, Object>> providerMap;

		public MyHandler(List<Map<String, Object>> providerMap) {
			this.providerMap = providerMap;

		}

		@Override
		public void handle(String target, org.eclipse.jetty.server.Request baseRequest, HttpServletRequest request,
				HttpServletResponse response) throws IOException, ServletException {
			for (Map<String, Object> providerDef : providerMap) {
				Map<String, Object> endpoint = (Map<String, Object>) providerDef.get("endpoint");
				Map<String, Object> expectedRequestHeaders = (Map<String, Object>) providerDef.get("request-headers");
				Map<String, Object> responseHeaders = (Map<String, Object>) providerDef.get("response-headers");
				String path = (String) endpoint.get("path");
				Map<String, String[]> parameters = (Map<String, String[]>) endpoint.get("parameters");
				int responseCode = (int) providerDef.get("response-code");
				Object responseBody = providerDef.get("response-body");
				Object expectedRequestBody = providerDef.get("request-body");
				String requestPath = request.getPathInfo();
				if (requestPath.equals(path) && Maps.difference(request.getParameterMap(), parameters).areEqual()) {
					for (Entry<String, Object> expectedRequestHeader : expectedRequestHeaders.entrySet()) {
						Enumeration<String> header = request.getHeaders(expectedRequestHeader.getKey());
						if (header == null || !header.hasMoreElements()) {
							logger.error(expectedRequestHeader.getKey() + " not received from request "
									+ request.toString());
							response.sendError(500, expectedRequestHeader.getKey() + " not received from request");
							return;
						}
						Object tmp = expectedRequestHeader.getValue();
						if (tmp instanceof List) {
							List<String> valueList = (List<String>) tmp;
							for (String value : valueList) {
								if (!Iterators.contains(header.asIterator(), value)) {
									logger.error(expectedRequestHeader.getKey() + " was expected to have value " + value
											+ " but had "
											+ JsonUtils.toPrettyString(Lists.newArrayList(header.asIterator())));
									response.sendError(500, expectedRequestHeader.getKey()
											+ " was expected to have value " + value + " but had "
											+ JsonUtils.toPrettyString(Lists.newArrayList(header.asIterator())));
									return;
								}
							}
						} else {
							String value = (String) tmp;
							if (!Iterators.contains(header.asIterator(), value)) {
								logger.error(expectedRequestHeader.getKey() + " was expected to have value " + value
										+ " but had "
										+ JsonUtils.toPrettyString(Lists.newArrayList(header.asIterator())));
								response.sendError(500,
										expectedRequestHeader.getKey() + " was expected to have value " + value
												+ " but had "
												+ JsonUtils.toPrettyString(Lists.newArrayList(header.asIterator())));
								return;
							}
						}
					}
					BufferedReader reader = request.getReader();
					String body = reader.lines().collect(Collectors.joining());
					if (expectedRequestBody != null) {
						Object bodyObj = JsonUtils.fromString(body);
						if (expectedRequestBody instanceof List) {
							if (!(bodyObj instanceof List)) {
								logger.error("Body was expected to be a list but was " + bodyObj.getClass());
								response.sendError(500, "Body was expected to be a list but was " + bodyObj.getClass());
								return;
							}
							List receivedList = (List) bodyObj;
							List expectedList = (List) expectedRequestBody;
							Set receivedSet = new HashSet(receivedList);
							Set expectedSet = new HashSet(expectedList);
							if (receivedList.size() != receivedSet.size()) {
								logger.error(
										"Received result has top level duplicates which is not allowed in NGSI-LD");
								response.sendError(500,
										"Received result has top level duplicates which is not allowed in NGSI-LD");
								return;
							}
							if (expectedList.size() != expectedSet.size()) {
								logger.error(
										"Expected result has top level duplicates which is not allowed in NGSI-LD");
								response.sendError(500,
										"Expected result has top level duplicates which is not allowed in NGSI-LD");
								return;
							}
							SetView missingInReceived = Sets.difference(expectedSet, receivedSet);
							SetView missingInExpected = Sets.difference(receivedSet, expectedSet);

							String result = "";
							if (!missingInExpected.isEmpty()) {
								result = JsonUtils.toPrettyString(missingInExpected) + " was provided but not expected";
							}
							if (!missingInReceived.isEmpty()) {
								result = JsonUtils.toPrettyString(missingInReceived) + " was expected but not received";
							}
							if (!result.isEmpty()) {
								logger.error(result);
								response.sendError(500, result);
								return;
							}

						} else if (expectedRequestBody instanceof Map) {
							if (!(bodyObj instanceof Map)) {
								logger.error("Body was expected to be a map but was " + bodyObj.getClass());
								response.sendError(500, "Body was expected to be a map but was " + bodyObj.getClass());
								return;
							}
							Map mapExpectedBody = (Map) expectedRequestBody;
							Map mapActualBody = (Map) bodyObj;
							MapDifference diff = Maps.difference(mapActualBody, mapExpectedBody);
							if (!diff.areEqual()) {
								logger.error("Body was expected to be " + JsonUtils.toPrettyString(mapExpectedBody)
										+ " but was " + JsonUtils.toPrettyString(mapActualBody));
								response.sendError(500,
										"Body was expected to be " + JsonUtils.toPrettyString(mapExpectedBody)
												+ " but was " + JsonUtils.toPrettyString(mapActualBody));
								return;
							}
						}
					} else {
						if (body != null && !body.isBlank()) {
							logger.error("expected no body but got " + body);
							response.sendError(500, "expected no body but got " + body);
							return;
						}
					}
					String actualResponse = "";
					if (responseBody != null) {
						actualResponse = JsonUtils.toPrettyString(responseBody);
					}
					response.sendError(responseCode, actualResponse);
				}

			}
			logger.error("requested target not found");
			response.sendError(500, "requested target not found");

		}

	}

	public static void main(String[] args) {
		Map<String, Object> tmp1 = Maps.newHashMap();
		Map<String, Object> tmp2 = Maps.newHashMap();
		Map<String, Object> tmp3 = Maps.newHashMap();
		Map<String, Object> tmp4 = Maps.newHashMap();
		tmp1.put("same", "entry");
		tmp2.put("same", "entry");
		tmp3.put("same2", "entry2");
		tmp4.put("same2", "entry2");
		MapDifference<String, Object> diff = Maps.difference(tmp1, tmp2);
		tmp1.put("same1", tmp3);
		tmp2.put("same1", tmp4);
		MapDifference<String, Object> diff2 = Maps.difference(tmp1, tmp2);
		Map<String, Object> tmp11 = Maps.newHashMap();
		Map<String, Object> tmp21 = Maps.newHashMap();
		Map<String, Object> tmp31 = Maps.newHashMap();
		Map<String, Object> tmp41 = Maps.newHashMap();
		tmp11.put("same1", "entry1");
		tmp21.put("same1", "entry1");
		tmp31.put("same21", "entry21");
		tmp41.put("same21", "entry21");
		tmp11.put("same11", tmp31);
		tmp21.put("same11", tmp41);
		Set test1 = new HashSet();
		Set test2 = new HashSet();
		test1.add(tmp1);
		test1.add(tmp11);
		test2.add(tmp2);
		test2.add(tmp21);
		SetView diff3 = Sets.difference(test1, test2);
		Set test3 = new HashSet();
		test3.add(tmp2);
		SetView diff4 = Sets.difference(test1, test3);
		SetView diff5 = Sets.difference(test3, test1);
		System.out.println();

	}
}
