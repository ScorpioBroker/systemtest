package eu.neclab.ngsildbroker.base;

import java.io.BufferedReader;
import java.io.IOException;
import java.util.Enumeration;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;
import java.util.stream.Collectors;

import org.apache.commons.compress.utils.Lists;
import org.eclipse.jetty.server.handler.AbstractHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.jsonldjava.utils.JsonUtils;
import com.google.common.collect.Iterators;
import com.google.common.collect.MapDifference;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.collect.Sets.SetView;

import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;

public class MyHandler extends AbstractHandler {
	Logger logger = LoggerFactory.getLogger(MyHandler.class);
	private List<Map<String, Object>> providerMap;
	private Map<Map<String, Object>, Boolean> gotCalled = Maps.newHashMap();

	public MyHandler(List<Map<String, Object>> providerMap) {
		this.providerMap = providerMap;
		for (Map<String, Object> providerDef : providerMap) {
			gotCalled.put(providerDef, false);
		}
	}

	public void addNewDefs(List<Map<String, Object>> providerList) {
		for (Map<String, Object> providerDef : providerList) {
			gotCalled.put(providerDef, false);
		}
		this.providerMap.addAll(providerList);

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
						logger.error(
								expectedRequestHeader.getKey() + " not received from request " + request.toString());
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
								response.sendError(500,
										expectedRequestHeader.getKey() + " was expected to have value " + value
												+ " but had "
												+ JsonUtils.toPrettyString(Lists.newArrayList(header.asIterator())));
								return;
							}
						}
					} else {
						String value = (String) tmp;
						if (!Iterators.contains(header.asIterator(), value)) {
							logger.error(expectedRequestHeader.getKey() + " was expected to have value " + value
									+ " but had " + JsonUtils.toPrettyString(Lists.newArrayList(header.asIterator())));
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
							logger.error("Received result has top level duplicates which is not allowed in NGSI-LD");
							response.sendError(500,
									"Received result has top level duplicates which is not allowed in NGSI-LD");
							return;
						}
						if (expectedList.size() != expectedSet.size()) {
							logger.error("Expected result has top level duplicates which is not allowed in NGSI-LD");
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
									"Body was expected to be " + JsonUtils.toPrettyString(mapExpectedBody) + " but was "
											+ JsonUtils.toPrettyString(mapActualBody));
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
				gotCalled.put(providerDef, true);
			}

		}
		logger.error("requested target not found");
		response.sendError(500, "requested target not found");

	}

	public Map<Map<String, Object>, Boolean> getGotCalled() {
		return gotCalled;
	}

}
