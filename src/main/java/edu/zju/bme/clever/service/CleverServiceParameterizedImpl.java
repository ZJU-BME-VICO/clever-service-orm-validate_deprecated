package edu.zju.bme.clever.service;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import javax.jws.WebService;
import javax.xml.ws.BindingType;
import javax.xml.ws.soap.SOAPBinding;

@WebService(endpointInterface = "edu.zju.bme.clever.service.CleverServiceParameterized")
@BindingType(value = SOAPBinding.SOAP12HTTP_BINDING)
public class CleverServiceParameterizedImpl implements CleverServiceParameterized {

	public CleverServiceParameterizedImpl() {
	}
	
	@Override
	public List<String> select(String aql, Map<String, String> parameters) {

		Map<String, Object> p = new HashMap<>();	
		if (parameters != null) {
			p.putAll(parameters);			
		}
		return CleverServiceSingleton.INSTANCE.select(
				Optional.ofNullable(aql), Optional.ofNullable(p));

	}
	
	@Override
	public long selectCount(String aql, Map<String, String> parameters) {

		Map<String, Object> p = new HashMap<>();
		if (parameters != null) {
			p.putAll(parameters);			
		}
		return CleverServiceSingleton.INSTANCE.selectCount(
				Optional.ofNullable(aql), Optional.ofNullable(p));

	}
	
	@Override
	public int delete(String aql, Map<String, String> parameters) {

		Map<String, Object> p = new HashMap<>();
		if (parameters != null) {
			p.putAll(parameters);			
		}
		return CleverServiceSingleton.INSTANCE.delete(
				Optional.ofNullable(aql), Optional.ofNullable(p));

	}

	@Override
	public int update(String aql, Map<String, String> parameters) {

		Map<String, Object> p = new HashMap<>();
		if (parameters != null) {
			p.putAll(parameters);			
		}
		return CleverServiceSingleton.INSTANCE.update(
				Optional.ofNullable(aql), Optional.ofNullable(p));

	}

}
