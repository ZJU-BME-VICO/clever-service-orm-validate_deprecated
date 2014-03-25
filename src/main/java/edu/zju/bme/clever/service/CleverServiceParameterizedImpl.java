package edu.zju.bme.clever.service;

import java.util.List;
import java.util.Map;

import javax.jws.WebService;
import javax.xml.ws.BindingType;
import javax.xml.ws.soap.SOAPBinding;

@WebService(endpointInterface = "edu.zju.bme.clever.service.CleverServiceParameterized")
@BindingType(value = SOAPBinding.SOAP12HTTP_BINDING)
public class CleverServiceParameterizedImpl implements CleverServiceParameterized {

	public CleverServiceParameterizedImpl() {
	}
	
	@Override
	public List<String> select(String aql, Map<String, Object> parameters) {

		return CleverServiceSingleton.INSTANCE.select(aql, parameters);

	}
	
	@Override
	public int delete(String aql, Map<String, Object> parameters) {

		return CleverServiceSingleton.INSTANCE.delete(aql, parameters);

	}

	@Override
	public int update(String aql, Map<String, Object> parameters) {

		return CleverServiceSingleton.INSTANCE.update(aql, parameters);

	}

}
