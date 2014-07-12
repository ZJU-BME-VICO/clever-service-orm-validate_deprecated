package edu.zju.bme.clever.service;

import java.util.List;
import java.util.Map;

import javax.jws.WebMethod;
import javax.jws.WebParam;
import javax.jws.WebService;
import javax.xml.bind.annotation.adapters.XmlJavaTypeAdapter;

import edu.zju.bme.clever.service.util.MapAdapter;

@WebService
public interface CleverServiceParameterized {

	@WebMethod
	List<String> select(
			@WebParam(name="aql") String aql, 
			@WebParam(name="parameters") @XmlJavaTypeAdapter(MapAdapter.class) Map<String, String> parameters);

	@WebMethod
	long selectCount(
			@WebParam(name="aql") String aql, 
			@WebParam(name="parameters") @XmlJavaTypeAdapter(MapAdapter.class) Map<String, String> parameters);
	
	@WebMethod
	int delete(
			@WebParam(name="aql") String aql, 
			@WebParam(name="parameters") @XmlJavaTypeAdapter(MapAdapter.class) Map<String, String> parameters);

	@WebMethod
	int update(
			@WebParam(name="aql") String aql, 
			@WebParam(name="parameters") @XmlJavaTypeAdapter(MapAdapter.class) Map<String, String> parameters);
}
