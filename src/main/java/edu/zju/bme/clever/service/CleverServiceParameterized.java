package edu.zju.bme.clever.service;

import java.util.List;
import java.util.Map;

import javax.jws.WebMethod;
import javax.jws.WebService;

@WebService
public interface CleverServiceParameterized {

	/**
	 * @param aql
     * @param parameters
	 * @return
	 * -1 service running
	 */
	@WebMethod
	List<String> select(String aql, Map<String, Object> parameters);

	@WebMethod
	long selectCount(String aql, Map<String, Object> parameters);
	
	/**
	 * @param aql
	 * @param parameters
	 * @return
	 * -1 service running
	 */
	@WebMethod
	int delete(String aql, Map<String, Object> parameters);

	/**
	 * @param aql
	 * @param parameters
	 * @return
	 * -1 service running
	 */
	@WebMethod
	int update(String aql, Map<String, Object> parameters);
}
