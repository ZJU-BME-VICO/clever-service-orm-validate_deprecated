package edu.zju.bme.clever.service;

import java.util.Collection;
import java.util.List;
import java.util.Set;

import javax.jws.WebMethod;
import javax.jws.WebService;

@WebService
public interface CleverService {

	/**
	 * @return
	 * 0 success
	 */
	@WebMethod
	int start();

	/**
	 * @return
	 * 0 success
	 */
	@WebMethod
	int stop();

	/**
	 * @return
	 */
	@WebMethod
	boolean getServiceStatus();

	/**
	 * @return
	 * 0 success
	 * -1 service running
	 * -2 internal error
	 */
	@WebMethod
	int reconfigure(Collection<String> archetypes, Collection<String> arms);

	/**
	 * @param aql
	 * @param archetypeId
	 * @return
	 * -1 service running
	 * @throws Exception
	 */
	@WebMethod
	List<String> select(String aql);

	/**
	 * @param aql
	 * @return
	 * -1 service running
	 */
	@WebMethod
	long selectCount(String aql);

	/**
	 * @param dadls
	 * @return
	 * -1 service running
	 * @throws UnsupportedEncodingException
	 * @throws ParseException
	 * @throws RMObjectBuildingException
	 * @throws DADLBindingException
	 */
	@WebMethod
	int insert(List<String> dadls);

	/**
	 * @param aql
	 * @return
	 * -1 service running
	 */
	@WebMethod
	int delete(String aql);

	/**
	 * @param aql
	 * @return
	 * -1 service running
	 */
	@WebMethod
	int update(String aql);

	/**
	 * @param aql
	 * @return sql
	 */
	@WebMethod
	List<String> getSQL(String aql);

	@WebMethod
	Set<String> getArchetypeIds();

	@WebMethod
	String getArchetypeString(String archetypeId);
}
