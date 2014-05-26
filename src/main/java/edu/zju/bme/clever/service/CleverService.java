package edu.zju.bme.clever.service;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.jws.WebMethod;
import javax.jws.WebService;
import javax.xml.bind.annotation.adapters.XmlJavaTypeAdapter;

import edu.zju.bme.clever.service.util.MapAdapter;

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
     * @param archetypes
     * @param arms
	 * @return
	 * 0 success
	 * -1 service running
	 * -2 internal error
	 */
	@WebMethod
	int reconfigure(Collection<String> archetypes, Collection<String> arms);

	/**
	 * @param aql
	 * @return
	 * -1 service running
	 */
	@WebMethod
	List<String> select(String aql);

	@WebMethod	
	List<String> selectParameterized(String aql, @XmlJavaTypeAdapter(MapAdapter.class) Map<String, String> parameters);

	/**
	 * @param aql
	 * @return
	 * -1 service running
	 */
	@WebMethod
	long selectCount(String aql);

	@WebMethod
	long selectCountParameterized(String aql, @XmlJavaTypeAdapter(MapAdapter.class) Map<String, String> parameters);

	/**
	 * @param dadls
	 * @return
	 * -1 service running
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

	@WebMethod
	int deleteParameterized(String aql, @XmlJavaTypeAdapter(MapAdapter.class) Map<String, String> parameters);

	/**
	 * @param aql
	 * @return
	 * -1 service running
	 */
	@WebMethod
	int update(String aql);

	@WebMethod
	int updateParameterized(String aql, @XmlJavaTypeAdapter(MapAdapter.class) Map<String, String> parameters);

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
