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
	 * Set CDR service status to start
	 * @return
	 * 0 success
	 */
	@WebMethod
	int start();

	/**
	 * Set CDR service status to stop
	 * @return
	 * 0 success
	 */
	@WebMethod
	int stop();

	/**
	 * Get CDR service status
	 * @return
	 * TRUE CDR service status is start
	 * FALSE CDR service status is stop
	 */
	@WebMethod
	boolean getServiceStatus();

	/**
	 * Reconfigure CDR service with input archetypes, CDR service must be in stop status
     * @param archetypes Input archetypes 
     * @param arms Input archetype relational mappings, not used
	 * @return
	 * 0 success
	 * -1 service is start
	 * -2 internal error
	 */
	@WebMethod
	int reconfigure(Collection<String> archetypes, Collection<String> arms);

	/**
	 * Execute select aql
	 * @param aql Select archetype query language
	 * @return
	 * null service is stop or internal error
	 * else results in ODIN format
	 */
	@WebMethod
	List<String> select(String aql);

	@WebMethod	
	List<String> selectParameterized(String aql, @XmlJavaTypeAdapter(MapAdapter.class) Map<String, String> parameters);

	/**
	 * Execute select count aql
	 * @param aql Select count archetype query language
	 * @return
	 * -1 service is stop
	 * -2 internal error
	 * else result count
	 */
	@WebMethod
	long selectCount(String aql);

	@WebMethod
	long selectCountParameterized(String aql, @XmlJavaTypeAdapter(MapAdapter.class) Map<String, String> parameters);

	/**
	 * Insert ODIN format archetype instances into CDR service 
	 * @param dadls ODIN format archetype instances
	 * @return
	 * 0 success
	 * -1 service is stop
	 * -2 internal error
	 */
	@WebMethod
	int insert(List<String> dadls);

	/**
	 * Execute delete aql
	 * @param aql Delete archetype query language
	 * @return
	 * -1 service is stop
	 * -2 internal error
	 * else deleted record count
	 */
	@WebMethod
	int delete(String aql);

	@WebMethod
	int deleteParameterized(String aql, @XmlJavaTypeAdapter(MapAdapter.class) Map<String, String> parameters);

	/**
	 * Execute update aql
	 * @param aql Update archetype query language
	 * @return
	 * -1 service is stop
	 * -2 internal error
	 * else updated record count
	 */
	@WebMethod
	int update(String aql);

	@WebMethod
	int updateParameterized(String aql, @XmlJavaTypeAdapter(MapAdapter.class) Map<String, String> parameters);

	/**
	 * Get sql from aql 
	 * @param aql 
	 * @return 
	 * null service is stop or internal error
	 * else sqls
	 */
	@WebMethod
	List<String> getSQL(String aql);

	/**
	 * Get archetype ids deployed in CDR service
	 * @return 
	 * null service is stop or internal error
	 * else archetype ids 
	 */
	@WebMethod
	Set<String> getArchetypeIds();

	/**
	 * Get archetype content string deployed in CDR service
	 * @param archetypeId
	 * @return 
	 * "" service is stop or internal error
	 * else archetype content string
	 */
	@WebMethod
	String getArchetypeString(String archetypeId);

	/**
	 * Get archetype content strings deployed in CDR service
	 * @param archetypeIds Archetype ids
	 * @return 
	 * "" service is stop or internal error
	 * else archetype content strings
	 */
	@WebMethod
	Set<String> getArchetypeStrings(Set<String> archetypeIds);
	
}
