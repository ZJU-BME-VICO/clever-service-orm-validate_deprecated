package edu.zju.bme.clever.service;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.jws.WebService;
import javax.xml.ws.BindingType;
import javax.xml.ws.soap.SOAPBinding;

@WebService(endpointInterface = "edu.zju.bme.clever.service.CleverService")
@BindingType(value = SOAPBinding.SOAP12HTTP_BINDING)
public class CleverServiceImpl implements CleverService {

	public CleverServiceImpl() {
	}

	@Override
	public int start() {

		return CleverServiceSingleton.INSTANCE.start();

	}

	@Override
	public int stop() {

		return CleverServiceSingleton.INSTANCE.stop();

	}

	@Override
	public boolean getServiceStatus() {

		return CleverServiceSingleton.INSTANCE.getServiceStatus();

	}

	@Override
	public int reconfigure(Collection<String> archetypes, Collection<String> arms) {

		return CleverServiceSingleton.INSTANCE.reconfigure(archetypes, arms);

	}

	@Override
	public List<String> select(String aql) {

		return CleverServiceSingleton.INSTANCE.select(aql);

	}

	@Override
	public List<String> selectParameterized(String aql, Map<String, String> parameters) {

		Map<String, Object> p = new HashMap<>();
		p.putAll(parameters);
		return CleverServiceSingleton.INSTANCE.select(aql, p);

	}

	@Override
	public long selectCount(String aql) {

		return CleverServiceSingleton.INSTANCE.selectCount(aql);

	}

	@Override
	public long selectCountParameterized(String aql, Map<String, String> parameters) {

		Map<String, Object> p = new HashMap<>();
		p.putAll(parameters);
		return CleverServiceSingleton.INSTANCE.selectCount(aql, p);

	}

	@Override
	public int insert(List<String> dadls) {

		return CleverServiceSingleton.INSTANCE.insert(dadls);

	}

	@Override
	public int delete(String aql) {

		return CleverServiceSingleton.INSTANCE.delete(aql);

	}

	@Override
	public int deleteParameterized(String aql, Map<String, String> parameters) {

		Map<String, Object> p = new HashMap<>();
		p.putAll(parameters);
		return CleverServiceSingleton.INSTANCE.delete(aql, p);

	}

	@Override
	public int update(String aql) {

		return CleverServiceSingleton.INSTANCE.update(aql);

	}

	@Override
	public int updateParameterized(String aql, Map<String, String> parameters) {

		Map<String, Object> p = new HashMap<>();
		p.putAll(parameters);
		return CleverServiceSingleton.INSTANCE.update(aql, p);

	}

	@Override
	public List<String> getSQL(String aql) {

		return CleverServiceSingleton.INSTANCE.getSQL(aql);

	}

	@Override
	public Set<String> getArchetypeIds() {

		return CleverServiceSingleton.INSTANCE.getArchetypeIds();
		
	}

	@Override
	public String getArchetypeString(String archetypeId) {

		return CleverServiceSingleton.INSTANCE.getArchetypeString(archetypeId);
		
	}

}
