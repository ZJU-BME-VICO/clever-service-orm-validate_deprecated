package edu.zju.bme.clever.service;

import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.Logger;
import org.hibernate.Query;
import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.hibernate.Transaction;
import org.hibernate.boot.registry.StandardServiceRegistry;
import org.hibernate.boot.registry.StandardServiceRegistryBuilder;
import org.hibernate.cfg.Configuration;
import org.hibernate.engine.spi.SessionFactoryImplementor;
import org.hibernate.hql.internal.ast.ASTQueryTranslatorFactory;
import org.hibernate.hql.spi.QueryTranslator;
import org.hibernate.hql.spi.QueryTranslatorFactory;
import org.openehr.rm.binding.DADLBinding;
import org.openehr.rm.common.archetyped.Locatable;

import edu.zju.bme.archetype2java.Archetype2Java;
import edu.zju.bme.archetype2java.JavaClass;
import edu.zju.bme.clever.service.util.ArchetypeManipulator;

public enum CleverServiceSingleton {

	INSTANCE;

	private final Logger logger = Logger.getLogger(CleverServiceSingleton.class.getName());

	private Configuration cfg;
	private SessionFactory sessionFactory;

	private boolean serviceStatus = false;

	private CleverServiceSingleton() {
	}

	public int start() {

		logger.info("start");
		serviceStatus = true;
		return 0;

	}

	public int stop() {

		logger.info("stop");
		serviceStatus = false;
		return 0;

	}

	public boolean getServiceStatus() {

		logger.info(serviceStatus);
		return serviceStatus;

	}

	public int reconfigure(Collection<String> archetypes, Collection<String> arms) {

		logger.info("reconfigure");

		try {
			if (getServiceStatus()) {
				return -1;
			}

			if (sessionFactory != null) {
				sessionFactory.close();
			}

			cfg = new Configuration().configure();
			
			Archetype2Java.INSTANCE.setClassFilePath(Thread.currentThread().getContextClassLoader().getResource("").getPath());
			Archetype2Java.INSTANCE.setSourceFilePath(Thread.currentThread().getContextClassLoader().getResource("").getPath());
			Archetype2Java.INSTANCE.setPackageName("edu.zju.bme.clever.service.model");

			archetypes.forEach(a -> {
				Archetype2Java.INSTANCE.addArchetype(a);
			});
			Map<String, Class<?>> classes = Archetype2Java.INSTANCE.compile();
			classes.values().forEach(c -> {
				cfg.addAnnotatedClass(c);
			});

			StandardServiceRegistry serviceRegistry = 
					new StandardServiceRegistryBuilder().applySettings(cfg.getProperties()).build();
			sessionFactory = cfg.buildSessionFactory(serviceRegistry);

			return 0;
		} catch (Exception e) {
			logger.error(e);
			return -2;
		}

	}

	public List<String> select(String aql) {

		return select(aql, null);

	}

	public List<String> select(String aql, Map<String, Object> parameters) {

		logger.info("select");

		logger.info(aql);
		
		String hql = ArchetypeManipulator.INSTANCE.Aql2Hql(aql);
		
		logger.info(hql);
		
		Session s = sessionFactory.openSession();
		Transaction txn = s.beginTransaction();

		try {
			if (!getServiceStatus()) {
				return null;
			}

			long startTime = System.currentTimeMillis();

			Query q = s.createQuery(hql);
			passParameters(q, parameters);
			@SuppressWarnings("rawtypes")
			List results = q.list();

			txn.commit();
			
			long endTime = System.currentTimeMillis();
			logger.info("aql execute time (ms) : " + (endTime - startTime));

			startTime = System.currentTimeMillis();
			
			List<String> dadlResults = new ArrayList<>();
			for (Object arr : results) {
				if (arr.getClass().isArray()) {
					for (int i = 0; i < Array.getLength(arr); i++) {
						generateReturnDADL(Array.get(arr, i), dadlResults);
					}
				} else {
					generateReturnDADL(arr, dadlResults);
				}
			}

			endTime = System.currentTimeMillis();
			logger.info("generate dadl time (ms) : " + (endTime - startTime));
			
			return dadlResults;
		} catch (Exception e) {
    		try {
    			txn.rollback();
    		} catch (Exception rbe) {
    			logger.error("Couldn’t roll back transaction", rbe);
    		}
			logger.error(e);
			return null;
		} finally {
            s.close();
    	}

	}

	protected void generateReturnDADL(Object obj, List<String> dadlResults)
			throws Exception {

		if (obj instanceof Locatable) {
			DADLBinding binding = new DADLBinding();
			Locatable loc = (Locatable) obj;
			String str = binding.toDADLString(loc);
			if (!dadlResults.contains(str)) {
				logger.info(str);
				dadlResults.add(str);

				for (Object associatedObject : loc.getAssociatedObjects().values()) {
					generateReturnDADL(associatedObject, dadlResults);
				}
			}
		}
		else {
			generateReturnDADL(ArchetypeManipulator.INSTANCE.createArchetypeClassObject(obj), dadlResults);
		}

	}

	public long selectCount(String aql) {
		
		return selectCount(aql, null);
	}

	public long selectCount(String aql, Map<String, Object> parameters) {

		logger.info("selectCount");

		logger.info(aql);
		
		String hql = ArchetypeManipulator.INSTANCE.Aql2Hql(aql);
		
		logger.info(hql);

		Session s = sessionFactory.openSession();
		Transaction txn = s.beginTransaction();

		try {
			if (!getServiceStatus()) {
				return -1;
			}

			long startTime = System.currentTimeMillis();

			Query q = s.createQuery(hql);
			passParameters(q, parameters);
			List<?> l = q.list();
			long ret = (Long) l.get(0);

			txn.commit();
			
			long endTime = System.currentTimeMillis();
			logger.info("aql execute time (ms) : " + (endTime - startTime));
			
			logger.info(ret);

			return ret;
		} catch (Exception e) {
    		try {
    			txn.rollback();
    		} catch (Exception rbe) {
    			logger.error("Couldn’t roll back transaction", rbe);
    		}
			logger.error(e);
			return -2;
		} finally {
            s.close();
    	}

	}

	public int insert(List<String> dadls) {

		logger.info("insert");

		Session s = sessionFactory.openSession();
		Transaction txn = s.beginTransaction();

		try {
			if (!getServiceStatus()) {
				return -1;
			}

			long startTime = System.currentTimeMillis();

			List<Object> mappingObjects = new ArrayList<>();

			for (String dadl : dadls) {
				logger.info(dadl);
				mappingObjects.add(ArchetypeManipulator.INSTANCE.createMappingClassObject(dadl));
			}
            
            mappingObjects.forEach((object) -> {
                s.save(object);
            });

			txn.commit();
			
			long endTime = System.currentTimeMillis();
			logger.info("aql execute time (ms) : " + (endTime - startTime));
		} catch (Exception e) {
    		try {
    			txn.rollback();
    		} catch (Exception rbe) {
    			logger.error("Couldn’t roll back transaction", rbe);
    		}
			logger.error(e);
			return -2;
		} finally {
            s.close();
    	}

		return 0;

	}

	public int delete(String aql) {

		return delete(aql, null);

	}

	public int delete(String aql, Map<String, Object> parameters) {

		return executeUpdate(aql, parameters);

	}

	public int update(String aql) {

		return update(aql, null);

	}

	public int update(String aql, Map<String, Object> parameters) {

		return executeUpdate(aql, parameters);

	}

	protected int executeUpdate(String aql, Map<String, Object> parameters) {

		logger.info("executeUpdate");

		logger.info(aql);
		
		String hql = ArchetypeManipulator.INSTANCE.Aql2Hql(aql);
		
		logger.info(hql);

		Session s = sessionFactory.openSession();
		Transaction txn = s.beginTransaction();

		try {
			if (!getServiceStatus()) {
				return -1;
			}

			long startTime = System.currentTimeMillis();

			Query q = s.createQuery(hql);
			passParameters(q, parameters);
			int ret = q.executeUpdate();

			txn.commit();
			
			long endTime = System.currentTimeMillis();
			logger.info("aql execute time (ms) : " + (endTime - startTime));

			logger.info(ret);

			return ret;
		} catch (Exception e) {
    		try {
    			txn.rollback();
    		} catch (Exception rbe) {
    			logger.error("Couldn’t roll back transaction", rbe);
    		}
			logger.error(e);
			return -2;
		} finally {
            s.close();
    	}

	}

	protected void passParameters(Query q, Map<String, Object> parameters) {

		if (parameters != null) {
			parameters.keySet().forEach(p -> {
				q.setParameter(p, parameters.get(p));				
			});
		}

	}

	public List<String> getSQL(String aql) {

		logger.info("getSQL");

		try {
			if (!getServiceStatus()) {
				return null;
			}

			if (aql == null || aql.trim().length() <= 0) {
				return null;
			}
			
			logger.info(aql);
			
			String hql = ArchetypeManipulator.INSTANCE.Aql2Hql(aql);
			
			logger.info(hql);

			long startTime = System.currentTimeMillis();

			final QueryTranslatorFactory translatorFactory = new ASTQueryTranslatorFactory();
			final SessionFactoryImplementor factory = (SessionFactoryImplementor) sessionFactory;
			final QueryTranslator translator = translatorFactory
					.createQueryTranslator(hql, hql, Collections.EMPTY_MAP, factory, null);
			translator.compile(Collections.EMPTY_MAP, false);
			List<String> sqls = translator.collectSqlStrings();
			
			long endTime = System.currentTimeMillis();
			logger.info("aql execute time (ms) : " + (endTime - startTime));
			
			logger.info(sqls);

			return sqls;
		} catch (Exception e) {
			logger.error(e);
			return null;
		}

	}

	public Set<String> getArchetypeIds() {

		logger.info("getArchetypes");
		
		try {
			if (!getServiceStatus()) {
				return null;
			}
			
			Set<String> archetypeIds = new HashSet<>();
			Archetype2Java.INSTANCE.getJavaClasses().forEach(j -> archetypeIds.add(j.getArchetypeName()));
			return archetypeIds;
			
		} catch (Exception e) {
			logger.error(e);
			return null;
		}
		
	}
	
	public String getArchetypeString(String archetypeId) {
		
		logger.info("getArchetypeString");
		
		try {
			if (!getServiceStatus()) {
				return "";
			}

			for (JavaClass jc : Archetype2Java.INSTANCE.getJavaClasses()) {
				if (jc.getArchetypeName().compareTo(archetypeId) == 0) {
					return jc.getArchetypeString();
				}
			}

			return "";
			
		} catch (Exception e) {
			logger.error(e);
			return "";
		}
		
	}

}
