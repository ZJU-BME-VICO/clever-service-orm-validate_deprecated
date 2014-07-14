package edu.zju.bme.clever.service;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.lang.reflect.Array;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
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
import org.openehr.am.parser.ContentObject;
import org.openehr.am.parser.DADLParser;
import org.openehr.rm.binding.DADLBinding;
import org.openehr.rm.common.archetyped.Locatable;

import edu.zju.bme.archetype2java.Archetype2Java;
import edu.zju.bme.archetype2java.JavaClass;
import edu.zju.bme.archetype2java.JavaField;
import edu.zju.bme.clever.service.util.ArchetypeManipulator;

public enum CleverServiceSingleton {

	INSTANCE;

	private final Logger logger = Logger.getLogger(CleverServiceSingleton.class.getName());

	private Configuration cfg;
	private SessionFactory sessionFactory;

	private boolean serviceStatus = false;

	private CleverServiceSingleton() {
	}

	public synchronized int start() {

		serviceStatus = true;
		return 0;

	}

	public synchronized int stop() {

		serviceStatus = false;
		return 0;

	}

	public synchronized boolean getServiceStatus() {

		logger.info(serviceStatus);
		return serviceStatus;

	}

	public int reconfigure(
			Optional<Collection<String>> archetypes, 
			Optional<Collection<String>> arms) {

		try {
			if (getServiceStatus()) {
				return -1;
			}

			if (sessionFactory != null) {
				sessionFactory.close();
			}

			cfg = new Configuration().configure();
			
			Archetype2Java.INSTANCE.reset();			
			Archetype2Java.INSTANCE.setClassFilePath(
					Thread.currentThread().getContextClassLoader().getResource("").getPath());
			Archetype2Java.INSTANCE.setSourceFilePath(
					Thread.currentThread().getContextClassLoader().getResource("").getPath());
			Archetype2Java.INSTANCE.setPackageName("edu.zju.bme.clever.service.model");

			archetypes.ifPresent(as -> 
				as.forEach(a -> {
					Archetype2Java.INSTANCE.addArchetype(a);
				})
			);
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

	public List<String> select(Optional<String> aql) {

		return select(aql, Optional.empty());

	}

	public List<String> select(Optional<String> aql, Optional<Map<String, Object>> parameters) {
		
		Optional<String> hql = this.aql2hql(aql);
		
		Session s = sessionFactory.openSession();
		Transaction txn = s.beginTransaction();

		try {
			if (!getServiceStatus()) {
				return null;
			}

			long startTime = System.currentTimeMillis();

			Query q = s.createQuery(hql.get());
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
		
		if (obj == null) {
			return;
		}

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
			generateReturnDADL(ArchetypeManipulator.INSTANCE.createArchetypeClassObject(obj, new HashMap<>(), new HashMap<>()), dadlResults);
		}

	}

	public long selectCount(Optional<String> aql) {
		
		return selectCount(aql, Optional.empty());
	}

	public long selectCount(Optional<String> aql, Optional<Map<String, Object>> parameters) {
		
		Optional<String> hql = this.aql2hql(aql);

		Session s = sessionFactory.openSession();
		Transaction txn = s.beginTransaction();

		try {
			if (!getServiceStatus()) {
				return -1;
			}

			long startTime = System.currentTimeMillis();

			Query q = s.createQuery(hql.get());
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

	public int insert(Optional<List<String>> dadls) {

		Session s = sessionFactory.openSession();
		Transaction txn = s.beginTransaction();

		try {
			if (!getServiceStatus()) {
				return -1;
			}

			long startTime = System.currentTimeMillis();
			
			DADLBinding binding = new DADLBinding();
			List<String> archetypeIds = new ArrayList<>();
			List<Locatable> dadlObjects = new ArrayList<>();

			for (String dadl : dadls.get()) {
				logger.info(dadl);
				
				InputStream is = new ByteArrayInputStream(dadl.getBytes(StandardCharsets.UTF_8));
				DADLParser parser = new DADLParser(is);
				ContentObject contentObj = parser.parse();
				Object bp = binding.bind(contentObj);

				if (bp instanceof Locatable) {
					Locatable loc = (Locatable) bp;
					archetypeIds.add(loc.getArchetypeDetails().getArchetypeId().getValue());
					dadlObjects.add(loc);
				}
			}
            
            this.insert(archetypeIds, dadlObjects, s);

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
	
	private void insert(List<String> archetypeIds, List<Locatable> dadlObjects, Session s) 
			throws Exception {
		
        if (archetypeIds.size() <= 0) {
            return;
        }

        List<Object> mappingClassObjects = new ArrayList<>();
		List<String> archetypeIdsLeft = new ArrayList<>();
		List<Locatable> dadlObjectsLeft = new ArrayList<>();
		
		for (int i = 0; i < archetypeIds.size(); i++) {
			String archetypeId = archetypeIds.get(i);
			Locatable loc = dadlObjects.get(i);
			String mappingClassName = Archetype2Java.INSTANCE.getClassNameFromArchetypeId(archetypeId);
            Class<?> compiledMappingClass = Archetype2Java.INSTANCE.getCompiledMappingClassFromMappingClassName(mappingClassName);
            try {
                Object mappingClassObject = compiledMappingClass.newInstance();
                JavaClass mappingClass = Archetype2Java.INSTANCE.getMappingClassFromArchetypeId(archetypeId);
                Set<JavaField> mappingClassFields = mappingClass.getFields();
                mappingClassFields.forEach(f -> {
                    try {
                        JavaClass fieldMappingClass = Archetype2Java.INSTANCE.getMappingClassFromMappingClassName(f.getType());
                        if (fieldMappingClass != null) {
                        	String aql = "from " + fieldMappingClass.getArchetypeName() + " where /uid/value = '" + loc.itemAtPath(f.getArchetypePath()) + "'";
//                        	String hql = ArchetypeManipulator.INSTANCE.Aql2Hql(aql);
                        	Optional<String> hql = this.aql2hql(Optional.ofNullable(aql));
                            Query q = s.createQuery(hql.get());
                            List<?> results = q.list();
                            if (results.size() <= 0) {
                            	archetypeIdsLeft.add(archetypeId);
                            	dadlObjectsLeft.add(loc);
                            } else {
                                compiledMappingClass.getField(f.getName()).set(mappingClassObject, results.get(0));
                            }
                        } else {
                            compiledMappingClass.getField(f.getName()).set(mappingClassObject, loc.itemAtPath(f.getArchetypePath()));
                        }
                    } catch (Exception e) {
                        logger.error("insert", e);
                    }
                });
                mappingClassObjects.add(mappingClassObject);
            } catch (Exception e) {
                logger.error("insert", e);                
            }
		}
        
        if (archetypeIdsLeft.size() == archetypeIds.size()) {
            throw new Exception("DADL insert failed: size = " + archetypeIdsLeft.size());
        }
        
        for (int i = 0; i < dadlObjects.size(); i++) {
			if (!dadlObjectsLeft.contains(dadlObjects.get(i))) {
				s.save(mappingClassObjects.get(i));
			}
		}
        
        this.insert(archetypeIdsLeft, dadlObjectsLeft, s);
        
	}

	public int delete(Optional<String> aql) {

		return delete(aql, Optional.empty());

	}

	public int delete(Optional<String> aql, Optional<Map<String, Object>> parameters) {

		return executeUpdate(aql, parameters);

	}

	public int update(Optional<String> aql) {

		return update(aql, Optional.empty());

	}

	public int update(Optional<String> aql, Optional<Map<String, Object>> parameters) {

		return executeUpdate(aql, parameters);

	}

	protected int executeUpdate(Optional<String> aql, Optional<Map<String, Object>> parameters) {
		
		Optional<String> hql = this.aql2hql(aql);

		Session s = sessionFactory.openSession();
		Transaction txn = s.beginTransaction();

		try {
			if (!getServiceStatus()) {
				return -1;
			}

			long startTime = System.currentTimeMillis();

			Query q = s.createQuery(hql.get());
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

	protected void passParameters(Query q, Optional<Map<String, Object>> parameters) {

		parameters.ifPresent(ps -> ps.keySet().forEach(p -> {
				q.setParameter(p, ps.get(p));				
			})
		);

	}

	public List<String> getSQL(Optional<String> aql) {

		try {
			if (!getServiceStatus()) {
				return null;
			}
			
			Optional<String> hql = this.aql2hql(aql);

			long startTime = System.currentTimeMillis();

			final QueryTranslatorFactory translatorFactory = new ASTQueryTranslatorFactory();
			final SessionFactoryImplementor factory = (SessionFactoryImplementor) sessionFactory;
			final QueryTranslator translator = translatorFactory
					.createQueryTranslator(hql.get(), hql.get(), Collections.EMPTY_MAP, factory, null);
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
	
	public String getArchetypeString(Optional<String> archetypeId) {
		
		try {
			if (!getServiceStatus()) {
				return "";
			}
			
			return Archetype2Java.INSTANCE.getJavaClasses().stream().filter(
					jc -> jc.getArchetypeName().compareTo(archetypeId.get()) == 0).findFirst().map(
							jc -> jc.getArchetypeString()).orElse("");
			
		} catch (Exception e) {
			logger.error(e);
			return "";
		}
		
	}
	
	private Optional<String> aql2hql(Optional<String> aql) {

		logger.info(aql.get());
		
		Optional<String> hql = aql.flatMap(ArchetypeManipulator.INSTANCE::Aql2Hql);
		
		logger.info(hql.get());
		
		return hql;
		
	}

}
