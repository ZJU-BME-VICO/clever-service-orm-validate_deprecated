package edu.zju.bme.clever.service.util;

import java.lang.reflect.Constructor;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang.ClassUtils;
import org.apache.log4j.Logger;
import org.hibernate.MappingException;
import org.hibernate.PropertyNotFoundException;
import org.hibernate.collection.internal.PersistentBag;
import org.hibernate.property.BasicPropertyAccessor;
import org.hibernate.property.ChainedPropertyAccessor;
import org.hibernate.property.DirectPropertyAccessor;
import org.hibernate.property.Getter;
import org.hibernate.property.PropertyAccessor;
import org.hibernate.property.PropertyAccessorFactory;
import org.hibernate.property.Setter;
import org.joda.time.DateTime;
import org.openehr.am.archetype.Archetype;
import org.openehr.am.archetype.constraintmodel.CObject;
import org.openehr.build.RMObjectBuilder;
import org.openehr.build.SystemValue;
import org.openehr.rm.common.archetyped.Locatable;
import org.openehr.rm.datastructure.itemstructure.representation.Element;
import org.openehr.rm.datatypes.quantity.datetime.DvDateTimeParser;
import org.openehr.rm.datatypes.text.CodePhrase;
import org.openehr.rm.support.measurement.MeasurementService;
import org.openehr.rm.support.measurement.SimpleMeasurementService;
import org.openehr.rm.support.terminology.TerminologyService;
import org.openehr.rm.util.GenerationStrategy;
import org.openehr.rm.util.SkeletonGenerator;
import org.openehr.terminology.SimpleTerminologyService;

import edu.zju.bme.archetype2java.Archetype2Java;
import edu.zju.bme.archetype2java.JavaClass;

public enum ArchetypeManipulator {

	INSTANCE;

	private Logger logger = Logger.getLogger(ArchetypeManipulator.class.getName());
	
	private static final PropertyAccessor BASIC_PROPERTY_ACCESSOR = new BasicPropertyAccessor();
	private static final PropertyAccessor DIRECT_PROPERTY_ACCESSOR = new DirectPropertyAccessor();
	
	private RMObjectBuilder rmBuilder = null;

	protected CodePhrase lang = new CodePhrase("ISO_639-1", "en");
	protected CodePhrase charset = new CodePhrase("IANA_character-sets", "UTF-8");
	protected TerminologyService ts = null;
	protected MeasurementService ms = null;
	
	private ArchetypeManipulator() {
		try {
			ts = SimpleTerminologyService.getInstance();
			ms = SimpleMeasurementService.getInstance();

			Map<SystemValue, Object> values = new HashMap<>();
			values.put(SystemValue.LANGUAGE, lang);
			values.put(SystemValue.CHARSET, charset);
			values.put(SystemValue.ENCODING, charset);
			values.put(SystemValue.TERMINOLOGY_SERVICE, ts);
			values.put(SystemValue.MEASUREMENT_SERVICE, ms);

			rmBuilder = new RMObjectBuilder(values);
		} catch (Exception e) {
			throw new RuntimeException(
					"failed to start terminology or measure service");
		}		
	}
	
	public void setArchetypeValues(
			Locatable loc, Map<String, Object> values, Archetype archetype) 
					throws Exception {
		for (String path : values.keySet()) {
			setArchetypeValue(loc, path, values.get(path), archetype);
		}
	}
	
	public void setArchetypeValue(
			Locatable loc, String propertyPath, Object propertyValue, Archetype archetype) 
					throws Exception {
		
		Map<String, CObject> pathNodeMap = archetype.getPathNodeMap();
		String nodePath = getArchetypeNodePath(archetype, propertyPath);
		if (nodePath.compareTo(propertyPath) == 0) {
			loc.set(nodePath, propertyValue);
		} else {
			CObject node = pathNodeMap.get(nodePath);
			Object target = loc.itemAtPath(nodePath);
			if (target == null) {
				Class<?> klass = rmBuilder.retrieveRMType(node.getRmTypeName());
				Constructor<?> c = klass.getDeclaredConstructor();
				c.setAccessible(true);
				target = c.newInstance();
			}
			
			String attributePath = propertyPath.substring(nodePath.length());
			String[] attributePathSegments = attributePath.split("/");
			Object tempTarget = target;
			for (String pathSegment : attributePathSegments) {
				if (!pathSegment.isEmpty()) {
					Class<?> klass = getter(tempTarget.getClass(), pathSegment).getReturnType();
					PropertyAccessor propertyAccessor = new ChainedPropertyAccessor(
							new PropertyAccessor[] {
									PropertyAccessorFactory.getPropertyAccessor(tempTarget.getClass(), null),
									PropertyAccessorFactory.getPropertyAccessor("field")
							}
					);
					
					Setter setter = propertyAccessor.getSetter(tempTarget.getClass(), pathSegment);
					if (klass.isPrimitive() || 
							ClassUtils.wrapperToPrimitive(klass) != null ||
							String.class.isAssignableFrom(klass) ||
							Set.class.isAssignableFrom(klass) ||
							List.class.isAssignableFrom(klass)) {						
						if (propertyValue instanceof Locatable) {
							String uid = ((Locatable) propertyValue).getUid().getValue();
							setter.set(tempTarget, uid, null);
							loc.getAssociatedObjects().put(uid, propertyValue);
						} else {
							if (propertyValue instanceof String) {
								if (Short.class.isAssignableFrom(klass)) {
									setter.set(tempTarget, Short.parseShort((String) propertyValue), null);									
								}
								if (Integer.class.isAssignableFrom(klass)) {
									setter.set(tempTarget, Integer.parseInt((String) propertyValue), null);									
								}
								if (Long.class.isAssignableFrom(klass)) {
									setter.set(tempTarget, Long.parseLong((String) propertyValue), null);									
								}
								if (Float.class.isAssignableFrom(klass)) {
									setter.set(tempTarget, Float.parseFloat((String) propertyValue), null);
								}
								if (Double.class.isAssignableFrom(klass)) {
									setter.set(tempTarget, Double.parseDouble((String) propertyValue), null);
								}
								if (Boolean.class.isAssignableFrom(klass)) {
									setter.set(tempTarget, Boolean.parseBoolean((String) propertyValue), null);
								}
								if (DateTime.class.isAssignableFrom(klass)) {
									setter.set(tempTarget, DvDateTimeParser.parseDateTime((String) propertyValue), null);
								}
								if (String.class.isAssignableFrom(klass)) {
									setter.set(tempTarget, propertyValue, null);
								}
							} else {
								setter.set(tempTarget, propertyValue, null);
							}
						}
					} else {
						Object value = klass.newInstance();
						setter.set(tempTarget, value, null);
						tempTarget = value;								
					}
				}
			}
		}
	}

	public String getArchetypeNodePath(Archetype archetype, String name) {
		Map<String, CObject> patheNodeMap = archetype.getPathNodeMap();
		Set<String> pathSet = patheNodeMap.keySet();
		String nodePath = "";
		for (String path : pathSet) {
			if (name.startsWith(path)) {
				if (path.length() > nodePath.length()) {
					nodePath = path;
				}
			}
		}			
		
		return nodePath;
	}
	
	public Object createArchetypeClassObject(Object obj, Map<Object, Object> processedObjs, Map<Object, String> processedObjIds) throws Exception {	
		Class<?> compiledMappingClass = Archetype2Java.INSTANCE.getCompiledMappingClassFromMappingClassName(obj.getClass().getSimpleName());
		JavaClass mappingClass = Archetype2Java.INSTANCE.getMappingClassFromMappingClassName(obj.getClass().getSimpleName());
		SkeletonGenerator generator = SkeletonGenerator.getInstance();
		Object result = generator.create(mappingClass.getArchetype(), GenerationStrategy.MAXIMUM_EMPTY);		
		if (result instanceof Locatable) {
			Locatable loc = (Locatable) result;
			processedObjs.put(obj, loc);
			String processedObjId = (String) compiledMappingClass.getField("_uid_value").get(obj);
			processedObjIds.put(obj, processedObjId);
			Map<String, Object> values = new HashMap<>();
			mappingClass.getFields().forEach(f -> {
				try {
					if (f.getType().startsWith("List<")) {
						PersistentBag pb = (PersistentBag) compiledMappingClass.getField(f.getName()).get(obj);
						if (pb.wasInitialized()) {
							List<Locatable> clusterItems = new ArrayList<>();
							for (int i = 0; i < pb.size(); i++) {
								Object fieldObj = pb.get(i);
								if (!processedObjs.keySet().contains(fieldObj)) {
									Locatable fieldLoc = (Locatable) this.createArchetypeClassObject(fieldObj, processedObjs, processedObjIds);
									if (fieldLoc != null) {
				                		loc.getAssociatedObjects().put(fieldLoc.getUid().getValue(), fieldLoc);
				                		Element e = new Element("", fieldLoc.getUid().getValue(), null);
				                		clusterItems.add(e);								
									}
								} else {
									String fieldLocId = processedObjIds.get(fieldObj);
									Object fieldLoc = processedObjs.get(fieldObj);
			                		loc.getAssociatedObjects().put(fieldLocId, fieldLoc);
			                		Element e = new Element("", fieldLocId, null);
			                		clusterItems.add(e);								
								}
							}
							values.put(f.getArchetypePath(), clusterItems);
						}
					} else {
		                JavaClass fieldMappingClass = Archetype2Java.INSTANCE.getMappingClassFromMappingClassName(f.getType());
		                if (fieldMappingClass != null) {
		                	Object fieldObj = compiledMappingClass.getField(f.getName()).get(obj);
		                	if (!processedObjs.keySet().contains(fieldObj)) {
			                	Locatable fieldLoc = (Locatable) this.createArchetypeClassObject(fieldObj, processedObjs, processedObjIds);
			                	if (fieldLoc != null) {
			                		values.put(f.getArchetypePath(), fieldLoc.getUid().getValue());
			                		loc.getAssociatedObjects().put(fieldLoc.getUid().getValue(), fieldLoc);
								}
							} else {
								String fieldLocId = processedObjIds.get(fieldObj);
								Object fieldLoc = processedObjs.get(fieldObj);
		                		values.put(f.getArchetypePath(), fieldLocId);
		                		loc.getAssociatedObjects().put(fieldLocId, fieldLoc);								
							}
						} else {
							values.put(f.getArchetypePath(), compiledMappingClass.getField(f.getName()).get(obj));
						}						
					}
				} catch (Exception e) {
					logger.error("createArchetypeClassObject", e);
				}
			});
			ArchetypeManipulator.INSTANCE.setArchetypeValues(loc, values, mappingClass.getArchetype());
			return loc;
		}
		return null;
	}
	
	public String Aql2Hql(String aql) {
		List<String> aqlSegments = new ArrayList<>();
		while (aql.indexOf("'") > 0) {
			int start = aql.indexOf("'");
			int end = aql.indexOf("'", start + 1);
			aqlSegments.add(aql.substring(0, start - 1));
			aqlSegments.add(aql.substring(start, end + 1));
			aql = aql.substring(end + 1);
		}
		aqlSegments.add(aql);
		
        
		String hql = aqlSegments.stream().map((aqlSegement) -> {
            if (!aqlSegement.startsWith("'")) {
                aqlSegement = Archetype2Java.INSTANCE.getClassNameFromArchetypeId(aqlSegement);
                aqlSegement = Archetype2Java.INSTANCE.getAttributeNameFromArchetypePath(aqlSegement);
                aqlSegement = aqlSegement.replaceAll("#", ".");
            }
            return aqlSegement;
        }).reduce("", String::concat);
        
		return hql;
	}

	private Getter getter(Class<? extends Object> clazz, String name) 
			throws MappingException {
		try {
			return BASIC_PROPERTY_ACCESSOR.getGetter( clazz, name );
		}
		catch ( PropertyNotFoundException pnfe ) {
			return DIRECT_PROPERTY_ACCESSOR.getGetter( clazz, name );
		}
	}

}
