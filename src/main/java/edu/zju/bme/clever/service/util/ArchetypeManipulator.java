package edu.zju.bme.clever.service.util;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang.ClassUtils;
import org.hibernate.MappingException;
import org.hibernate.PropertyNotFoundException;
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
import org.openehr.rm.datatypes.quantity.datetime.DvDateTimeParser;
import org.openehr.rm.datatypes.text.CodePhrase;
import org.openehr.rm.support.measurement.MeasurementService;
import org.openehr.rm.support.measurement.SimpleMeasurementService;
import org.openehr.rm.support.terminology.TerminologyService;
import org.openehr.terminology.SimpleTerminologyService;

public enum ArchetypeManipulator {

	INSTANCE;
	
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
					throws InstantiationException, IllegalAccessException {
		for (String path : values.keySet()) {
			setArchetypeValue(loc, path, values.get(path), archetype);
		}
	}
	
	public void setArchetypeValue(
			Locatable loc, String propertyPath, Object propertyValue, Archetype archetype) 
					throws InstantiationException, IllegalAccessException {
		
		Map<String, CObject> pathNodeMap = archetype.getPathNodeMap();
		String nodePath = getArchetypeNodePath(archetype, propertyPath);
		if (nodePath.compareTo(propertyPath) == 0) {
			loc.set(nodePath, propertyValue);
		} else {
			CObject node = pathNodeMap.get(nodePath);
			Object target = loc.itemAtPath(nodePath);
			if (target == null) {
				Class<?> klass = rmBuilder.retrieveRMType(node.getRmTypeName());
				target = klass.newInstance();
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
							Set.class.isAssignableFrom(klass)) {						
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
