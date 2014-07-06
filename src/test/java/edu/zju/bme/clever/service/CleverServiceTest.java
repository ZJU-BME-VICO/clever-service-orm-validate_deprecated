package edu.zju.bme.clever.service;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.junit.Test;
import org.openehr.am.archetype.Archetype;
import org.openehr.am.parser.ContentObject;
import org.openehr.am.parser.DADLParser;
import org.openehr.am.serialize.XMLSerializer;
import org.openehr.rm.binding.DADLBinding;
import org.openehr.rm.common.archetyped.Locatable;
import se.acode.openehr.parser.ADLParser;

public class CleverServiceTest extends CleverServiceTestBase {

	public CleverServiceTest() throws IOException {
		super();
	}

	@Test
	public void testDelete() throws Exception {
		reconfigure();

		cleanTestBaseData();
		createTestBaseData();

		{
			String query = "from openEHR-EHR-COMPOSITION.visit.v3 as o ";
			List<String> results = cleverImpl.select(query);

			assertEquals(results.size(), 5);
		}

		{
			String query = "delete "
					+ "from openEHR-EHR-COMPOSITION.visit.v3 as o "
					+ "where o#/uid/value = 'visit3'";
			int ret = cleverImpl.delete(query);

			assertEquals(ret, 1);
		}

		{
			String query = "from openEHR-EHR-COMPOSITION.visit.v3 as o ";
			List<String> results = cleverImpl.select(query);

			assertEquals(results.size(), 3);
		}

		{
			String query = "delete "
					+ "from openEHR-EHR-COMPOSITION.visit.v3 as o ";
			int ret = cleverImpl.delete(query);

			assertEquals(ret, 2);
		}

		{
			String query = "from openEHR-EHR-COMPOSITION.visit.v3 as o ";
			List<String> results = cleverImpl.select(query);

			assertEquals(results.size(), 0);
		}

		cleanTestBaseData();
	}

	@Test
	public void testDeleteParameterized() throws Exception {
		reconfigure();

		cleanTestBaseData();
		createTestBaseData();

		{
			String query = "from openEHR-EHR-COMPOSITION.visit.v3 as o ";
			List<String> results = cleverImpl.select(query);

			assertEquals(results.size(), 5);
		}

		{
			String query = "delete "
					+ "from openEHR-EHR-COMPOSITION.visit.v3 as o "
					+ "where o#/uid/value = :name";
			Map<String, Object> parameters = new HashMap<>();
			parameters.put("name", "visit3");
			int ret = aqlParameterizedImpl.delete(query, parameters);

			assertEquals(ret, 1);
		}

		{
			String query = "from openEHR-EHR-COMPOSITION.visit.v3 as o ";
			List<String> results = cleverImpl.select(query);

			assertEquals(results.size(), 3);
		}

		{
			String query = "delete "
					+ "from openEHR-EHR-COMPOSITION.visit.v3 as o ";
			int ret = cleverImpl.delete(query);

			assertEquals(ret, 2);
		}

		{
			String query = "from openEHR-EHR-COMPOSITION.visit.v3 as o ";
			List<String> results = cleverImpl.select(query);

			assertEquals(results.size(), 0);
		}

		cleanTestBaseData();
	}

	@Test
	public void testGetSQL() throws Exception {
		reconfigure();

		cleanTestBaseData();
		createTestBaseData();

		{
			String query = "from openEHR-EHR-COMPOSITION.visit.v3 as o ";
			List<String> sqls = cleverImpl.getSQL(query);

			assertTrue(sqls.size() > 0);
            sqls.stream().forEach((sql) -> {
                System.out.println(sql);
            });
		}

		{
			String query = "select o "
					+ "from openEHR-DEMOGRAPHIC-PERSON.patient.v1 as o "
					+ "where o#/details[at0001]/items[at0009]/value/value = 'lisi'";
			List<String> sqls = cleverImpl.getSQL(query);

			assertTrue(sqls.size() > 0);
            sqls.stream().forEach((sql) -> {
                System.out.println(sql);
            });
		}

		{
			String query = "update openEHR-DEMOGRAPHIC-PERSON.patient.v1 as o set "
					+ "o#/details[at0001]/items[at0009]/value/value = 'lisi', "
					+ "o#/details[at0001]/items[at0004]/value/value = '1994-08-11T19:20:30+08:00' "
					+ "where o#/uid/value = 'patient1'";
			List<String> sqls = cleverImpl.getSQL(query);

			assertTrue(sqls.size() > 0);
            sqls.stream().forEach((sql) -> {
                System.out.println(sql);
            });
		}

		{
			String query = "select o "
					+ "from openEHR-DEMOGRAPHIC-PERSON.patient.v1 as o "
					+ "where o#/details[at0001]/items[at0009]/value/value = :name";
			List<String> sqls = cleverImpl.getSQL(query);

			assertTrue(sqls.size() > 0);
            sqls.stream().forEach((sql) -> {
                System.out.println(sql);
            });
		}

		cleanTestBaseData();
	}

	@Test
	public void testGetArchetypeIds() throws Exception {
		reconfigure();

		Set<String> archetypeIds = cleverImpl.getArchetypeIds();
		System.out.println(archetypeIds);
		System.out.println(archetypes.keySet());
		assertTrue(archetypeIds.containsAll(archetypes.keySet()));
		assertTrue(archetypes.keySet().containsAll(archetypeIds));
	}

	@Test
	public void testSelect() throws Exception {
		reconfigure();

		cleanTestBaseData();
		createTestBaseData();

		DADLBinding binding = new DADLBinding();

		{
			String query = "select o "
					+ "from openEHR-DEMOGRAPHIC-PERSON.patient.v1 as o "
					+ "order by o#/uid/value asc";
			List<String> results = cleverImpl.select(query);

			assertEquals(results.size(), 3);

			DADLParser parser1 = new DADLParser(results.get(0));
			ContentObject contentObj1 = parser1.parse();
			Locatable loc1 = (Locatable) binding.bind(contentObj1);
			String d1 = (String) loc1.itemAtPath("/uid/value");
			String d2 = (String) loc1
					.itemAtPath("/details[at0001]/items[at0003]/value/value");
			String d3 = (String) loc1
					.itemAtPath("/details[at0001]/items[at0004]/value/value");
			String d4 = (String) loc1
					.itemAtPath("/details[at0001]/items[at0009]/value/value");
			assertEquals(d1, "patient1");
			assertEquals(d2, "M");
			assertEquals(d3, "1984-08-11T19:20:30+08:00");
			assertEquals(d4, "zhangsan");

			DADLParser parser2 = new DADLParser(results.get(1));
			ContentObject contentObj2 = parser2.parse();
			Locatable loc2 = (Locatable) binding.bind(contentObj2);
			String d5 = (String) loc2.itemAtPath("/uid/value");
			String d6 = (String) loc2
					.itemAtPath("/details[at0001]/items[at0003]/value/value");
			String d7 = (String) loc2
					.itemAtPath("/details[at0001]/items[at0004]/value/value");
			String d8 = (String) loc2
					.itemAtPath("/details[at0001]/items[at0009]/value/value");
			assertEquals(d5, "patient2");
			assertEquals(d6, "F");
			assertEquals(d7, "1986-08-11T19:20:30+08:00");
			assertEquals(d8, "lisi");

			DADLParser parser3 = new DADLParser(results.get(2));
			ContentObject contentObj3 = parser3.parse();
			Locatable loc3 = (Locatable) binding.bind(contentObj3);
			String d9 = (String) loc3.itemAtPath("/uid/value");
			String d10 = (String) loc3
					.itemAtPath("/details[at0001]/items[at0003]/value/value");
			String d11 = (String) loc3
					.itemAtPath("/details[at0001]/items[at0004]/value/value");
			String d12 = (String) loc3
					.itemAtPath("/details[at0001]/items[at0009]/value/value");
			assertEquals(d9, "patient3");
			assertEquals(d10, "O");
			assertEquals(d11, "1988-08-11T19:20:30+08:00");
			assertEquals(d12, "wangwu");
		}

		{
			String query = "select o "
					+ "from openEHR-DEMOGRAPHIC-PERSON.patient.v1 as o "
					+ "where o#/details[at0001]/items[at0009]/value/value = 'lisi'";
			List<String> results = cleverImpl.select(query);

			assertEquals(results.size(), 1);

			DADLParser parser2 = new DADLParser(results.get(0));
			ContentObject contentObj2 = parser2.parse();
			Locatable loc2 = (Locatable) binding.bind(contentObj2);
			String d5 = (String) loc2.itemAtPath("/uid/value");
			String d6 = (String) loc2
					.itemAtPath("/details[at0001]/items[at0003]/value/value");
			String d7 = (String) loc2
					.itemAtPath("/details[at0001]/items[at0004]/value/value");
			String d8 = (String) loc2
					.itemAtPath("/details[at0001]/items[at0009]/value/value");
			assertEquals(d5, "patient2");
			assertEquals(d6, "F");
			assertEquals(d7, "1986-08-11T19:20:30+08:00");
			assertEquals(d8, "lisi");
		}

		{
			String query = "select o "
					+ "from openEHR-DEMOGRAPHIC-PERSON.patient.v1 as o "
					+ "where o#/uid/value = 'patient1'";
			List<String> results = cleverImpl.select(query);

			assertEquals(results.size(), 1);

			DADLParser parser1 = new DADLParser(results.get(0));
			ContentObject contentObj1 = parser1.parse();
			Locatable loc1 = (Locatable) binding.bind(contentObj1);
			String d1 = (String) loc1.itemAtPath("/uid/value");
			String d2 = (String) loc1
					.itemAtPath("/details[at0001]/items[at0003]/value/value");
			String d3 = (String) loc1
					.itemAtPath("/details[at0001]/items[at0004]/value/value");
			String d4 = (String) loc1
					.itemAtPath("/details[at0001]/items[at0009]/value/value");
			assertEquals(d1, "patient1");
			assertEquals(d2, "M");
			assertEquals(d3, "1984-08-11T19:20:30+08:00");
			assertEquals(d4, "zhangsan");
		}

		{
			String query = "select o "
					+ "from openEHR-EHR-COMPOSITION.visit.v3 as o "
					+ "where o#/uid/value = 'visit1'";
			List<String> results = cleverImpl.select(query);

			assertEquals(results.size(), 2);
		}

		cleanTestBaseData();
	}

//	@Test
//	public void testSelectColumn() throws Exception {
//		reconfigure();
//
//		cleanTestBaseData();
//		createTestBaseData();
//
//		{
//			String query = "select "
//					+ "o#/uid/value, "
//					+ "o#/details[at0001]/items[at0003]/value/value, "
//					+ "o#/details[at0001]/items[at0004]/value/value, "
//					+ "o#/details[at0001]/items[at0009]/value/value "
//					+ "from openEHR-DEMOGRAPHIC-PERSON.patient.v1 as o "
//					+ "order by o#/uid/value asc";
//			List results = aqlImpl.select(query, null, null);
//
//			assertEquals(results.size(), 3);
//			Object[] loc1 = (Object[]) results.get(0);
//			assertEquals(loc1[0], "patient1");
//			assertEquals(loc1[1], "M");
//			assertEquals(loc1[2], "1984-08-11T19:20:30+08:00");
//			assertEquals(loc1[3], "zhangsan");
//			Object[] loc2 = (Object[]) results.get(1);
//			assertEquals(loc2[0], "patient2");
//			assertEquals(loc2[1], "F");
//			assertEquals(loc2[2], "1986-08-11T19:20:30+08:00");
//			assertEquals(loc2[3], "lisi");
//			Object[] loc3 = (Object[]) results.get(2);
//			assertEquals(loc3[0], "patient3");
//			assertEquals(loc3[1], "O");
//			assertEquals(loc3[2], "1988-08-11T19:20:30+08:00");
//			assertEquals(loc3[3], "wangwu");
//		}
//
//		{
//			String query = "select "
//					+ "o#/uid/value, "
//					+ "o#/details[at0001]/items[at0003]/value/value, "
//					+ "o#/details[at0001]/items[at0004]/value/value, "
//					+ "o#/details[at0001]/items[at0009]/value/value "
//					+ "from openEHR-DEMOGRAPHIC-PERSON.patient.v1 as o "
//					+ "where o#/details[at0001]/items[at0009]/value/value = :name";
//			Map<String, Object> parameters = new HashMap<String, Object>();
//			parameters.put("name", "lisi");
//			List results = aqlImpl.select(query, null, parameters);
//
//			assertEquals(results.size(), 1);
//			Object[] loc2 = (Object[]) results.get(0);
//			assertEquals(loc2[0], "patient2");
//			assertEquals(loc2[1], "F");
//			assertEquals(loc2[2], "1986-08-11T19:20:30+08:00");
//			assertEquals(loc2[3], "lisi");
//		}
//
//		{
//			String query = "select "
//					+ "o#/uid/value, "
//					+ "o#/details[at0001]/items[at0003]/value/value, "
//					+ "o#/details[at0001]/items[at0004]/value/value, "
//					+ "o#/details[at0001]/items[at0009]/value/value "
//					+ "from openEHR-DEMOGRAPHIC-PERSON.patient.v1 as o "
//					+ "where o#/uid/value = :name";
//			Map<String, Object> parameters = new HashMap<String, Object>();
//			parameters.put("name", "patient1");
//			List results = aqlImpl.select(query, null, parameters);
//
//			assertEquals(results.size(), 1);
//			Object[] loc1 = (Object[]) results.get(0);
//			assertEquals(loc1[0], "patient1");
//			assertEquals(loc1[1], "M");
//			assertEquals(loc1[2], "1984-08-11T19:20:30+08:00");
//			assertEquals(loc1[3], "zhangsan");
//		}
//
//		cleanTestBaseData();
//	}

	@Test
	public void testSelectJoinCartesian() throws Exception {
		reconfigure();

		cleanTestBaseData();
		createTestBaseData();

		DADLBinding binding = new DADLBinding();

		{
			String query = "select p, v " +
					"from openEHR-DEMOGRAPHIC-PERSON.patient.v1 as p, openEHR-EHR-COMPOSITION.visit.v3 as v ";
			List<String> results = cleverImpl.select(query);

			assertEquals(results.size(), 6);

			List<String> patients = new ArrayList<>();
			List<String> visits = new ArrayList<>();
			for (String arr : results) {
				DADLParser parser = new DADLParser(arr);
				ContentObject contentObj = parser.parse();
				Locatable loc = (Locatable) binding.bind(contentObj);			
				if (loc.getArchetypeNodeId().compareToIgnoreCase("openEHR-DEMOGRAPHIC-PERSON.patient.v1") == 0) {
					patients.add(arr);	
					assertEquals(binding.toDADLString(loc), arr);
				}
				
				if (loc.getArchetypeNodeId().compareToIgnoreCase("openEHR-EHR-COMPOSITION.visit.v3") == 0) {
					visits.add(arr);	
					assertEquals(binding.toDADLString(loc), arr);
				}
			}

			assertEquals(patients.size(), 3);
			assertEquals(visits.size(), 3);
		}

		cleanTestBaseData();
	}

	@Test
	public void testSelectJoinFetchManyToOne() throws Exception {
		reconfigure();

		cleanTestBaseData();
		createTestBaseData();

		DADLBinding binding = new DADLBinding();

		{
			String query = "select v " +
					"from openEHR-EHR-COMPOSITION.visit.v3 as v " +
					"join fetch v#/context/other_context[at0001]/items[at0015]/value/value as p ";
			List<String> results = cleverImpl.select(query);

			assertEquals(results.size(), 5);

			List<String> patients = new ArrayList<>();
			List<String> visits = new ArrayList<>();
			for (String arr : results) {
				DADLParser parser = new DADLParser(arr);
				ContentObject contentObj = parser.parse();
				Locatable loc = (Locatable) binding.bind(contentObj);			
				if (loc.getArchetypeNodeId().compareToIgnoreCase("openEHR-DEMOGRAPHIC-PERSON.patient.v1") == 0) {
					patients.add(arr);	
					assertEquals(binding.toDADLString(loc), arr);
				}
				
				if (loc.getArchetypeNodeId().compareToIgnoreCase("openEHR-EHR-COMPOSITION.visit.v3") == 0) {
					visits.add(arr);	
					assertEquals(binding.toDADLString(loc), arr);
				}
			}

			assertEquals(patients.size(), 2);
			assertEquals(visits.size(), 3);
		}

		cleanTestBaseData();
	}

	@Test
	public void testSelectJoinFetchOneToMany() throws Exception {
		reconfigure();

		cleanTestBaseData();
		createTestBaseData();

		DADLBinding binding = new DADLBinding();

		{
			String query = "select p " +
					"from openEHR-DEMOGRAPHIC-PERSON.patient.v1 as p " +
					"join fetch p#/details[at0001]/items[at0032]/items as v ";
			List<String> results = cleverImpl.select(query);

			assertEquals(results.size(), 5);

			List<String> patients = new ArrayList<>();
			List<String> visits = new ArrayList<>();
			for (String arr : results) {
				DADLParser parser = new DADLParser(arr);
				ContentObject contentObj = parser.parse();
				Locatable loc = (Locatable) binding.bind(contentObj);			
				if (loc.getArchetypeNodeId().compareToIgnoreCase("openEHR-DEMOGRAPHIC-PERSON.patient.v1") == 0) {
					patients.add(arr);
					assertEquals(binding.toDADLString(loc), arr);
				}
				
				if (loc.getArchetypeNodeId().compareToIgnoreCase("openEHR-EHR-COMPOSITION.visit.v3") == 0) {
					visits.add(arr);	
					assertEquals(binding.toDADLString(loc), arr);
				}
			}

			assertEquals(patients.size(), 2);
			assertEquals(visits.size(), 3);
		}

		{
			String query = "select distinct p " +
					"from openEHR-DEMOGRAPHIC-PERSON.patient.v1 as p " +
					"join fetch p#/details[at0001]/items[at0032]/items as v ";
			List<String> results = cleverImpl.select(query);

			assertEquals(results.size(), 5);

			List<String> patients = new ArrayList<>();
			List<String> visits = new ArrayList<>();
			for (String arr : results) {
				DADLParser parser = new DADLParser(arr);
				ContentObject contentObj = parser.parse();
				Locatable loc = (Locatable) binding.bind(contentObj);			
				if (loc.getArchetypeNodeId().compareToIgnoreCase("openEHR-DEMOGRAPHIC-PERSON.patient.v1") == 0) {
					patients.add(arr);	
					assertEquals(binding.toDADLString(loc), arr);
				}
				
				if (loc.getArchetypeNodeId().compareToIgnoreCase("openEHR-EHR-COMPOSITION.visit.v3") == 0) {
					visits.add(arr);	
					assertEquals(binding.toDADLString(loc), arr);
				}
			}

			assertEquals(patients.size(), 2);
			assertEquals(visits.size(), 3);
		}

		cleanTestBaseData();
	}

	@Test
	public void testSelectJoinManyToOne() throws Exception {
		reconfigure();

		cleanTestBaseData();
		createTestBaseData();

		DADLBinding binding = new DADLBinding();

		{
			String query = "select v " +
					"from openEHR-EHR-COMPOSITION.visit.v3 as v " +
					"join v#/context/other_context[at0001]/items[at0015]/value/value as p ";
			List<String> results = cleverImpl.select(query);

			assertEquals(results.size(), 5);

			List<String> patients = new ArrayList<>();
			List<String> visits = new ArrayList<>();
			for (String arr : results) {
				DADLParser parser = new DADLParser(arr);
				ContentObject contentObj = parser.parse();
				Locatable loc = (Locatable) binding.bind(contentObj);			
				if (loc.getArchetypeNodeId().compareToIgnoreCase("openEHR-DEMOGRAPHIC-PERSON.patient.v1") == 0) {
					patients.add(arr);	
					assertEquals(binding.toDADLString(loc), arr);
				}
				
				if (loc.getArchetypeNodeId().compareToIgnoreCase("openEHR-EHR-COMPOSITION.visit.v3") == 0) {
					visits.add(arr);	
					assertEquals(binding.toDADLString(loc), arr);
				}
			}

			assertEquals(patients.size(), 2);
			assertEquals(visits.size(), 3);
		}

		{
			String query = "select p, v " +
					"from openEHR-EHR-COMPOSITION.visit.v3 as v " +
					"join v#/context/other_context[at0001]/items[at0015]/value/value as p ";
			List<String> results = cleverImpl.select(query);

			assertEquals(results.size(), 5);

			List<String> patients = new ArrayList<>();
			List<String> visits = new ArrayList<>();
			for (String arr : results) {
				DADLParser parser = new DADLParser(arr);
				ContentObject contentObj = parser.parse();
				Locatable loc = (Locatable) binding.bind(contentObj);			
				if (loc.getArchetypeNodeId().compareToIgnoreCase("openEHR-DEMOGRAPHIC-PERSON.patient.v1") == 0) {
					patients.add(arr);	
					assertEquals(binding.toDADLString(loc), arr);
				}
				
				if (loc.getArchetypeNodeId().compareToIgnoreCase("openEHR-EHR-COMPOSITION.visit.v3") == 0) {
					visits.add(arr);	
					assertEquals(binding.toDADLString(loc), arr);
				}
			}

			assertEquals(patients.size(), 2);
			assertEquals(visits.size(), 3);
		}

		cleanTestBaseData();
	}

	@Test
	public void testSelectJoinOneToMany() throws Exception {
		reconfigure();

		cleanTestBaseData();
		createTestBaseData();

		DADLBinding binding = new DADLBinding();

		{
			String query = "select p " +
					"from openEHR-DEMOGRAPHIC-PERSON.patient.v1 as p " +
					"join p#/details[at0001]/items[at0032]/items as v ";
			List<String> results = cleverImpl.select(query);

			assertEquals(results.size(), 2);

			List<String> patients = new ArrayList<>();
			List<String> visits = new ArrayList<>();
			for (String arr : results) {
				DADLParser parser = new DADLParser(arr);
				ContentObject contentObj = parser.parse();
				Locatable loc = (Locatable) binding.bind(contentObj);			
				if (loc.getArchetypeNodeId().compareToIgnoreCase("openEHR-DEMOGRAPHIC-PERSON.patient.v1") == 0) {
					patients.add(arr);	
					assertEquals(binding.toDADLString(loc), arr);
				}
				
				if (loc.getArchetypeNodeId().compareToIgnoreCase("openEHR-EHR-COMPOSITION.visit.v3") == 0) {
					visits.add(arr);	
					assertEquals(binding.toDADLString(loc), arr);
				}
			}

			assertEquals(patients.size(), 2);
			assertEquals(visits.size(), 0);
		}

		{
			String query = "select p, v " +
					"from openEHR-DEMOGRAPHIC-PERSON.patient.v1 as p " +
					"join p#/details[at0001]/items[at0032]/items as v ";
			List<String> results = cleverImpl.select(query);

			assertEquals(results.size(), 5);

			List<String> patients = new ArrayList<>();
			List<String> visits = new ArrayList<>();
			for (String arr : results) {
				DADLParser parser = new DADLParser(arr);
				ContentObject contentObj = parser.parse();
				Locatable loc = (Locatable) binding.bind(contentObj);			
				if (loc.getArchetypeNodeId().compareToIgnoreCase("openEHR-DEMOGRAPHIC-PERSON.patient.v1") == 0) {
					patients.add(arr);	
					assertEquals(binding.toDADLString(loc), arr);
				}
				
				if (loc.getArchetypeNodeId().compareToIgnoreCase("openEHR-EHR-COMPOSITION.visit.v3") == 0) {
					visits.add(arr);	
					assertEquals(binding.toDADLString(loc), arr);
				}
			}

			assertEquals(patients.size(), 2);
			assertEquals(visits.size(), 3);
		}

		{
			String query = "select p " +
					"from openEHR-DEMOGRAPHIC-PERSON.patient.v1 as p " +
					"left join p#/details[at0001]/items[at0032]/items as v ";
			List<String> results = cleverImpl.select(query);

			assertEquals(results.size(), 3);

			List<String> patients = new ArrayList<>();
			List<String> visits = new ArrayList<>();
			for (String arr : results) {
				DADLParser parser = new DADLParser(arr);
				ContentObject contentObj = parser.parse();
				Locatable loc = (Locatable) binding.bind(contentObj);			
				if (loc.getArchetypeNodeId().compareToIgnoreCase("openEHR-DEMOGRAPHIC-PERSON.patient.v1") == 0) {
					patients.add(arr);	
					assertEquals(binding.toDADLString(loc), arr);
				}
				
				if (loc.getArchetypeNodeId().compareToIgnoreCase("openEHR-EHR-COMPOSITION.visit.v3") == 0) {
					visits.add(arr);	
					assertEquals(binding.toDADLString(loc), arr);
				}
			}

			assertEquals(patients.size(), 3);
			assertEquals(visits.size(), 0);
		}

		{
			String query = "select p, v " +
					"from openEHR-DEMOGRAPHIC-PERSON.patient.v1 as p " +
					"left join p#/details[at0001]/items[at0032]/items as v ";
			List<String> results = cleverImpl.select(query);

			assertEquals(results.size(), 6);

			List<String> patients = new ArrayList<>();
			List<String> visits = new ArrayList<>();
			for (String arr : results) {
				DADLParser parser = new DADLParser(arr);
				ContentObject contentObj = parser.parse();
				Locatable loc = (Locatable) binding.bind(contentObj);			
				if (loc.getArchetypeNodeId().compareToIgnoreCase("openEHR-DEMOGRAPHIC-PERSON.patient.v1") == 0) {
					patients.add(arr);	
					assertEquals(binding.toDADLString(loc), arr);
				}
				
				if (loc.getArchetypeNodeId().compareToIgnoreCase("openEHR-EHR-COMPOSITION.visit.v3") == 0) {
					visits.add(arr);	
					assertEquals(binding.toDADLString(loc), arr);
				}
			}

			assertEquals(patients.size(), 3);
			assertEquals(visits.size(), 3);
		}

		cleanTestBaseData();
	}

	@Test
	public void testSelectParameterized() throws Exception {
		reconfigure();

		cleanTestBaseData();
		createTestBaseData();

		DADLBinding binding = new DADLBinding();

		{
			String query = "select o "
					+ "from openEHR-DEMOGRAPHIC-PERSON.patient.v1 as o "
					+ "where o#/details[at0001]/items[at0009]/value/value = :name";
			Map<String, Object> parameters = new HashMap<>();
			parameters.put("name", "lisi");
			List<String> results = aqlParameterizedImpl.select(query, parameters);

			assertEquals(results.size(), 1);

			DADLParser parser2 = new DADLParser(results.get(0));
			ContentObject contentObj2 = parser2.parse();
			Locatable loc2 = (Locatable) binding.bind(contentObj2);
			String d5 = (String) loc2.itemAtPath("/uid/value");
			String d6 = (String) loc2
					.itemAtPath("/details[at0001]/items[at0003]/value/value");
			String d7 = (String) loc2
					.itemAtPath("/details[at0001]/items[at0004]/value/value");
			String d8 = (String) loc2
					.itemAtPath("/details[at0001]/items[at0009]/value/value");
			assertEquals(d5, "patient2");
			assertEquals(d6, "F");
			assertEquals(d7, "1986-08-11T19:20:30+08:00");
			assertEquals(d8, "lisi");
		}

		{
			String query = "select o "
					+ "from openEHR-EHR-COMPOSITION.visit.v3 as o "
					+ "where o#/uid/value = :VisitId";
			Map<String, Object> parameters = new HashMap<>();
			parameters.put("VisitId", "visit1");
			List<String> results = aqlParameterizedImpl.select(query, parameters);

			assertEquals(results.size(), 2);
		}

		cleanTestBaseData();
	}

	@Test
	public void testSelectCount() throws Exception {
		reconfigure();

		cleanTestBaseData();
		createTestBaseData();

		{
			String query = "select count(*) "
					+ "from openEHR-DEMOGRAPHIC-PERSON.patient.v1 as o ";
			long count = cleverImpl.selectCount(query);

			assertEquals(count, 3);
		}

		{
			String query = "select count(*) "
					+ "from openEHR-DEMOGRAPHIC-PERSON.patient.v1 as o "
					+ "where o#/uid/value = 'patient1'";
			long count = cleverImpl.selectCount(query);

			assertEquals(count, 1);
		}

		{
			String query = "select count(*) "
					+ "from openEHR-DEMOGRAPHIC-PERSON.patient.v1 as o "
					+ "where o#/uid/value = 'patientXXX'";
			long count = cleverImpl.selectCount(query);

			assertEquals(count, 0);
		}

		cleanTestBaseData();
	}

	@Test
	public void testUpdate() throws Exception {
		reconfigure();

		cleanTestBaseData();
		createTestBaseData();

		DADLBinding binding = new DADLBinding();

		{
			String query = "select o "
					+ "from openEHR-DEMOGRAPHIC-PERSON.patient.v1 as o "
					+ "where o#/uid/value = 'patient1'";
			List<String> results = cleverImpl.select(query);

			assertEquals(results.size(), 1);

			DADLParser parser1 = new DADLParser(results.get(0));
			ContentObject contentObj1 = parser1.parse();
			Locatable loc1 = (Locatable) binding.bind(contentObj1);
			String d1 = (String) loc1.itemAtPath("/uid/value");
			String d2 = (String) loc1
					.itemAtPath("/details[at0001]/items[at0003]/value/value");
			String d3 = (String) loc1
					.itemAtPath("/details[at0001]/items[at0004]/value/value");
			String d4 = (String) loc1
					.itemAtPath("/details[at0001]/items[at0009]/value/value");
			assertEquals(d1, "patient1");
			assertEquals(d2, "M");
			assertEquals(d3, "1984-08-11T19:20:30+08:00");
			assertEquals(d4, "zhangsan");
		}

		{
			String query = "update openEHR-DEMOGRAPHIC-PERSON.patient.v1 as o set "
					+ "o#/details[at0001]/items[at0009]/value/value = 'lisi', "
					+ "o#/details[at0001]/items[at0004]/value/value = '1994-08-11T19:20:30+08:00' "
					+ "where o#/uid/value = 'patient1'";

			int ret = cleverImpl.update(query);

			assertEquals(ret, 1);
		}

		{
			String query = "select o "
					+ "from openEHR-DEMOGRAPHIC-PERSON.patient.v1 as o "
					+ "where o#/uid/value = 'patient1'";
			List<String> results = cleverImpl.select(query);

			assertEquals(results.size(), 1);

			DADLParser parser1 = new DADLParser(results.get(0));
			ContentObject contentObj1 = parser1.parse();
			Locatable loc1 = (Locatable) binding.bind(contentObj1);
			String d1 = (String) loc1.itemAtPath("/uid/value");
			String d2 = (String) loc1
					.itemAtPath("/details[at0001]/items[at0003]/value/value");
			String d3 = (String) loc1
					.itemAtPath("/details[at0001]/items[at0004]/value/value");
			String d4 = (String) loc1
					.itemAtPath("/details[at0001]/items[at0009]/value/value");
			assertEquals(d1, "patient1");
			assertEquals(d2, "M");
			assertEquals(d3, "1994-08-11T19:20:30+08:00");
			assertEquals(d4, "lisi");
		}

		cleanTestBaseData();
	}

	@Test
	public void testUpdateParameterized() throws Exception {
		reconfigure();

		cleanTestBaseData();
		createTestBaseData();

		DADLBinding binding = new DADLBinding();

		{
			String query = "select o "
					+ "from openEHR-DEMOGRAPHIC-PERSON.patient.v1 as o "
					+ "where o#/uid/value = 'patient1'";
			List<String> results = cleverImpl.select(query);

			assertEquals(results.size(), 1);

			DADLParser parser1 = new DADLParser(results.get(0));
			ContentObject contentObj1 = parser1.parse();
			Locatable loc1 = (Locatable) binding.bind(contentObj1);
			String d1 = (String) loc1.itemAtPath("/uid/value");
			String d2 = (String) loc1
					.itemAtPath("/details[at0001]/items[at0003]/value/value");
			String d3 = (String) loc1
					.itemAtPath("/details[at0001]/items[at0004]/value/value");
			String d4 = (String) loc1
					.itemAtPath("/details[at0001]/items[at0009]/value/value");
			assertEquals(d1, "patient1");
			assertEquals(d2, "M");
			assertEquals(d3, "1984-08-11T19:20:30+08:00");
			assertEquals(d4, "zhangsan");
		}

		{
			String query = "update openEHR-DEMOGRAPHIC-PERSON.patient.v1 as o set "
					+ "o#/details[at0001]/items[at0009]/value/value = :name, "
					+ "o#/details[at0001]/items[at0004]/value/value = :birthday "
					+ "where o#/uid/value = :pid";
			Map<String, Object> parameters = new HashMap<>();
			parameters.put("name", "lisi");
			parameters.put("birthday", "1994-08-11T19:20:30+08:00");
			parameters.put("pid", "patient1");
			int ret = aqlParameterizedImpl.update(query, parameters);

			assertEquals(ret, 1);
		}

		{
			String query = "select o "
					+ "from openEHR-DEMOGRAPHIC-PERSON.patient.v1 as o "
					+ "where o#/uid/value = 'patient1'";
			List<String> results = cleverImpl.select(query);

			assertEquals(results.size(), 1);

			DADLParser parser1 = new DADLParser(results.get(0));
			ContentObject contentObj1 = parser1.parse();
			Locatable loc1 = (Locatable) binding.bind(contentObj1);
			String d1 = (String) loc1.itemAtPath("/uid/value");
			String d2 = (String) loc1
					.itemAtPath("/details[at0001]/items[at0003]/value/value");
			String d3 = (String) loc1
					.itemAtPath("/details[at0001]/items[at0004]/value/value");
			String d4 = (String) loc1
					.itemAtPath("/details[at0001]/items[at0009]/value/value");
			assertEquals(d1, "patient1");
			assertEquals(d2, "M");
			assertEquals(d3, "1994-08-11T19:20:30+08:00");
			assertEquals(d4, "lisi");
		}

		cleanTestBaseData();
	}

	@Test
	public void testArchetypeAndXML() throws Exception {
		
		archetypes.forEach((id, archetypeString) -> {
			try {
				XMLSerializer xmlSerializer = new XMLSerializer();
				ADLParser parser = new ADLParser(archetypeString);
				Archetype archetype = parser.parse();
				String archetypeXML = xmlSerializer.output(archetype);
				System.out.println(archetypeXML);
			} catch (Exception e) {
				assertTrue(false);
			}			
		});
		
	}

}
