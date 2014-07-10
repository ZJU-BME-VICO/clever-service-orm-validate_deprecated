package edu.zju.bme.clever.service;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.openehr.am.archetype.Archetype;
import org.openehr.rm.binding.DADLBinding;
import org.openehr.rm.common.archetyped.Locatable;
import org.openehr.rm.util.GenerationStrategy;
import org.openehr.rm.util.SkeletonGenerator;

import com.google.common.collect.ImmutableList;
import com.google.common.io.Files;

import edu.zju.bme.clever.service.util.ArchetypeManipulator;
import se.acode.openehr.parser.ADLParser;

public class CleverServiceTestBase {
	protected Map<String, String> archetypes = new LinkedHashMap<>();
	protected Map<String, String> arms = new LinkedHashMap<>();
	protected CleverServiceImpl cleverImpl = new CleverServiceImpl();
	protected CleverServiceParameterizedImpl aqlParameterizedImpl = new CleverServiceParameterizedImpl();

	public CleverServiceTestBase() throws IOException {
//		archetypes
//				.put("openEHR-EHR-OBSERVATION.blood_pressure.v1",
//						readLines("../document/knowledge/CKM/archetype/entry/observation/openEHR-EHR-OBSERVATION.blood_pressure.v1.adl"));
//		arms.put(
//				"openEHR-EHR-OBSERVATION.blood_pressure.v1",
//				readLines("../document/knowledge/CKM/archetype/entry/observation/openEHR-EHR-OBSERVATION.blood_pressure.v1.arm"));

		archetypes
				.put("openEHR-EHR-OBSERVATION.adl.v1",
						readLines("../document/knowledge/ZJU/archetype/ad/openEHR-EHR-OBSERVATION.adl.v1.adl"));
		arms.put(
				"openEHR-EHR-OBSERVATION.adl.v1",
				readLines("../document/knowledge/ZJU/archetype/ad/openEHR-EHR-OBSERVATION.adl.v1.arm"));

		archetypes
				.put("openEHR-EHR-OBSERVATION.cdr.v1",
						readLines("../document/knowledge/ZJU/archetype/ad/openEHR-EHR-OBSERVATION.cdr.v1.adl"));
		arms.put(
				"openEHR-EHR-OBSERVATION.cdr.v1",
				readLines("../document/knowledge/ZJU/archetype/ad/openEHR-EHR-OBSERVATION.cdr.v1.arm"));

		archetypes
				.put("openEHR-EHR-OBSERVATION.gds.v1",
						readLines("../document/knowledge/ZJU/archetype/ad/openEHR-EHR-OBSERVATION.gds.v1.adl"));
		arms.put(
				"openEHR-EHR-OBSERVATION.gds.v1",
				readLines("../document/knowledge/ZJU/archetype/ad/openEHR-EHR-OBSERVATION.gds.v1.arm"));

		archetypes
				.put("openEHR-EHR-OBSERVATION.mmse.v1",
						readLines("../document/knowledge/ZJU/archetype/ad/openEHR-EHR-OBSERVATION.mmse.v1.adl"));
		arms.put(
				"openEHR-EHR-OBSERVATION.mmse.v1",
				readLines("../document/knowledge/ZJU/archetype/ad/openEHR-EHR-OBSERVATION.mmse.v1.arm"));

		archetypes
				.put("openEHR-EHR-OBSERVATION.other_cognitions_scale_exams.v1",
						readLines("../document/knowledge/ZJU/archetype/ad/openEHR-EHR-OBSERVATION.other_cognitions_scale_exams.v1.adl"));
		arms.put(
				"openEHR-EHR-OBSERVATION.other_cognitions_scale_exams.v1",
				readLines("../document/knowledge/ZJU/archetype/ad/openEHR-EHR-OBSERVATION.other_cognitions_scale_exams.v1.arm"));

		archetypes
				.put("openEHR-EHR-COMPOSITION.visit.v3",
						readLines("../document/knowledge/ZJU/archetype/openEHR-EHR-COMPOSITION.visit.v3.adl"));
		arms.put(
				"openEHR-EHR-COMPOSITION.visit.v3",
				readLines("../document/knowledge/ZJU/archetype/openEHR-EHR-COMPOSITION.visit.v3.arm"));

		archetypes
				.put("openEHR-DEMOGRAPHIC-PERSON.patient.v1",
						readLines("../document/knowledge/ZJU/archetype/openEHR-DEMOGRAPHIC-PERSON.patient.v1.adl"));
		arms.put(
				"openEHR-DEMOGRAPHIC-PERSON.patient.v1",
				readLines("../document/knowledge/ZJU/archetype/openEHR-DEMOGRAPHIC-PERSON.patient.v1.arm"));

		archetypes
				.put("openEHR-DEMOGRAPHIC-PERSON.patient.v2",
						readLines("../document/knowledge/ZJU/archetype/openEHR-DEMOGRAPHIC-PERSON.patient.v2.adl"));
		arms.put(
				"openEHR-DEMOGRAPHIC-PERSON.patient.v2",
				readLines("../document/knowledge/ZJU/archetype/openEHR-DEMOGRAPHIC-PERSON.patient.v2.arm"));
	}

	protected String[] getDadlFiles() {
		return new String[] {
				"../document/knowledge/CKM/archetype/entry/observation/openEHR-EHR-OBSERVATION.blood_pressure.v1.1.dadl",
				"../document/knowledge/CKM/archetype/entry/observation/openEHR-EHR-OBSERVATION.blood_pressure.v1.2.dadl", 
				};
	}

	protected List<Map<HashMap<String, Object>, String>> getArchetypeValues() {

		Map<HashMap<String, Object>, String> visits = new HashMap<>();

		{
			HashMap<String, Object> visit1 = new HashMap<>();
			visit1.put("/uid/value", "visit1");
			visit1.put(
					"/context/other_context[at0001]/items[at0007]/value/value",
					"2010-01-15T19:20:30+08:00");
			visit1.put(
					"/context/other_context[at0001]/items[at0015]/value/value",
					"patient1");
			visit1.put(
					"/context/other_context[at0001]/items[at0016]/value/magnitude",
					null);
			visits.put(visit1, "openEHR-EHR-COMPOSITION.visit.v3");
		}

		{
			HashMap<String, Object> visit2 = new HashMap<>();
			visit2.put("/uid/value", "visit2");
			visit2.put(
					"/context/other_context[at0001]/items[at0007]/value/value",
					"2010-01-25T19:20:30+08:00");
			visit2.put(
					"/context/other_context[at0001]/items[at0015]/value/value",
					"patient1");
			visits.put(visit2, "openEHR-EHR-COMPOSITION.visit.v3");
		}

		{
			HashMap<String, Object> visit3 = new HashMap<>();
			visit3.put("/uid/value", "visit3");
			visit3.put(
					"/context/other_context[at0001]/items[at0007]/value/value",
					"2011-02-05T19:20:30+08:00");
			visit3.put(
					"/context/other_context[at0001]/items[at0015]/value/value",
					"patient2");
			visits.put(visit3, "openEHR-EHR-COMPOSITION.visit.v3");
		}
		
		Map<HashMap<String, Object>, String> patients = new HashMap<>();

		{
			HashMap<String, Object> patient1 = new HashMap<>();
			patient1.put("/uid/value", "patient1");
			patient1.put("/details[at0001]/items[at0003]/value/value", "M");
			patient1.put("/details[at0001]/items[at0004]/value/value",
					"1984-08-11T19:20:30+08:00");
			patient1.put("/details[at0001]/items[at0009]/value/value",
					"zhangsan");
			patients.put(patient1, "openEHR-DEMOGRAPHIC-PERSON.patient.v1");
		}

		{
			HashMap<String, Object> patient2 = new HashMap<>();
			patient2.put("/uid/value", "patient2");
			patient2.put("/details[at0001]/items[at0003]/value/value", "F");
			patient2.put("/details[at0001]/items[at0004]/value/value",
					"1986-08-11T19:20:30+08:00");
			patient2.put("/details[at0001]/items[at0009]/value/value", "lisi");
			patients.put(patient2, "openEHR-DEMOGRAPHIC-PERSON.patient.v1");
		}

		{
			HashMap<String, Object> patient3 = new HashMap<>();
			patient3.put("/uid/value", "patient3");
			patient3.put("/details[at0001]/items[at0003]/value/value", "O");
			patient3.put("/details[at0001]/items[at0004]/value/value",
					"1988-08-11T19:20:30+08:00");
			patient3.put("/details[at0001]/items[at0009]/value/value", "wangwu");
			patients.put(patient3, "openEHR-DEMOGRAPHIC-PERSON.patient.v1");
		}

		Map<HashMap<String, Object>, String> others = new HashMap<>();

		{
			HashMap<String, Object> other_cognitions_scale_exams1 = new HashMap<>();
			other_cognitions_scale_exams1.put("/uid/value",
					"other_cognitions_scale_exams1");
			other_cognitions_scale_exams1
					.put("/data[at0001]/events[at0002]/data[at0003]/items[at0004]/items[at0005]/value/magnitude",
							1);
			other_cognitions_scale_exams1
					.put("/data[at0001]/events[at0002]/data[at0003]/items[at0004]/items[at0006]/value/magnitude",
							2);
			other_cognitions_scale_exams1
					.put("/data[at0001]/events[at0002]/data[at0003]/items[at0004]/items[at0007]/value/magnitude",
							3);
			other_cognitions_scale_exams1
					.put("/data[at0001]/events[at0002]/data[at0003]/items[at0004]/items[at0096]/value/magnitude",
							6);
			others.put(other_cognitions_scale_exams1,
					"openEHR-EHR-OBSERVATION.other_cognitions_scale_exams.v1");
		}

		{
			HashMap<String, Object> mmse1 = new HashMap<>();
			mmse1.put("/uid/value", "mmse1");
			mmse1.put(
					"/data[at0001]/events[at0002]/data[at0003]/items[at0004]/items[at0005]/value/value",
					false);
			mmse1.put(
					"/data[at0001]/events[at0002]/data[at0003]/items[at0004]/items[at0009]/value/value",
					false);
			mmse1.put(
					"/data[at0001]/events[at0002]/data[at0003]/items[at0004]/items[at0012]/value/value",
					false);
			others.put(mmse1, "openEHR-EHR-OBSERVATION.mmse.v1");
		}

		List<Map<HashMap<String, Object>, String>> results = new ArrayList<>();
		results.add(patients);
		results.add(visits);
		results.add(others);

		return results;
		
	}

	protected void createTestBaseData() throws Exception {
		DADLBinding binding = new DADLBinding();

		List<Map<HashMap<String, Object>, String>> list = getArchetypeValues();
		List<String> dadls = new ArrayList<>();
		for (Map<HashMap<String, Object>, String> archetypeValues : list) {

			for (HashMap<String, Object> values : archetypeValues.keySet()) {
				String archetypeId = archetypeValues.get(values);
				SkeletonGenerator generator = SkeletonGenerator.getInstance();
				String archetypeString = archetypes.get(archetypeId);
				ADLParser parser = new ADLParser(archetypeString);
				Archetype archetype = parser.parse();
				Object result = generator.create(archetype, GenerationStrategy.MAXIMUM_EMPTY);
				if (result instanceof Locatable) {
					Locatable loc = (Locatable) result;
					ArchetypeManipulator.INSTANCE.setArchetypeValues(loc, values, archetype);
					dadls.add(binding.toDADLString(loc));
				}
			}
		}
		cleverImpl.insert(dadls);
	}

	protected void cleanTestBaseData() {
        archetypes.keySet().stream().map((str) -> String.format("delete from %s as o", str)).forEach((aql) -> {
            aqlParameterizedImpl.delete(aql, null);
        });
	}

	protected static String readLines(String name) throws IOException {
		File file = new File(name);
		ImmutableList<String> lines = Files.asCharSource(file, StandardCharsets.UTF_8).readLines();
		return String.join("\n", lines);
	}

	protected void reconfigure() throws IOException {
		cleverImpl.stop();

		cleverImpl.reconfigure(archetypes.values(), arms.values());
		
		cleverImpl.start();
	}

}
