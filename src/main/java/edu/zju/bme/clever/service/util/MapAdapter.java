package edu.zju.bme.clever.service.util;

import java.util.HashMap;
import java.util.Map;
 
import javax.xml.bind.annotation.adapters.XmlAdapter;
 
public class MapAdapter extends XmlAdapter<MapConvertor, Map<String, Object>>
{
 
    @Override
    public MapConvertor marshal(Map<String, Object> map) throws Exception
    {
        MapConvertor convertor = new MapConvertor();
        map.entrySet().stream().map((entry) -> new MapConvertor.MapEntry(entry)).forEach((e) -> {
            convertor.addEntry(e);
        });
        return convertor;
    }
 
    @Override
    public Map<String, Object> unmarshal(MapConvertor map) throws Exception
    {
        Map<String, Object> result = new HashMap<>();
        map.getEntries().stream().forEach((e) -> {
            result.put(e.getKey(), e.getValue());
        });
        return result;
    }
}