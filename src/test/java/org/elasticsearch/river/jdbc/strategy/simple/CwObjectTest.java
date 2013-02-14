package org.elasticsearch.river.jdbc.strategy.simple;

import org.elasticsearch.river.jdbc.support.CWObject;
import org.testng.Assert;
import org.testng.annotations.*;
import java.util.*;

public class CwObjectTest extends Assert {

    @Test
    public void testCwMerge() {
        List<String> keys = Arrays.asList("project.attributes.[id]", "project.attributes.[key]", "project.attributes.[value]", "project.title", "excerptId", "excerptType", "excerpt");
        CWObject nested = new CWObject(keys);
        Map<String, Object> items = new HashMap<String, Object>() {{
            put("project.attributes.[id]", 1);
            put("project.attributes.[key]", "country");
            put("project.attributes.[value]", "USA");
        }};
        nested.add(items);
        Map<String,Object>obj=new HashMap<String, Object>();
        obj.put("excerpt", "This is for test");
        obj.put("project", new HashMap<String, Object>() {{
            put("title", "codewell");
        }});

        nested.merge(obj);

        assert obj.containsKey("project");
        Map projectMap = (Map) obj.get("project");
        assert projectMap.containsKey("attributes");
        assertEquals(projectMap.get("title"), "codewell");
        assert projectMap.get("attributes") instanceof List;
    }
}
