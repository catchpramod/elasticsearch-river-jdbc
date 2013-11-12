package org.xbib.elasticsearch.river.jdbc.strategy.simple;

import org.testng.Assert;
import org.testng.annotations.Test;
import org.xbib.elasticsearch.river.jdbc.support.CWObject;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created with IntelliJ IDEA.
 * User: pramod
 * Date: 11/11/13
 * Time: 10:17 PM
 * To change this template use File | Settings | File Templates.
 */
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
