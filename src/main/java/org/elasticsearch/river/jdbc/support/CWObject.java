package org.elasticsearch.river.jdbc.support;

import java.util.*;

/**
 * Created by IntelliJ IDEA.
 * User: kushum
 * Date: 1/30/13
 * Time: 10:39 PM
 * To change this template use File | Settings | File Templates.
 */
public class CWObject {
    public static int count=0;
    public static final char delimiter = '.';
    Map<String, List<Object>> data;
    Set<String> ids = new HashSet<String>();
    public static final String ID = "id"; //Hardcoded to id field for maintaining unique objects in array

    public CWObject(List<String> keys) {
        for (String key : keys) {
            if (isNestedKey(key) && key.contains(ID)) this.ids.add(key);
        }
        this.data = new HashMap<String, List<Object>>();
    }

    public void add(Map<String, Object> items) {
        Map<String, Map<String, Object>> attributes = new HashMap();
        Set<String> skip = skip(items);
        for (String key : items.keySet()) {
            String parent = getParent(key);
            if (!skip.contains(parent)) {
                if (attributes.get(parent) == null) attributes.put(parent, new HashMap<String, Object>());
                attributes.get(parent).put(getChild(key), new ValueSet(null, items.get(key),false));
            }
        }
        flush(attributes);
    }

    protected Set<String> skip(Map<String, Object> items) {
        Set<String> skipParentSet = new HashSet<String>();
        for (String _id : ids) {
            String id = items.get(_id).toString();
            if (hasId(getParent(_id), id)) skipParentSet.add(getParent(_id));
        }
        return skipParentSet;
    }

    public static boolean isNestedKey(String key) {
        int i = key.lastIndexOf(delimiter);
//        System.out.println("Key is "+key);
        String[] childs=key.split("\\.");
//        for(String st:childs) System.out.println("split"+st);
        return childs[childs.length-1].startsWith("[")&&childs[childs.length-1].endsWith("]"); //nested elements being designated within square brackets viz. parent.[element]
    }

    public static boolean isNullOREmpty(String s) {
        if (s == null) return true;
        return s.isEmpty();
    }

    public static String getChild(String key) {
        int i = key.lastIndexOf(delimiter);
        return key.substring(i + 1).replace("[", "").replace("]", "");
    }

    public static String getParent(String key) {
        int i = key.lastIndexOf(delimiter);
        return key.substring(0, i);
    }

    public boolean hasId(String key, String id) {
        if (data != null && data.size() > 0)
            for (Object o : data.get(key)) {
                Map m = (Map<String, Object>) o;
                if (m.containsKey(ID) && m.get(ID).toString().equals(id)) return true;
            }
        return false;
    }

    public void flush(Map<String, Map<String, Object>> lineData) {
        for (String parentKey : lineData.keySet()) {
            if (data.get(parentKey) == null) data.put(parentKey, new ArrayList<Object>());
            data.get(parentKey).add(lineData.get(parentKey));
        }

    }

    public void reset(StructuredObject obj) {
        merge(obj);
        data = new HashMap<String, List<Object>>();
    }

    protected void merge(StructuredObject obj) {
        for (String key : data.keySet()) {
            int rootIndex = key.indexOf(delimiter);
            String parent = key.substring(0, rootIndex);
            String child = key.substring(rootIndex + 1);
            Map<String, Object> childMap = new HashMap<String, Object>();
            childMap.put(child, data.get(key));
            if(obj.source().containsKey(parent)){
                for(Map.Entry<String,Object> entry:((Map<String,Object>) obj.source().get(parent)).entrySet())
                childMap.put(entry.getKey(),entry.getValue());
            }
            obj.source().put(parent, childMap);
        }
    }
}
