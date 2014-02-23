package org.xbib.elasticsearch.river.jdbc.support;

/**
 * Created with IntelliJ IDEA.
 * User: pramod
 * Date: 11/10/13
 * Time: 7:49 PM
 * To change this template use File | Settings | File Templates.
 */

import org.xbib.elasticsearch.gatherer.IndexableObject;
import org.xbib.elasticsearch.gatherer.Values;

import java.util.*;

/**
 * Adds additional functionality to SimpleValueListener to merge Array of objects into the document
 */
public class CWObject<K> {
    public static int count=0;
    public static final char delimiter = '.';
    Map<String, List<Object>> data;
    Set<String> ids = new HashSet<String>();
    public static final String ID = "id"; //Hardcoded to id field for maintaining unique objects in array

    public CWObject(List<K> keys) {
        for (K key : keys) {
            if (isNestedKey(key.toString()) && key.toString().contains(ID)) this.ids.add(key.toString());
        }
        this.data = new HashMap<String, List<Object>>();
    }

    /**
     * add new set of nested items obtained from a row of mysql db
     * @param items all nested fields from a row
     */
    public void add(Map<String, Object> items) {
        Map<String, Map<String, Object>> attributes = new HashMap();
        Set<String> skip = skip(items);
        for (String key : items.keySet()) {
            String parent = getParent(key);
            if (!skip.contains(parent)) {
                if (attributes.get(parent) == null) attributes.put(parent, new HashMap<String, Object>());
                attributes.get(parent).put(getChild(key), new Values(null, items.get(key),false));
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

    /**
     * nested keys is denoted by a field within a square bracket '[]' and separated from parent with a dot '.'
     * @param key
     * @return
     */
    public static boolean isNestedKey(String key) {
        int i = key.lastIndexOf(delimiter);
        String[] childs=key.split("\\.");
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

    /**
     * After reading all records for a row, need to added to a store for reading next row
     * @param lineData data of nested fields from current row
     */
    public void flush(Map<String, Map<String, Object>> lineData) {
        for (String parentKey : lineData.keySet()) {
            if (data.get(parentKey) == null) data.put(parentKey, new ArrayList<Object>());
            data.get(parentKey).add(lineData.get(parentKey));
        }

    }

    /**
     * After reading all records for a particular id from multiple rows, nested fields should be pushed into the parent JSON object
     * and empty the datastructures for reading rows for next id.
     * @param obj parent object that contains all fields of a document in a JSON representation.
     */
    public void reset(IndexableObject obj) {
        merge(obj.source());
        System.out.println("reset... ");
        data = new HashMap<String, List<Object>>();
    }

    /**
     * merge the nested fields for an id to the parent json
     * @param obj
     */
    public void merge(Map<String,Object> obj) {
        for (String key : data.keySet()) {
            int rootIndex = key.indexOf(delimiter);

            String parent = key.substring(0, rootIndex);
            String child = key.substring(rootIndex + 1);
            Map<String, Object> childMap = new HashMap<String, Object>();
            childMap.put(child, data.get(key));
            if(obj.containsKey(parent)){
                for(Map.Entry<String,Object> entry:((Map<String,Object>) obj.get(parent)).entrySet())
                    childMap.put(entry.getKey(),entry.getValue());
            }
            obj.put(parent, childMap);

        }
    }
}
