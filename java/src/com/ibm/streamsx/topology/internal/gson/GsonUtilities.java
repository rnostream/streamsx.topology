/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2016  
 */
package com.ibm.streamsx.topology.internal.gson;

import java.util.Collection;
import java.util.Iterator;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.ibm.streamsx.topology.function.Consumer;

public class GsonUtilities {
    
    private static final Gson gson = new Gson();
    
    public static Gson gson() {
        return gson;
    }
    
    public static String toJson(JsonElement element) {
        return gson().toJson(element);
    }
    
    /**
     * Perform an action on every JsonObject in an array.
     */
    public static void objectArray(JsonObject object, String property, Consumer<JsonObject> action) {
        if (object == null)
            return;
        JsonArray array = array(object, property);
        if (array == null)
            return;
        array.forEach(e -> action.accept(e.getAsJsonObject()));
    }
    
    /**
     * Perform an action on every String in an array.
     */
    public static void stringArray(JsonObject object, String property, Consumer<String> action) {
        if (object == null)
            return;

        JsonArray array = array(object, property);
        if (array == null)
            return;
        array.forEach(e -> action.accept(e.getAsString()));
    }
    
    /**
     * Return a Json array. If the value is not
     * an array then an array containing the single
     * value is returned.
     * Returns null if the array is not present or present as JSON null.
     */
    public static JsonArray array(JsonObject object, String property) {
        if (object.has(property)) {
            JsonElement je = object.get(property);
            if (je.isJsonNull())
                return null;
            if (je.isJsonArray())
                return je.getAsJsonArray();
            JsonArray array = new JsonArray();
            array.add(je);
            return array;
        }
        return null;
    }
    /**
     * Return a Json object.
     * Returns null if the object is not present or null.
     */
    public static JsonObject jobject(JsonObject object, String property) {
        if (object.has(property)) {
            JsonElement je = object.get(property);
            if (je.isJsonNull())
                return null;
            return je.getAsJsonObject();
        }
        return null;
    }
    
    public static boolean jisEmpty(JsonObject object) {
        return object == null || object.isJsonNull() || object.entrySet().isEmpty();
    }
    public static boolean jisEmpty(JsonArray array) {
        return array == null || array.size() == 0;
    }
    
    public static void gclear(JsonArray array) {
        Iterator<JsonElement> it = array.iterator();
        while(it.hasNext())
            it.remove();
    }
    
    /**
     * Returns a property as a String.
     * @param object
     * @param property
     * @return Value or null if it is not set.
     */
    public static String jstring(JsonObject object, String property) {
        if (object.has(property)) {
            JsonElement je = object.get(property);
            if (je.isJsonNull())
                return null;
            return je.getAsString();
        }
        return null;
    }
    public static boolean jboolean(JsonObject object, String property) {
        if (object.has(property)) {
            JsonElement je = object.get(property);
            if (je.isJsonNull())
                return false;
            return je.getAsBoolean();
        }
        return false;
    }
    
    public static JsonObject first(Collection<JsonObject> objects) {
        return objects.iterator().next();
    }
    
    public static JsonObject nestedObject(JsonObject object, String nested, String property) {
        JsonObject nester = jobject(object, nested);
        if (nester == null)
            return null;
        
        return jobject(nester, property);
    }
    
    /**
     * Get a json object from a property or properties.
     * @param object
     * @param property
     * @return Valid object of null if any element of the properties does not exist.
     */
    public static JsonObject object(JsonObject object,  String ...property) {
        
        assert property.length > 0;
        
        JsonObject item = null;
        for (String key : property) {
            item = jobject(object, key);
            if (item == null)
                return null;
            object = item;
        }

        return item; 
    }
    
    /**
     * Create nested set of JSON objects in object.
     * 
     *  E.g. if passed obj, "a", "b", "c" then obj contain:
     *  
     *  "a": { "b": { "c" : {} } } }
     *  
     *  If any of the properties already exist then they must be objects
     *  then they are not modifed at that level. E.g. if "a" already exists
     *  and has "b" then it is not modified, but "b" will have "c" added to it
     *  if it didn't already exist. 
     */
    public static JsonObject objectCreate(JsonObject object, String ...property) {
        
        assert property.length > 0;
        
        JsonObject item = null;
        for (String key : property) {
            item = jobject(object, key);
            if (item == null)
                object.add(key, item = new JsonObject());
            object = item;
        }

        return item; 
    }
}
