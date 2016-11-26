package assignment5;

import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

public class MapUtil {
	
	/*
	 * This MapUtil class is a helper class used to define the sortByValue function that sorts any given
	 * map based on not it's keys, but it's values. This is a generic sort function and hence can be used
	 * with any map of any key and value object types. Note that this function sorts the map in the
	 * decreasing order and not the increasing order
	 */
	
    public static <K, V extends Comparable<? super V>> Map<K, V> sortByValue(Map<K, V> map) {
    	
        List<Map.Entry<K, V>> list = new LinkedList<Map.Entry<K, V>>(map.entrySet());
        Collections.sort(list, 
    		new Comparator<Map.Entry<K, V>>() {
	            public int compare(Map.Entry<K, V> o1, Map.Entry<K, V> o2) {
	                	return -1 * (o1.getValue()).compareTo(o2.getValue());
	            }
	        }
        );

        Map<K, V> result = new LinkedHashMap<K, V>();
        for (Map.Entry<K, V> entry : list) {
            result.put(entry.getKey(), entry.getValue());
        }
        
        return result;
    }
}