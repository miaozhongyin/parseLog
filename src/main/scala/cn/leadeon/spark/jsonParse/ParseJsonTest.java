package cn.leadeon.spark.jsonParse;


import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;

import java.util.*;

/**
 * ParseJsonTest 为 解析JSON java 实现。可以直接用来测试。
 */
public class ParseJsonTest {

    public static final String  json = "";



    /*用于储存最终结果的list*/
    public static List<JSONObject> resultList = new ArrayList<>();

//    public static void main(String[] args) {
//
//        JSONObject jsonObject = JSON.parseObject(json);
//
//        parseJSON(jsonObject, new HashMap<>());
//
//        for (JSONObject obj :resultList) {
//            System.out.println(obj);
//        }
//
//
//    }

    private static List parseJSON(JSONObject jsonObject, Map<String, Object> map) {

        Set<String> keySet = jsonObject.keySet();

        List currKeys = Arrays.asList(keySet);

        boolean flag = false;
        for (String key : keySet) {
            Object value = jsonObject.get(key);
            if (value == null) {
                continue;
            }
            if (value instanceof JSONArray) {
                flag = true;
            } else {
                map.put(key, value);
            }
        }
        for (String key : keySet) {
            Object value = jsonObject.get(key);
            if (value instanceof JSONArray) {
                for (Object o1 : (JSONArray) value) {
                    map.keySet().removeAll(parseJSON((JSONObject) o1, map));
                }
            }
        }
        //如果flag为true说明还有array没有解析完
        if (!flag) {
            resultList.add(JSONObject.parseObject(JSON.toJSONString(map)));
        }
        return currKeys;
    }

}