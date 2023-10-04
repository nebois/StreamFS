package com.nebois.streamfs.client.util;

import com.google.gson.Gson;
import java.lang.reflect.Type;

public class OneGson {

    private static Gson gson = new Gson();

    public static String toJson(Object obj) {
        return gson.toJson(obj);
    }

    public static <T> T fromJson(String jsonStr, Class<T> clzT) {
        return gson.fromJson(jsonStr, clzT);
    }

    public static <T> T fromJson(String json, Type typeOfT) {
        return gson.fromJson(json, typeOfT);
    }
}
