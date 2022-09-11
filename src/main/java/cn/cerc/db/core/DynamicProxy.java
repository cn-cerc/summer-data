package cn.cerc.db.core;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Proxy;

/**
 * https://www.liaoxuefeng.com/wiki/1252599548343744/1264804593397984
 */
public class DynamicProxy {

    public static void main(String[] args) {
        InvocationHandler handler = (proxy, method, args1) -> {
            System.out.println(method);
            if (method.getName().equals("morning")) {
                System.out.println("Good morning, " + args1[0]);
            }
            return null;
        };
        Hello hello = (Hello) Proxy.newProxyInstance(
                Hello.class.getClassLoader(), // 传入ClassLoader
                new Class[]{Hello.class}, // 传入要实现的接口
                handler); // 传入处理调用方法的InvocationHandler
        hello.morning("Bob");
    }

}

interface Hello {
    void morning(String name);
}
