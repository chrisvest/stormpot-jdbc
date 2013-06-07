package stormpot.jdbc;

import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.sql.Connection;

class AdaptorMetaFactory {
  static final String JDBC40 = "stormpot.jdbc.Jdbc40AdaptorFactory";
  static final String JDBC41 = "stormpot.jdbc.Jdbc41AdaptorFactory";
  
  public static AdaptorFactory getAdaptorFactory() {
    Method[] connectionMethods = Connection.class.getDeclaredMethods();
    
    if (hasMethod("getSchema", connectionMethods)) {
      return buildFactory(JDBC41);
    }
    return buildFactory(JDBC40);
  }

  private static boolean hasMethod(String methodName, Method[] methods) {
    for (Method method : methods) {
      if (method.getName().equals(methodName)) {
        return true;
      }
    }
    return false;
  }

  private static AdaptorFactory buildFactory(String className) {
    try {
      Class<?> cls = tryGetClass(className);
      Constructor<?> ctor = cls.getConstructor();
      return (AdaptorFactory) ctor.newInstance();
    } catch (Exception e) {
      throw new RuntimeException("Could not instansiate AdaptorFactory", e);
    }
  }

  private static Class<?> tryGetClass(String className)
      throws ClassNotFoundException {
    return Class.forName(className);
  }
}
