package springer.di;

import java.util.Properties;

public class PropertyGetter<K> {

    private final Properties properties;
    private final Class<K> componentKeyClass;

    public PropertyGetter(Properties properties, Class<K> componentKeyClass) {
        this.properties = properties;
        this.componentKeyClass = componentKeyClass;
    }

    public IComponentSource<Object, K> get(String name) {
        return DI.constantly(properties.get(name), componentKeyClass);
    }

    public IComponentSource<String, K> getString(String name) {
        return DI.constantly((String) properties.get(name), componentKeyClass);
    }

    public IComponentSource<Boolean, K> getBoolean(String name) {
        return DI.constantly(Boolean.parseBoolean((String) properties.get(name)), componentKeyClass);
    }

    public IComponentSource<Integer, K> getInteger(String name) {
        return DI.constantly(Integer.parseInt((String) properties.get(name)), componentKeyClass);
    }

    public IComponentSource<Long, K> getLong(String name) {
        return DI.constantly(Long.parseLong((String) properties.get(name)), componentKeyClass);
    }

    public IComponentSource<Float, K> getFloat(String name) {
        return DI.constantly(Float.parseFloat((String) properties.get(name)), componentKeyClass);
    }

    public IComponentSource<Double, K> getDouble(String name) {
        return DI.constantly(Double.parseDouble((String) properties.get(name)), componentKeyClass);
    }

}
