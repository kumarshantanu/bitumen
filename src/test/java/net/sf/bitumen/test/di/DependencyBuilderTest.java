package net.sf.bitumen.test.di;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.util.Map;

import net.sf.bitumen.di.DI;
import net.sf.bitumen.di.DependencyBuilder;
import net.sf.bitumen.di.IComponentSource;
import net.sf.bitumen.util.MapBuilder;

import org.junit.Test;

public class DependencyBuilderTest {

    @Test
    public void testSet() {
        final DependencyBuilder<String> db = new DependencyBuilder<String>();
        try {
            db.set(null, DI.constantly("", String.class));
            fail("null key should be rejected");
        } catch (IllegalArgumentException e) {
            // do nothing
        }
        try {
            db.set("absent", null);
            fail("null source should be rejected");
        } catch (IllegalArgumentException e) {
            // do nothing
        }

        // setting source should work fine
        db.set("foo", DI.constantly(10, String.class));

        // repeated setting of source for same key should work fine
        db.set("foo", DI.constantly(20, String.class));
    }

    @Test
    public void testAdd() {
        final DependencyBuilder<String> db = new DependencyBuilder<String>();
        try {
            db.add(null, DI.constantly("", String.class));
            fail("null key should be rejected");
        } catch (IllegalArgumentException e) {
            // do nothing
        }
        try {
            db.add("absent", null);
            fail("null source should be rejected");
        } catch (IllegalArgumentException e) {
            // do nothing
        }

        // adding source first time should work fine
        db.add("foo", DI.constantly(10, String.class));

        // repeated adding of source for same key should fail
        try {
            db.add("foo", DI.constantly(20, String.class));
            fail("Duplicating adding of source for same key should be rejected");
        } catch (IllegalArgumentException e) {
            // do nothing
        }
    }

    @Test
    public void testAddSingleton() {
        final DependencyBuilder<String> db = new DependencyBuilder<String>();
        try {
            db.addSingleton(null, DI.constantly("", String.class));
            fail("null key should be rejected");
        } catch (IllegalArgumentException e) {
            // do nothing
        }
        try {
            db.addSingleton("absent", null);
            fail("null source should be rejected");
        } catch (IllegalArgumentException e) {
            // do nothing
        }

        final IComponentSource<Integer, String> counterSource = new IComponentSource<Integer, String>() {
            int c = 0;
            @Override
            public Integer get(Map<String, IComponentSource<?, String>> dependencies) {
                return c++;
            }
        };
        Map<String, IComponentSource<?, String>> deps = new MapBuilder<String, IComponentSource<?, String>>()
                .add("counter", counterSource)
                .get();

        // factory returns a new value on each call
        assertEquals(new Integer(0), DI.getInstance(deps, "counter", Integer.class));
        assertEquals(new Integer(1), DI.getInstance(deps, "counter", Integer.class));

        // singleton returns the same cached value that is generated once
        db.addSingleton("counter", counterSource);
        assertEquals(new Integer(2), db.getInstance("counter", Integer.class));
        assertEquals(new Integer(2), db.getInstance("counter", Integer.class));

        // adding a singleton as singleton should also work fine
        final IComponentSource<?, String> singleton = DI.singleton(counterSource);
        db.addSingleton("counter2", singleton);
    }

    @Test
    public void testAddFactory() {
        final DependencyBuilder<String> db = new DependencyBuilder<String>();
        try {
            db.addFactory(null, DI.constantly("", String.class));
            fail("null key should be rejected");
        } catch (IllegalArgumentException e) {
            // do nothing
        }
        try {
            db.addFactory("absent", null);
            fail("null source should be rejected");
        } catch (IllegalArgumentException e) {
            // do nothing
        }

        final IComponentSource<Integer, String> counterSource = new IComponentSource<Integer, String>() {
            int c = 0;
            @Override
            public Integer get(Map<String, IComponentSource<?, String>> dependencies) {
                return c++;
            }
        };
        db.addFactory("counter", counterSource);
        try {
            db.addFactory("counter2", DI.singleton(counterSource));
            fail("Singleton cannot be added as factory");
        } catch (IllegalArgumentException e) {
            // do nothing
        }
    }

    @Test
    public void testAddConstant() {
        final DependencyBuilder<String> db = new DependencyBuilder<String>();
        try {
            db.addConstant(null, DI.constantly("", String.class));
            fail("null key should be rejected");
        } catch (IllegalArgumentException e) {
            // do nothing
        }
        // null value should work fine
        db.addConstant("absent", null);
        // ordinary object should work fine
        db.addConstant("present", 100);

        final IComponentSource<Integer, String> counterSource = new IComponentSource<Integer, String>() {
            int c = 0;
            @Override
            public Integer get(Map<String, IComponentSource<?, String>> dependencies) {
                return c++;
            }
        };
        try {
            db.addConstant("counter", counterSource);
            fail("Component sources cannot be added as constant");
        } catch (IllegalArgumentException e) {
            // do nothing
        }
    }

}
