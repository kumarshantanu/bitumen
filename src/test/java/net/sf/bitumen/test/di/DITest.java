package net.sf.bitumen.test.di;

import static org.junit.Assert.*;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import net.sf.bitumen.di.DI;
import net.sf.bitumen.di.IComponentSource;
import net.sf.bitumen.util.MapBuilder;

import org.junit.Test;

public class DITest {

    @Test
    public void testConstantlyReturnsConstant() {
        final String strConstant = "hello";
        final IComponentSource<String, String> sf = DI.constantly(strConstant, String.class);
        assertEquals(strConstant, sf.get(new HashMap<String, IComponentSource<?, String>>()));
        final int intConstant = 100;
        final IComponentSource<Integer, String> inf = DI.constantly(intConstant, String.class);
        assertEquals((Integer) intConstant, inf.get(new HashMap<String, IComponentSource<?, String>>()));
    }

    @Test
    public void testSingletonProducesExactlyOneInstance() {
        final AtomicInteger counter = new AtomicInteger(0);
        final IComponentSource<?, String> factory = new IComponentSource<String, String>() {
            @Override
            public String get(Map<String, IComponentSource<?, String>> dependencies) {
                counter.incrementAndGet();
                return "hello";
            }
        };
        final IComponentSource<?, String> singleton = DI.singleton(factory);
        // before invocation
        assertEquals(0, counter.get());
        // first invocation - counter should increment
        assertEquals("hello", singleton.get(new HashMap<String, IComponentSource<?, String>>()));
        assertEquals(1, counter.get());
        // second invocation - count should remain the same
        assertEquals("hello", singleton.get(new HashMap<String, IComponentSource<?, String>>()));
        assertEquals(1, counter.get());
    }

    @Test
    public void testGetInstance() {
        final Map<String, IComponentSource<?, String>> deps = new MapBuilder<String, IComponentSource<?, String>>(
                new HashMap<String, IComponentSource<?, String>>())
        .add("fooK", DI.constantly("foo", String.class))
        .add("barK", DI.constantly(32768, String.class))
        .add("bazK", new IComponentSource<String, String>() {
            @Override
            public String get(Map<String, IComponentSource<?, String>> dependencies) {
                String foo = DI.getInstance(dependencies, "fooK", String.class);
                Integer bar = DI.getInstance(dependencies, "barK", Integer.class);
                return foo + bar;
            }
        })
        .get();
        assertEquals("Simple String", "foo", DI.getInstance(deps, "fooK", String.class));;
        assertEquals("Simple Integer", (Integer) 32768, DI.getInstance(deps, "barK", Integer.class));;
        assertEquals("Dependency chaining", "foo32768", DI.getInstance(deps, "bazK", String.class));
    }

    private static final Map<String, IComponentSource<?, String>> EMPTY_DEPMAP =
            Collections.<String, IComponentSource<?, String>> emptyMap();

    @Test
    public void testConstruct() {
        // happy scenario
        final IComponentSource<?, String> bytes = DI.constantly("Hello".getBytes(), String.class);
        @SuppressWarnings("unchecked")
        final IComponentSource<?, String> hello = DI.construct(String.class, bytes);
        assertEquals("Happy scenario - single constructor argument", "Hello", hello.get(EMPTY_DEPMAP));
        // happy scenario - more than one constructor argument
        final IComponentSource<?, String> offset = DI.constantly(1, String.class);
        final IComponentSource<?, String> length = DI.constantly(4, String.class);
        @SuppressWarnings("unchecked")
        final IComponentSource<?, String> ello = DI.construct(String.class, bytes, offset, length);
        assertEquals("Happy scenario - multiple constructor args", "ello", ello.get(EMPTY_DEPMAP));
        // unhappy scenario
        final IComponentSource<?, String> integer = DI.constantly(10, String.class);
        @SuppressWarnings("unchecked")
        final IComponentSource<?, String> nostr = DI.construct(String.class, integer);
        try {
            assertEquals("Unhappy scenario", "", nostr.get(EMPTY_DEPMAP));
            fail("Expected to throw " + IllegalArgumentException.class + " but it did not");
        } catch(IllegalArgumentException e) {
            // do nothing
        }
    }

    @Test
    public void testSourceOf() {
        // happy scenario
        final Map<String, IComponentSource<?, String>> deps = new MapBuilder<String, IComponentSource<?, String>>()
                .add("n", DI.constantly(10, String.class))
                .get();
        final IComponentSource<?, String> source = DI.sourceOf("n");
        assertEquals("Happy scenario", 10, source.get(deps));
        // unhappy scenario
        try {
            assertEquals("Unhappy scenario", -1, source.get(EMPTY_DEPMAP));
            fail("Expected to throw " + IllegalArgumentException.class + " but it did not");
        } catch(IllegalArgumentException e) {
            // do nothing
        }
    }

    @Test
    public void testConstructByKey() {
        // happy scenario
        final IComponentSource<?, String> bytes = DI.constantly("Hello".getBytes(), String.class);
        final IComponentSource<?, String> hello = DI.constructByKey(String.class, "bytes");
        final Map<String, IComponentSource<?, String>> deps1 = new MapBuilder<String, IComponentSource<?, String>>()
                .add("bytes", bytes)
                .add("hello", hello)
                .get();
        assertEquals("Happy scenario - single constructor argument", "Hello", DI.getInstance(deps1, "hello", String.class));
        // happy scenario - more than one constructor argument
        final IComponentSource<?, String> offset = DI.constantly(1, String.class);
        final IComponentSource<?, String> length = DI.constantly(4, String.class);
        final IComponentSource<?, String> ello = DI.constructByKey(String.class, "bytes", "offset", "length");
        final Map<String, IComponentSource<?, String>> deps2 = new MapBuilder<String, IComponentSource<?, String>>()
                .add("bytes", bytes)
                .add("offset", offset)
                .add("length", length)
                .add("ello", ello)
                .get();
        assertEquals("Happy scenario - multiple constructor args", "ello", DI.getInstance(deps2, "ello", String.class));
        // unhappy scenario
        final IComponentSource<?, String> integer = DI.constantly(10, String.class);
        final IComponentSource<?, String> nostr = DI.constructByKey(String.class, "integer");
        final Map<String, IComponentSource<?, String>> deps3 = new MapBuilder<String, IComponentSource<?, String>>()
                .add("integer", integer)
                .add("nostr", nostr)
                .get();
        try {
            assertEquals("Unhappy scenario", "", DI.getInstance(deps3, "nostr", String.class));
            fail("Expected to throw " + IllegalArgumentException.class + " but it did not");
        } catch(IllegalArgumentException e) {
            // do nothing
        }
    }

    @Test
    public void testAutoConstruct() {
        // happy scenario
        final Map<String, Class<?>> types1 = new MapBuilder<String, Class<?>>()
                .add("bytes", byte[].class)
                .get();
        final IComponentSource<?, String> bytes = DI.constantly("Hello".getBytes(), String.class);
        final IComponentSource<?, String> hello = DI.autoConstruct(types1, String.class);
        final Map<String, IComponentSource<?, String>> deps1 = new MapBuilder<String, IComponentSource<?, String>>()
                .add("bytes", bytes)
                .add("hello", hello)
                .get();
        assertEquals("Happy scenario - single constructor argument", "Hello", hello.get(deps1));
        // unhappy scenario - ambiguous more than one constructor argument of same type
        final Map<String, Class<?>> types2 = new MapBuilder<String, Class<?>>()
                .add("bytes", byte[].class)
                .add("offset", Integer.class)
                .add("length", Integer.class)
                .get();
        final IComponentSource<?, String> offset = DI.constantly(1, String.class);
        final IComponentSource<?, String> length = DI.constantly(4, String.class);
        final IComponentSource<?, String> ello = DI.autoConstruct(types2, String.class);
        final Map<String, IComponentSource<?, String>> deps2 = new MapBuilder<String, IComponentSource<?, String>>()
                .add("bytes", bytes)
                .add("offset", offset)
                .add("length", length)
                .add("ello", ello)
                .get();
        try {
            assertEquals("Unhappy scenario - ambiguous multiple constructor args of same type ", "ello", ello.get(deps2));
            fail("Expected to throw " + IllegalArgumentException.class + " but it did not");
        } catch(IllegalArgumentException e) {
            // do nothing
        }
    }

}
