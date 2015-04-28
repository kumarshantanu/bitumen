package net.sf.bitumen.test.helper;

import net.sf.bitumen.jdbc.kv.IKeyvalRead;
import net.sf.bitumen.jdbc.kv.IKeyvalWrite;

public interface KeyvalTestSuite {

    public void insertTest(final IKeyvalWrite<Integer, String> writer, final IKeyvalRead<Integer, String> reader);

    public void crudTest(final IKeyvalWrite<Integer, String> writer, final IKeyvalRead<Integer, String> reader);

    public void versionTest(final IKeyvalWrite<Integer, String> writer, final IKeyvalRead<Integer, String> reader);

    public void readTest(final IKeyvalWrite<Integer, String> writer, final IKeyvalRead<Integer, String> reader);

}
