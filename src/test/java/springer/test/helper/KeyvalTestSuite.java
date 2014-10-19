package springer.test.helper;

import springer.jdbc.kv.IKeyvalRead;
import springer.jdbc.kv.IKeyvalWrite;

public interface KeyvalTestSuite {

    public void insertTest(final IKeyvalWrite<Integer, String> writer, final IKeyvalRead<Integer, String> reader);

    public void crudTest(final IKeyvalWrite<Integer, String> writer, final IKeyvalRead<Integer, String> reader);

    public void versionTest(final IKeyvalWrite<Integer, String> writer, final IKeyvalRead<Integer, String> reader);

    public void readTest(final IKeyvalWrite<Integer, String> writer, final IKeyvalRead<Integer, String> reader);

}
