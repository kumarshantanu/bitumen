package starfish.test.helper;

import starfish.KeyvalRead;
import starfish.KeyvalWrite;

public interface KeyvalTestSuite {

    public void insertTest(final KeyvalWrite<Integer, String> writer, final KeyvalRead<Integer, String> reader);

    public void crudTest(final KeyvalWrite<Integer, String> writer, final KeyvalRead<Integer, String> reader);

    public void versionTest(final KeyvalWrite<Integer, String> writer, final KeyvalRead<Integer, String> reader);

    public void readTest(final KeyvalWrite<Integer, String> writer, final KeyvalRead<Integer, String> reader);

}
