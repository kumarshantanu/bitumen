package starfish.test.helper;

import starfish.IOpsRead;
import starfish.IOpsWrite;

public interface OpsTestSuite {

    public void insertTest(final IOpsWrite<Integer, String> writer, final IOpsRead<Integer, String> reader);

    public void crudTest(final IOpsWrite<Integer, String> writer, final IOpsRead<Integer, String> reader);

    public void versionTest(final IOpsWrite<Integer, String> writer, final IOpsRead<Integer, String> reader);

    public void readTest(final IOpsWrite<Integer, String> writer, final IOpsRead<Integer, String> reader);

}
