import com.mars.fuzznnrl.data.DataPipelayer;
import org.junit.Test;

public class DataTest {
    @Test
    public void testDatapipelayer() {
        String test_dir = "data";
        int numOfShuffles = 5;
        int writingBatchSize = 200;
        DataPipelayer pipelayer = new DataPipelayer(test_dir, numOfShuffles, writingBatchSize, "csv");
        pipelayer.startJob();
    }
}
