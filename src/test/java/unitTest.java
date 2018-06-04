import com.vdtas.Application;
import org.junit.Test;

import java.util.Arrays;

import static org.junit.Assert.assertEquals;

/**
 * @author vvandertas
 */
public class unitTest {

  /**
   * Figure out how to extract the float values from the segments_timbre and segments_pitch fields
   */
  @Test
  public void parseTest() {
    String input = "[[1.    0.921 0.36  0.361 0.479 0.471 0.511 0.564 0.475 0.515 0.472 0.485] [0.917 0.681 0.625 0.652 0.721 0.659 0.55  0.594 0.702 1.    0.895 0.602] [1.    0.913 0.461 0.478 0.467 0.316 0.28  0.261 0.242 0.21  0.364 0.645] [1.    0.82  0.639 0.419 0.608 0.641 0.482 0.414 0.461 0.816 0.826 0.572]]";
    String[] split = input.substring(2,input.length() -2).split("\\]\\s*\\[");
    assertEquals(4, split.length);

    Float[][] topArray = new Float[split.length][12];
    for(int i = 0; i < split.length; i++) {
      String[] array = split[i].split("\\s+");
      Float[] floats = Arrays.stream(array).map(Float::valueOf).toArray(Float[]::new);
      topArray[i] = floats;
    }
  }
}
