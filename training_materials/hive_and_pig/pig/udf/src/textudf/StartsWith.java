
package textudf;

import java.io.IOException;
import java.util.Map;
import org.apache.pig.FilterFunc;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.util.WrappedIOException;


/**
 * @class StartsWith
 * implements the StartsWith() filter function in Pig
 * StartsWith(haystack, needle) returns true if haystack.startsWith(needle)
 */
public class StartsWith extends FilterFunc {
  public Boolean exec(Tuple input) throws IOException {
    if (input == null || input.size() == 0) {
      return null;
    }

    try {
      Object haystackObj = input.get(0);
      Object needleObj = input.get(1);

      String haystack = null;
      String needle = null;

      if (haystackObj instanceof String || haystackObj instanceof DataByteArray) {
        haystack = haystackObj.toString();
      } else {
        throw new IOException("Invalid datatype for haystack; expected String");
      }

      if (needleObj instanceof String || needleObj instanceof DataByteArray) {
        needle = needleObj.toString();
      } else {
        throw new IOException("Invalid datatype for needle; expected String");
      }

      return Boolean.valueOf(haystack.startsWith(needle));
    } catch (ExecException ee) {
      throw WrappedIOException.wrap("Exception processing input row", ee);
    }
  }
}

