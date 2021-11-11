import org.apache.commons.lang3.builder.ReflectionToStringBuilder;

public class Debuggable {
    public String toString() {
        return ReflectionToStringBuilder.toString(this);
    }
}
