package fastproto;

public final class Constants {
    /**
     * When true, always use FastList fallback mode instead of PrimitiveArrayWriter optimization.
     * Set to true to disable PrimitiveArrayWriter and simplify debugging/testing.
     */
    public static final boolean USE_FALLBACK_MODE = true;

    private Constants() {
    }
}
