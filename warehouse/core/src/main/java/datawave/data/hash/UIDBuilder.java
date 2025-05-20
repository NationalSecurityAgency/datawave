package datawave.data.hash;

import java.util.Date;

import org.apache.commons.cli.Option;
import org.apache.hadoop.conf.Configuration;

/**
 * Creates UIDs based on various types of input
 *
 * @param <UID_TYPE>
 *            the type of UID
 */
public interface UIDBuilder<UID_TYPE extends UID> {

    /**
     * Validate and adjust the Hadoop configuration based on the specified options
     *
     * @param config
     *            a Hadoop configuration
     * @param options
     *            command-line options (may include thread-inserted options, as in the case of {@link SnowflakeUID}s)
     */
    void configure(Configuration config, final Option... options);

    /**
     * Build a new UID, which may be appended with one or more "extra" values, if specified
     * <p>
     * <b>Note:</b> Any extra values will be presumably, but not necessarily, delimited by the {@link UIDConstants#DEFAULT_SEPARATOR} character
     *
     * @param extras
     *            values to append, if any
     * @return a new UID
     */
    UID_TYPE newId(String... extras);

    /**
     * Build a new UID based on binary data, which may be appended with one or more "extra" values, if specified
     *
     * @param data
     *            binary data
     * @param extras
     *            values to append, if any
     * @return a new UID
     */
    UID_TYPE newId(byte[] data, String... extras);

    /**
     * Build a new UID based on a timestamp, which may be appended with one or more "extra" values, if specified
     *
     * @param time
     *            a point in time
     * @param extras
     *            values to append, if any
     * @return a new UID
     */
    UID_TYPE newId(Date time, String... extras);

    /**
     * Build a new UID based on binary data and a timestamp, which may be appended with one or more "extra" values, if specified
     *
     * @param data
     *            binary data
     * @param time
     *            a point in time
     * @param extras
     *            values to append, if any
     * @return a new UID
     */
    UID_TYPE newId(byte[] data, Date time, String... extras);

    /**
     * Build a new UID based on a template UID (presumably a parent instance), plus any optional "extra" values with which to append to the new instance. If no
     * extras are applied, the method will a wholesale copy of the template, or an entirely new ID if the template is null.
     *
     * @param template
     *            a UID with which to model the returned instance
     * @param extras
     *            values to append, if any
     * @return a new UID
     */
    UID_TYPE newId(UID template, String... extras);
}
