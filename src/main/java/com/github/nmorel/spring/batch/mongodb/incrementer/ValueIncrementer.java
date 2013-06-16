package com.github.nmorel.spring.batch.mongodb.incrementer;

/**
 * Interface that defines contract of incrementing any data store field's
 * maximum value. Works much like a sequence number generator.
 */
public interface ValueIncrementer
{
    /**
     * Increment the data store field's max value as int.
     *
     * @return int next data store value such as <b>max + 1</b>
     */
    int nextIntValue();

    /**
     * Increment the data store field's max value as long.
     *
     * @return int next data store value such as <b>max + 1</b>
     */
    long nextLongValue();

    /**
     * Increment the data store field's max value as String.
     *
     * @return next data store value such as <b>max + 1</b>
     */
    String nextStringValue();
}
