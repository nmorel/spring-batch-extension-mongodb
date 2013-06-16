package com.github.nmorel.spring.batch.mongodb.incrementer;

/**
 * Factory for creating {@link ValueIncrementer} implementations
 * based upon a provided string.
 */
public interface ValueIncrementerFactory
{
    /**
     * Return a {@link ValueIncrementer}.
     *
     * @param incrementerName incrementer name to create. In many cases this may be the
     *                        sequence name
     *
     * @return incrementer
     *
     * @throws IllegalArgumentException if incrementerName
     *                                  is null.
     */
    ValueIncrementer getIncrementer( String incrementerName );
}
