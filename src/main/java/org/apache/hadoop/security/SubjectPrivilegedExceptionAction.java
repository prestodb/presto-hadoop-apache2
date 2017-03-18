package org.apache.hadoop.security;

public interface SubjectPrivilegedExceptionAction<T>
{
    T run()
            throws Exception;
}
