package org.apache.hadoop.security;

public interface SubjectPrivilegedAction<T>
{
    T run();
}
