package org.apache.hadoop.security;

import javax.security.auth.Subject;

import java.security.PrivilegedActionException;
import java.util.ArrayDeque;
import java.util.Deque;

class AsSubjectInvoker
{
    private static ThreadLocal<Deque<Subject>> SUBJECT_STACKS = new ThreadLocal<Deque<Subject>>()
    {
        @Override
        protected Deque<Subject> initialValue()
        {
            return new ArrayDeque<>();
        }
    };

    public static Subject getCurrentUser()
    {
        return subjectStack().peek();
    }

    private static Deque<Subject> subjectStack()
    {
        return SUBJECT_STACKS.get();
    }

    public static <T> T doAs(Subject subject, SubjectPrivilegedAction<T> action)
    {
        subjectStack().push(subject);
        try {
            return action.run();
        }
        finally {
            subjectStack().pop();
        }
    }

    public static <T> T doAs(Subject subject, SubjectPrivilegedExceptionAction<T> action)
            throws PrivilegedActionException
    {
        subjectStack().push(subject);
        try {
            return action.run();
        }
        catch (PrivilegedActionException e) {
            throw e;
        }
        catch (Exception e) {
            throw new PrivilegedActionException(e);
        }
        finally {
            subjectStack().pop();
        }
    }
}
