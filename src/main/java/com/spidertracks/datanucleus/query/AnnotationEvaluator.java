/**********************************************************************
Copyright (c) 2010 Pulasthi Supun. All rights reserved.
Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

Contributors :
...
***********************************************************************/

package com.spidertracks.datanucleus.query;

import java.lang.annotation.Annotation;
import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;
import java.util.Map;


/**
 * class to evaluate a given data object and figure out the properties of the
 * given class such as indexed and non indexed fields.
 *
 * @version $Id$
 * @since 1.1.1-0.7.0
 */
public class AnnotationEvaluator
{
    /**
     * loops through the class fields and finds annotations defined for each field and groups
     * together the fields with the same annotation.
     * @param cls the class to be evaluated
     * @return the Map containing each type and there respective fields array.
     */
    public static Map<String, Set<Class>> getFieldAnnotationMap(Class<?> cls)
    {
        final Map<String, Set<Class>> annotationMap = new HashMap<String, Set<Class>>();
        addAnnotatedFields(cls, annotationMap);
        return annotationMap;
    }

    private static void addAnnotatedFields(final Class<?> cls,
                                           final Map<String, Set<Class>> annotationsByFieldName)
    {
        if (cls.getSuperclass() != null) {
            addAnnotatedFields(cls.getSuperclass(), annotationsByFieldName);
        }
        for (final Field field : cls.getDeclaredFields()) {
            Set annotationsThisField = annotationsByFieldName.get(field.getName());
            if (annotationsThisField == null) {
                annotationsThisField = new HashSet<Class>();
                annotationsByFieldName.put(field.getName(), annotationsThisField);
            }
            for (final Annotation ann : field.getDeclaredAnnotations()) {
                annotationsThisField.add(ann.annotationType());
            }
        }
    }
}
