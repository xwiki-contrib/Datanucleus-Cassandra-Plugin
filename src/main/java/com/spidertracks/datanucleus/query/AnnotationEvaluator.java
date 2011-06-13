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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
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
     * Map to contain all the annotations and the related fields.
     */
    private Map<String, ArrayList<String>> annotationMap = new HashMap<String, ArrayList<String>>();

    /**
     * The class object to be evaluated .
     */
    private Class<? extends Object> cls;

    /**
     * non args constructor for the AnnotationEvaluator.
     */
    public AnnotationEvaluator() {


    }

    /**
     * Constructor that takes a class object as a argument.
     * @param cls the class to be evaluated
     */
    public AnnotationEvaluator(Class<? extends Object> cls) {
        this.cls = cls;
        this.annotationMap = getFieldannotationMap(cls);
    }


    /**
     * calls getFieldannotationMap(Class).
     * @param dataObject the object of the class to be evaluated
     * @return the Map containing each type and there respective fields array.
     */
    public Map<String, ArrayList<String>> getFieldannotationMap(Object dataObject) {
        cls = dataObject.getClass();

        return getFieldannotationMap(cls);
    }

    /**
     * loops through the class fields and finds annotations defined for each field and groups
     * together the fields with the same annotation.
     * @param cls the class to be evaluated
     * @return the Map containing each type and there respective fields array.
     */
    public Map<String, ArrayList<String>> getFieldannotationMap(Class<? extends Object> cls) {
        this.cls = cls;
        if (cls.getSuperclass() != null) {
            getFieldannotationMap(cls.getSuperclass());
        }
        for (Field field : cls.getDeclaredFields()) {

            String name = field.getName();
            Annotation[] annotations = field.getDeclaredAnnotations();
            for (Annotation ann : annotations) {
                String type = ann.annotationType().getName();

                if (annotationMap.containsKey(type)) {
                    annotationMap.get(type).add(name);
                } else {
                    annotationMap.put(type, new ArrayList<String>());
                    annotationMap.get(type).add(name);
                }
            }

        }

        return annotationMap;
    }

    /**
     * Retrieves the list of fields with the given annotation.
     * @param annotation the annotation to be evaluated
     * @return a ArrayList which contains all the fields that have the given annotation.
     */
    public List<String> getAnnotatedFields(String annotation) {
        List<String> annotatedFields = null;
        if (annotation == null) {
            return null;
        }
        if (annotation.startsWith("@")) {
            String type = annotation.substring(1);
            String annotationtype = "javax.jdo.annotations." + type;
            annotatedFields = annotationMap.get(annotationtype);
        } else {
            annotatedFields = annotationMap.get(annotation);
        }


        return annotatedFields;
    }

}
