/**********************************************************************
Copyright (c) 2010 Todd Nine. All rights reserved.
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
package com.spidertracks.datanucleus.serialization;

import java.lang.instrument.ClassFileTransformer;
import java.lang.instrument.IllegalClassFormatException;
import java.lang.instrument.Instrumentation;
import java.security.ProtectionDomain;

/**
 * Class that is used to estimate the size of an object for serialization
 * @author Todd Nine
 *
 */
public class ByteSizeEstimator  implements ClassFileTransformer {
	private static Instrumentation instrumentation;
	
	/**
	 * 
	 * @param args
	 * @param inst
	 */
	public static void agentmain(String args, Instrumentation inst) {
        instrumentation = inst;
    }
	
	public static void premain(String args, Instrumentation inst){
		agentmain(args, inst);
	}

	/**
	 * Get the approximate size in memory of an object
	 * @param o
	 * @return
	 */
    public static long getObjectSize(Object o) {
        return instrumentation.getObjectSize(o);
    }

	@Override
	public byte[] transform(ClassLoader loader, String className,
			Class<?> classBeingRedefined, ProtectionDomain protectionDomain,
			byte[] classfileBuffer) throws IllegalClassFormatException {
		// TODO Auto-generated method stub
		return null;
	}
}