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
package com.spidertracks.datanucleus.array.model;

import javax.jdo.annotations.Element;
import javax.jdo.annotations.IdentityType;
import javax.jdo.annotations.Inheritance;
import javax.jdo.annotations.InheritanceStrategy;
import javax.jdo.annotations.PersistenceCapable;
import javax.jdo.annotations.Persistent;

import com.spidertracks.datanucleus.model.BaseEntity;

/**
 * An object with a collection to many objects
 * @author Todd Nine 
 */
@PersistenceCapable(table = "PackArray", identityType = IdentityType.APPLICATION)
@Inheritance(strategy = InheritanceStrategy.NEW_TABLE)
public class PackArray extends BaseEntity {
    
    
    @Persistent(mappedBy="pack")
    @Element(dependent="true")
    private CardArray[] cards;
    
    public PackArray(){
        cards = new CardArray[2];
    }

    /**
     * @return the manyToOne
     */
    public CardArray[] getCards() {
        return cards;
    }
    
    

    
}
