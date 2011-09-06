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
package com.spidertracks.datanucleus.map.model;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import javax.jdo.annotations.IdentityType;
import javax.jdo.annotations.Inheritance;
import javax.jdo.annotations.InheritanceStrategy;
import javax.jdo.annotations.PersistenceCapable;
import javax.jdo.annotations.Persistent;

import com.spidertracks.datanucleus.model.BaseEntity;

/**
 * An object with a map to many objects.  Indexed by Date
 * 
 * @author Todd Nine
 */
@PersistenceCapable(table = "PackMapDate", identityType = IdentityType.APPLICATION, detachable="true")
@Inheritance(strategy = InheritanceStrategy.NEW_TABLE)
public class PackMapDate extends BaseEntity {
    
    @Persistent(mappedBy = "pack")
    private Map<Date, CardMapDate> cards;

    public PackMapDate() {
        cards = new  HashMap<Date, CardMapDate>();
    }

    /**
     * @return the manyToOne
     */
    public Map<Date, CardMapDate> getCards() {
        return cards;
    }
    

    public void AddCard(CardMapDate card){
        this.cards.put(card.getTime(), card);
        card.setPack(this);
    }

}
