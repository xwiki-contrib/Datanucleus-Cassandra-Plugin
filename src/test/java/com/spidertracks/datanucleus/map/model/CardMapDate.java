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

import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.TimeZone;

import javax.jdo.annotations.IdentityType;
import javax.jdo.annotations.Inheritance;
import javax.jdo.annotations.InheritanceStrategy;
import javax.jdo.annotations.PersistenceCapable;
import javax.jdo.annotations.Persistent;

import com.spidertracks.datanucleus.model.BaseEntity;

/**
 * This object represents the "many" side of a one to many collection
 * 
 * @author Todd Nine
 * 
 */
@PersistenceCapable(table = "CardMapDate", identityType = IdentityType.APPLICATION, detachable="true")
@Inheritance(strategy = InheritanceStrategy.NEW_TABLE)
public class CardMapDate extends BaseEntity {

    private static final TimeZone UTC = TimeZone.getTimeZone("UTC");

    @Persistent
    private PackMapDate pack;

    private String name;

    private Date time;

    public CardMapDate(int year, int month, int date) {
        
        Calendar cal = new GregorianCalendar(UTC);
        cal.clear();
        cal.set(year, month, date);
        
        this.time = cal.getTime();
        
    }

    public Date getTime() {
        return time;
    }

    /**
     * @return the pack
     */
    public PackMapDate getPack() {
        return pack;
    }

    /**
     * @param pack
     *            the pack to set
     */
    public void setPack(PackMapDate pack) {
        this.pack = pack;
    }

    /**
     * @return the name
     */
    public String getName() {
        return name;
    }

    /**
     * @param name
     *            the name to set
     */
    public void setName(String name) {
        this.name = name;
    }

}
