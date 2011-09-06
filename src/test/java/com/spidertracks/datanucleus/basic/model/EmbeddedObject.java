/**
 * 
 */
package com.spidertracks.datanucleus.basic.model;

import javax.jdo.annotations.IdentityType;
import javax.jdo.annotations.Inheritance;
import javax.jdo.annotations.InheritanceStrategy;
import javax.jdo.annotations.PersistenceCapable;

import com.spidertracks.datanucleus.model.BaseEntity;

/**
 * @author Todd Nine Basic object for testing primitive persistence. Only
 *         support types in java.lang
 * 
 */
@PersistenceCapable(table = "PrimitiveObject", identityType = IdentityType.APPLICATION)
@Inheritance(strategy = InheritanceStrategy.NEW_TABLE)
public class EmbeddedObject extends BaseEntity {

    @javax.jdo.annotations.Embedded
    private Embedded embedded = new Embedded();

    /**
     * @return the embedded
     */
    public Embedded getEmbedded() {
        return embedded;
    }

    /**
     * @param embedded
     *            the embedded to set
     */
    public void setEmbedded(Embedded embedded) {
        this.embedded = embedded;
    }

    @PersistenceCapable
    public static class Embedded {

    }
}
