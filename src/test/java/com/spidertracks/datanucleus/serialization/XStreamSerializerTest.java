package com.spidertracks.datanucleus.serialization;

import static org.junit.Assert.assertEquals;

import javax.jdo.identity.StringIdentity;

import org.junit.Test;

import com.spidertracks.datanucleus.basic.model.Person;
import com.spidertracks.datanucleus.collection.model.Card;
import com.spidertracks.datanucleus.collection.model.Pack;

public class XStreamSerializerTest {

    @Test
    public void testConvert() {
        XStreamSerializer serializer = new XStreamSerializer();
        
        Pack pack = new Pack();

        Card aceSpades = new Card();
        aceSpades.setName("Ace of Spades");
        pack.addCard(aceSpades);
        aceSpades.setPack(null);
        
        
        Card jackHearts = new Card();
        jackHearts.setName("Jack of Hearts");
        pack.addCard(jackHearts);
        jackHearts.setPack(null);
        
        
        
        byte[] bytes = serializer.getBytes(pack);
        
        Pack returned = serializer.getObject(bytes);
        
        assertEquals(pack, returned);
        
        assertEquals(pack.getCards().get(0), aceSpades);
        assertEquals(pack.getCards().get(1), jackHearts);
    }
    
    
    @Test
    public void testConvertStringIdentity() {
        
        
        
        XStreamSerializer serializer = new XStreamSerializer();
        
        StringIdentity ident = new StringIdentity(Person.class, "12345678910");
        
        
        byte[] bytes = serializer.getBytes(ident);
        
        StringIdentity returned = serializer.getObject(bytes);
        
        assertEquals(ident, returned);
        
    }

    

}
