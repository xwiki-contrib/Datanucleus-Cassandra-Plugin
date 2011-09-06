package com.spidertracks.datanucleus.serialization;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

import com.spidertracks.datanucleus.collection.model.Card;
import com.spidertracks.datanucleus.collection.model.Pack;

public class JavaSerializerTest {

    @Test
    public void testConvert() {
        JavaSerializer serializer = new JavaSerializer();
        
        Pack pack = new Pack();

        Card aceSpades = new Card();
        aceSpades.setName("Ace of Spades");
        pack.addCard(aceSpades);

        Card jackHearts = new Card();
        jackHearts.setName("Jack of Hearts");
        pack.addCard(jackHearts);
        
        
        
        byte[] bytes = serializer.getBytes(pack);
        
        Pack returned = serializer.getObject(bytes);
        
        assertEquals(pack, returned);
        
        assertEquals(pack.getCards().get(0), aceSpades);
        assertEquals(pack.getCards().get(1), jackHearts);
    }

    

}
