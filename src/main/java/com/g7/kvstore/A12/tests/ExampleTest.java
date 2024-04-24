package com.g7.CPEN431.A12.tests;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class ExampleTest {
    @Test
    @DisplayName("Example Test Add")
    public void testAdd(){
        int val = 10 + 5;
        assertEquals(15, val);
    }
}
