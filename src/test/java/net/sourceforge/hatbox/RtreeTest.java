/*
 *    HatBox : A user-space spatial add-on for the Java databases
 *    
 *    Copyright (C) 2007 - 2009 Peter Yuill
 *
 *    This library is free software; you can redistribute it and/or
 *    modify it under the terms of the GNU Lesser General Public
 *    License as published by the Free Software Foundation;
 *    version 2.1 of the License.
 *
 *    This library is distributed in the hope that it will be useful,
 *    but WITHOUT ANY WARRANTY; without even the implied warranty of
 *    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 *    Lesser General Public License for more details.
 */
package net.sourceforge.hatbox;

import java.util.HashMap;
import org.junit.Test;
import junit.framework.Assert;

public class RtreeTest {
    
    @Test()
    public void simpleTest() throws Exception {
        // Use a simple in-memory session with a small entry size (3)
        // to ensure we can test all the node splitting behaviour with
        // minimum numbers of entries.
        RTreeSessionMem session = new RTreeSessionMem(3);
        RTree rtree = new RTree(session);
        rtree.setMinNodeSplit(0.7);
        Entry e = null;
        HashMap<Long,Entry> entries = new HashMap<Long,Entry>();
        // populate a sequence of point entries that should create a 
        // predictable three tier rtree demonstrating leaf splits,
        // a non-root index split and a root split.
        for (int i = 0; i < 12; i++) {
            e = new Entry();
            e.setId(i + 1000);
            e.populate(i, i, i, i);
            entries.put(e.getId(), e);
            rtree.insert(e);
        }
        session.display();    
        
        NodeData[] expectedArrayIns = new NodeData[] {
          new NodeData(5L,-1L,false,new Envelope(0.0,11.0,0.0,11.0),new Entry[] {
            new Entry(0.0,3.0,0.0,3.0,1L), new Entry(4.0,7.0,4.0,7.0,6L), new Entry(8.0,11.0,8.0,11.0,9L)}),
          new NodeData(1L,5L,false,new Envelope(0.0,3.0,0.0,3.0),new Entry[] {
            new Entry(0.0,1.0,0.0,1.0,0L), new Entry(2.0,3.0,2.0,3.0,2L)}),
          new NodeData(6L,5L,false,new Envelope(4.0,7.0,4.0,7.0),new Entry[] {
            new Entry(4.0,5.0,4.0,5.0,3L), new Entry(6.0,7.0,6.0,7.0,4L)}),
          new NodeData(9L,5L,false,new Envelope(8.0,11.0,8.0,11.0),new Entry[] {
            new Entry(8.0,9.0,8.0,9.0,7L), new Entry(10.0,11.0,10.0,11.0,8L)}),
          new NodeData(0L,1L,true,new Envelope(0.0,1.0,0.0,1.0),new Entry[] {
            new Entry(0.0,0.0,0.0,0.0,1000L), new Entry(1.0,1.0,1.0,1.0,1001L)}),
          new NodeData(2L,1L,true,new Envelope(2.0,3.0,2.0,3.0),new Entry[] {
            new Entry(2.0,2.0,2.0,2.0,1002L), new Entry(3.0,3.0,3.0,3.0,1003L)}),
          new NodeData(3L,6L,true,new Envelope(4.0,5.0,4.0,5.0),new Entry[] {
            new Entry(4.0,4.0,4.0,4.0,1004L), new Entry(5.0,5.0,5.0,5.0,1005L)}),
          new NodeData(4L,6L,true,new Envelope(6.0,7.0,6.0,7.0),new Entry[] {
            new Entry(6.0,6.0,6.0,6.0,1006L), new Entry(7.0,7.0,7.0,7.0,1007L)}),
          new NodeData(7L,9L,true,new Envelope(8.0,9.0,8.0,9.0),new Entry[] {
            new Entry(8.0,8.0,8.0,8.0,1008L), new Entry(9.0,9.0,9.0,9.0,1009L)}),
          new NodeData(8L,9L,true,new Envelope(10.0,11.0,10.0,11.0),new Entry[] {
            new Entry(10.0,10.0,10.0,10.0,1010L), new Entry(11.0,11.0,11.0,11.0,1011L)}),
        };
        for (NodeData expected : expectedArrayIns) {
            Node actual = session.getNode(expected.getId());
            Assert.assertNotNull("Node not found: " + expected.getId(), actual);
            Assert.assertEquals("Incorrect id: " + actual.getId() + " for: " + expected.getId(),
                    expected.getId(), actual.getId());
            Assert.assertEquals("Incorrect parent: " + actual.getParentId() + " for: " + expected.getId(),
                    expected.getParentId(), actual.getParentId());
            Assert.assertEquals("Incorrect Index/Leaf: " + actual.isLeaf() + " for: " + expected.getId(),
                    expected.isLeaf(), actual.isLeaf());
            Assert.assertEquals("Incorrect Bounds: " + actual.getBounds() + " for: " + expected.getId(),
                    expected.getBounds(), actual.getBounds());
            Assert.assertEquals("Wrong number of entries: " + actual.getEntriesCount() + " for: " + expected.getId(),
                    expected.getEntries().length, actual.getEntriesCount());
            for (Entry expectedE : expected.getEntries()) {
                boolean found = false;
                for (int i = 0; i < actual.getEntriesCount(); i++) {
                    if (expectedE.equals(actual.getEntry(i))) {
                        found = true;
                        break;
                    }
                }
                Assert.assertTrue("Expected entry not found: " + expectedE + " for: " + expected.getId(), found);
            }
        }
        
        rtree.delete(entries.get(1001L));
        session.display();    
        
        NodeData[] expectedArrayDel = new NodeData[] {
          new NodeData(5L,-1L,false,new Envelope(0.0,11.0,0.0,11.0),new Entry[] {
            new Entry(0.0,7.0,0.0,7.0,6L), new Entry(8.0,11.0,8.0,11.0,9L)}),
          new NodeData(6L,5L,false,new Envelope(0.0,7.0,0.0,7.0),new Entry[] {
              new Entry(0.0,3.0,0.0,3.0,2L), new Entry(4.0,5.0,4.0,5.0,3L), new Entry(6.0,7.0,6.0,7.0,4L)}),
          new NodeData(9L,5L,false,new Envelope(8.0,11.0,8.0,11.0),new Entry[] {
            new Entry(8.0,9.0,8.0,9.0,7L), new Entry(10.0,11.0,10.0,11.0,8L)}),
          new NodeData(2L,6L,true,new Envelope(0.0,3.0,0.0,3.0),new Entry[] {
              new Entry(0.0,0.0,0.0,0.0,1000L), new Entry(2.0,2.0,2.0,2.0,1002L), new Entry(3.0,3.0,3.0,3.0,1003L)}),
          new NodeData(3L,6L,true,new Envelope(4.0,5.0,4.0,5.0),new Entry[] {
            new Entry(4.0,4.0,4.0,4.0,1004L), new Entry(5.0,5.0,5.0,5.0,1005L)}),
          new NodeData(4L,6L,true,new Envelope(6.0,7.0,6.0,7.0),new Entry[] {
            new Entry(6.0,6.0,6.0,6.0,1006L), new Entry(7.0,7.0,7.0,7.0,1007L)}),
          new NodeData(7L,9L,true,new Envelope(8.0,9.0,8.0,9.0),new Entry[] {
            new Entry(8.0,8.0,8.0,8.0,1008L), new Entry(9.0,9.0,9.0,9.0,1009L)}),
          new NodeData(8L,9L,true,new Envelope(10.0,11.0,10.0,11.0),new Entry[] {
            new Entry(10.0,10.0,10.0,10.0,1010L), new Entry(11.0,11.0,11.0,11.0,1011L)})
        };
        for (NodeData expected : expectedArrayDel) {
            Node actual = session.getNode(expected.getId());
            Assert.assertNotNull("Node not found: " + expected.getId(), actual);
            Assert.assertEquals("Incorrect id: " + actual.getId() + " for: " + expected.getId(),
                    expected.getId(), actual.getId());
            Assert.assertEquals("Incorrect parent: " + actual.getParentId() + " for: " + expected.getId(),
                    expected.getParentId(), actual.getParentId());
            Assert.assertEquals("Incorrect Index/Leaf: " + actual.isLeaf() + " for: " + expected.getId(),
                    expected.isLeaf(), actual.isLeaf());
            Assert.assertEquals("Incorrect Bounds: " + actual.getBounds() + " for: " + expected.getId(),
                    expected.getBounds(), actual.getBounds());
            Assert.assertEquals("Wrong number of entries: " + actual.getEntriesCount() + " for: " + expected.getId(),
                    expected.getEntries().length, actual.getEntriesCount());
            for (Entry expectedE : expected.getEntries()) {
                boolean found = false;
                for (int i = 0; i < actual.getEntriesCount(); i++) {
                    if (expectedE.equals(actual.getEntry(i))) {
                        found = true;
                        break;
                    }
                }
                Assert.assertTrue("Expected entry not found: " + expectedE + " for: " + expected.getId(), found);
            }
        }
        
    }
    
    public static class NodeData {
        private long id;
        private long parentId;
        private boolean leaf;
        Envelope bounds;
        private Entry[] entries;
        public NodeData(long id, long parentId, boolean leaf, Envelope bounds, Entry[] entries) {
            this.id = id;
            this.parentId = parentId;
            this.leaf = leaf;
            this.bounds = bounds;
            this.entries = entries;
        }
        public Envelope getBounds() {
            return bounds;
        }
        public Entry[] getEntries() {
            return entries;
        }
        public long getId() {
            return id;
        }
        public long getParentId() {
            return parentId;
        }
        public boolean isLeaf() {
            return leaf;
        }
    }

}
