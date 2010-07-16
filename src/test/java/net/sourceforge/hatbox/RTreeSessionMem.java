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

import java.sql.SQLException;
import java.util.HashMap;
import java.util.logging.Logger;

public class RTreeSessionMem implements RTreeSession {
    
	private static Logger logger = Logger.getLogger("net.sourceforge.hatbox");
    HashMap<Long,Node> nodes = new HashMap<Long,Node>();
    int nextId = 1;
    long rootId = 0;
    
    public RTreeSessionMem(int entriesMax) {
        Node root = new Node(0, -1, entriesMax);
        root.setId(getRootId());
        nodes.put(new Long(getRootId()), root);
    }

    public Node getNode(long id) throws SQLException {
        return nodes.get(new Long(id));
    }

    public long getRootId() {
        return rootId;
    }

    public void setRootId(long id) throws SQLException {
        rootId = id;
    }
    
    public void deleteNode(Node node) throws SQLException {
        nodes.remove(node.getId());
    }
    
    public void display() {
        logger.info("------ Root id = " + rootId);
        for (Node node : nodes.values()) {
        	logger.info(node.toString());
        }
        logger.info("******");
    }

	public long insertNode(Node node) throws SQLException {
        node.setId(nextId++);
        nodes.put(new Long(node.getId()), node);
        return node.getId();
	}

	public void updateNode(Node node) throws SQLException {
        nodes.put(new Long(node.getId()), node);
	}

}
