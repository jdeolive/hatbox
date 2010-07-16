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
package net.sourceforge.hatbox.jts;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;

import net.sourceforge.hatbox.Entry;
import net.sourceforge.hatbox.RTree;
import net.sourceforge.hatbox.RTreeDml;
import net.sourceforge.hatbox.RTreeSessionDb;

import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.io.ParseException;
import com.vividsolutions.jts.io.WKBReader;

/**
 * H2 trigger for deleting an entry from the RTree on delete from the spatial table. 
 * 
 * @author Peter Yuill, Justin Deoliveira
 */
public class DeleteTrigger extends AbstractTrigger {
    
    private String schema;
    private String table;

    public void fire(Connection con, Object[] oldRow, Object[] newRow) throws SQLException {
        RTreeDml dml = RTreeDml.createDml(con, schema, table);
        RTreeSessionDb session = new RTreeSessionDb(con, dml, true);
        RTree rTree = new RTree(session);
        int pkI = dml.getPkColumnIndex();
        int geomI = dml.getGeomColumnIndex();
        if (oldRow[geomI] != null) {
        	byte[] bytes;
        	try {
        		bytes = toBytes(oldRow[geomI]);
        	} catch (IOException e) {
        		throw (SQLException)
        		new SQLException("Failed to obtain geom for " + oldRow[pkI]).initCause(e);
        	}
            WKBReader reader = new WKBReader();
            Geometry geom = null;
            try {
                geom = reader.read(bytes);
            } catch (ParseException pe) {
                throw new SQLException("Failed to parse geom for " + oldRow[pkI]);
            }
            com.vividsolutions.jts.geom.Envelope e = geom.getEnvelopeInternal();
            Entry entry = new Entry(
                e.getMinX(), e.getMaxX(), e.getMinY(), e.getMaxY(), ((Number)oldRow[pkI]).longValue());
            rTree.delete(entry);
        }
        session.closeAll();
    }

    public void init(Connection con, String schema, String trigger, String table,
            boolean before, int type) throws SQLException {
        this.schema = schema;
        this.table = table;
    }

}
