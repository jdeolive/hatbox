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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.sql.SQLException;

import org.h2.api.Trigger;

/**
 * Abstract superclass of H2 triggers. Derby triggers reference declared procedures
 * rather than implementing a specific Trigger interface.
 * 
 * @author Justin Deoliveira
 */
public abstract class AbstractTrigger implements Trigger {
	
	protected byte[] toBytes(Object o) throws IOException {
		byte[] bytes = null;
		if (o instanceof InputStream) {
		        InputStream in = (InputStream)o;
		        bytes = new byte[in.available()];
		        ByteArrayOutputStream out = new ByteArrayOutputStream(bytes.length);
		        int n = in.read(bytes);
		        while(n > 0){
		            out.write(bytes, 0, n);
		            n = in.read(bytes);
		        }
		        bytes = out.toByteArray();
		}
		else {
			bytes = (byte[])o;
		}
    
		return bytes;
	}

	public void close() throws SQLException {
	}

	public void remove() throws SQLException {
	}

}
