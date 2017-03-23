package com.nextcentury.opensky;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectInputStream;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;
import java.io.Serializable;

/**
 *
 * 
 */
public class ByteSerializable implements Serializable {

    public ByteSerializable(){}

    public byte[] getAsBytes() throws IOException {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        ObjectOutput out = null;
        byte[] objBytes = null;
        try {
            out = new ObjectOutputStream(bos);
            out.writeObject(this);
            out.flush();
            objBytes = bos.toByteArray();
        } finally {
            try {
                bos.close();
            } catch (IOException ex) {
                // ignore close exception
            }
        }
        return objBytes;
    }

    static public ByteSerializable getFromBytes(byte[] objBytes) throws IOException, ClassNotFoundException {
        ByteArrayInputStream bis = new ByteArrayInputStream(objBytes);
        ObjectInput in = null;
        Object obj = null;
        try {
            in = new ObjectInputStream(bis);
            obj = in.readObject();
        } finally {
            try {
                if (in != null) {
                    in.close();
                }
            } catch (IOException ex) {
                // ignore close exception
            }
        }
        return (ByteSerializable)obj;
    }
}
