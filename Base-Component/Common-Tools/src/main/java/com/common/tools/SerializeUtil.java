package com.common.tools;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

/**
 * 序列化工具类
 * 
 */
public class SerializeUtil {
	
	/**
	 * 将对象转换成二进制
	 * @param object
	 * @return  
	 * @throws 
	 */
	public static byte[] serialize(Object object) {
		ObjectOutputStream oos = null;
		ByteArrayOutputStream baos = null;
		byte[] bytes=null;
		try {
			// 序列化
			baos = new ByteArrayOutputStream();
			oos = new ObjectOutputStream(baos);
			oos.writeObject(object);
			bytes = baos.toByteArray();
			oos.flush();
		} catch (Exception e) {
			e.printStackTrace();
		}finally{
			if(null!=oos){
				try {
					oos.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
			if(null!=baos){
				try {
					baos.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
		return bytes;
	}

	/**
	 * 二进制转换成对象
	 * @param bytes
	 * @return  
	 * @throws 
	 */
	public static Object unserialize(byte[] bytes) {

		ByteArrayInputStream bais = null;
		ObjectInputStream ois =null;
		Object object=null;
		try {
			// 反序列化
			bais = new ByteArrayInputStream(bytes);
			ois = new ObjectInputStream(bais);
			object = ois.readObject();
			return object;
		} catch (Exception e) {
			e.printStackTrace();
		}finally{
			if(null!=ois){
				try {
					ois.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
			if(null!=bais){
				try {
					bais.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
		return object;
	}

}
