/**  
 * @Project: EC-BaseComponent
 * @Title: Transaction.java
 * @Package org.lazicats.ecos.cache
 * @Description: TODO
 * @author: yong
 * @date 2012-12-19 下午04:21:59
 * @Copyright: BroadenGate Software Services Co.,Ltd. All rights reserved.
 * @version V1.0  
 */
package com.common.tools;

import redis.clients.jedis.Client;
import redis.clients.jedis.Transaction;


/** 
 * @ClassName: Transaction 
 * @Description: TODO
 *  
 */
public abstract class RedisTransactionExecute extends Transaction{
	
	public RedisTransactionExecute(Client client) {
		super(client);
	}

	public RedisTransactionExecute() {
	}

	public abstract void execute() throws Exception;

	public void setClient(Client client) {
		this.client = client;
	}
}

