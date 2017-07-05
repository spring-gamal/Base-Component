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
import redis.clients.jedis.Pipeline;


/** 
 * @ClassName: Transaction 
 * @Description: TODO
 * @author: yong
 * @date 2012-12-19 下午04:21:59
 *  
 */
public abstract class PipelineExecute extends Pipeline{
	public PipelineExecute() {
	}
	public abstract void execute() throws Exception;
	public void setClient(Client client) {
		super.setClient(client);
	}
}

