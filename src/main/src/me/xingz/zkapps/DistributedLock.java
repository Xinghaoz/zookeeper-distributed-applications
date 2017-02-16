package me.xingz.zkapps;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.Map;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.CountDownLatch;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

public class DistributedLock implements Watcher {
	private ZooKeeper zk;
	private CountDownLatch semaphore = new CountDownLatch(1);
	private static final int SESSION_TIMEOUT = 5000;
	public static final String lockPath = "/lock";
	Integer myId;
	
	public DistributedLock() {
		
	}
	
	public DistributedLock(ZooKeeper zk) {
		this.zk = zk;
	}
	
	public DistributedLock(ZooKeeper zk, int myId) {
		this.zk = zk;
		this.myId = myId;
	}
	
	public void setMyId(int myId) {
		this.myId = myId;
	}
	
	private void connect(String hosts) throws IOException, InterruptedException {
		this.zk = new ZooKeeper(hosts, SESSION_TIMEOUT, this);
		semaphore.await();
	}
	
	private String createPath() throws KeeperException, InterruptedException {
		String path = lockPath + "/lock-" + zk.getSessionId() + "-";
		String result = zk.create(path, null, Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
		System.out.println("Created path: " + result);
		
		return result;
	}
	
	private void waitForLastNode() throws KeeperException, InterruptedException {
		List<String> children = zk.getChildren(lockPath, false);
		int previous = 0;
		String path = "";
		
		for (String child : children) {
			int id = Integer.parseInt(child.split("-")[2]);
			if (id < myId && id > previous) {
				previous = id;
				path = this.lockPath + "/" + child;
			}
		}
		
		System.out.println("[" + myId +"]" + " is listening to : " + path);
		Stat stat = zk.exists(path, new DistributedLock(zk, myId)); // Remember to pass zk and [myId] into the new instance!!!
//		System.out.println(stat);
	}
	
	private boolean getLock() throws KeeperException, InterruptedException {
		List<String> children = zk.getChildren(lockPath, null);
		System.out.println("Children: " + children);
		boolean isMin = true;
		for (String child: children) {
			int id = Integer.parseInt(child.split("-")[2]);
			if (id < myId) {
				isMin = false;
				break;
			}
		}
		
		if (isMin) {
			System.out.println(new Date() + ": [" + myId + "] get the lock!");
			return true;
		} else {
			System.out.println(new Date() + ": [" + myId + "] didn't get the lock :(");
			return false;
		}
		
	}

	@Override
	public void process(WatchedEvent event) {
		System.out.println("Received event " + event);
		if (event.getState() == KeeperState.SyncConnected) {
			if (EventType.None == event.getType() && null == event.getPath()) {
				semaphore.countDown();
				System.out.println("+++++++ Connected to server +++++++");
			} else if (event.getType() == EventType.NodeDeleted) {
				try {
					if (!getLock()) {
						waitForLastNode();
					} else {
//						System.out.println("I get the lock!");
					}
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		} 
		System.out.println();
	}
	
	public static void main(String[] args) throws Exception {
		DistributedLock lock = new DistributedLock();
		lock.connect("54.172.70.251:2181");
		
		String id = lock.createPath();
		int myId = Integer.parseInt(id.split("-")[2]);
		lock.setMyId(myId);
		
		if (!lock.getLock()) {
			lock.waitForLastNode();
		} else {
//			System.out.println("I get the lock!");
		}
		Thread.sleep(Integer.MAX_VALUE);
	}
}
