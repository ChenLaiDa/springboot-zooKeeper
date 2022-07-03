package com.example.config;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.*;
import org.apache.zookeeper.data.Stat;

/**
 * @Description:
 * @Author: chenchong
 * @Date: 2022/6/26 16:00
 */
public class ZookeeperWatches {
    private CuratorFramework client;

    public ZookeeperWatches(CuratorFramework client) {
        this.client = client;
    }

    /**
     * 注册监听器 - 给指定的一个节点注册监听器
     * @throws Exception
     */
    public void znodeWatcher() throws Exception {
        //1.创建NodeCache对象
        NodeCache nodeCache = new NodeCache(client, "/node");
        //2.注册监听器
        nodeCache.getListenable().addListener(new NodeCacheListener() {
            @Override
            public void nodeChanged() throws Exception {
                System.out.println("=======节点改变===========");
                String path = nodeCache.getPath();
                String currentDataPath = nodeCache.getCurrentData().getPath();
                String currentData = new String(nodeCache.getCurrentData().getData());
                Stat stat = nodeCache.getCurrentData().getStat();
                System.out.println("path:"+path);
                System.out.println("currentDataPath:"+currentDataPath);
                System.out.println("currentData:"+currentData);
            }
        });
        //3.开启监听器
        nodeCache.start();

        System.out.println("节点监听注册完成");
    }

    /**
     * 注册监听器 - 给指定的一个节点的子节点注册监听器
     * @throws Exception
     */
    public void znodeChildrenWatcher() throws Exception {
        PathChildrenCache pathChildrenCache = new PathChildrenCache(client, "/node",true);

        pathChildrenCache.getListenable().addListener(new PathChildrenCacheListener() {
            @Override
            public void childEvent(CuratorFramework client, PathChildrenCacheEvent event) throws Exception {
                System.out.println("=======node节点的子节点改变===========");
                PathChildrenCacheEvent.Type type = event.getType();
                String childrenData = null;
                if(event.getData() != null && event.getData().getData() != null){
                    childrenData = new String(event.getData().getData());
                }
                String childrenPath = event.getData().getPath();
                Stat childrenStat = event.getData().getStat();
                System.out.println("子节点监听类型："+type);
                System.out.println("子节点路径："+childrenPath);
                System.out.println("子节点数据："+childrenData);
                System.out.println("子节点元数据："+childrenStat);
            }
        });
        pathChildrenCache.start();
        System.out.println("子节点监听注册完成");
    }

    /**
     * 注册监听器 - 给指定的一个节点和他的子节点注册监听器
     * @throws Exception
     */
    public void znodeTreeCacheWatcher() throws Exception {
        TreeCache treeCache = new TreeCache(client, "/node");
        treeCache.getListenable().addListener(new TreeCacheListener() {
            @Override
            public void childEvent(CuratorFramework client, TreeCacheEvent event) throws Exception {
                System.out.println("=======TreeCache监听===========");
                TreeCacheEvent.Type type = event.getType();
                String childrenData = null;
                if(event.getData() != null && event.getData().getData() != null){
                    childrenData = new String(event.getData().getData());
                }
                String childrenPath = event.getData().getPath();
                Stat childrenStat = event.getData().getStat();
                System.out.println("节点监听类型："+type);
                System.out.println("节点路径："+childrenPath);
                System.out.println("节点数据："+childrenData);
                System.out.println("节点元数据："+childrenStat);
            }
        });
        treeCache.start();
        System.out.println("节点监听注册完成");
    }

}
