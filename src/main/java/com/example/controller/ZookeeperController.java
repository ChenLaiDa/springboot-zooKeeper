package com.example.controller;

import com.alibaba.fastjson.JSONObject;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.api.BackgroundCallback;
import org.apache.curator.framework.api.CuratorEvent;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Id;
import org.apache.zookeeper.data.Stat;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import javax.annotation.Resource;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


/**
 * @Description:
 * @Author: chenchong
 * @Date: 2022/6/26 16:15
 */
@RestController
@RequestMapping("/zookeeper")
public class ZookeeperController {
    @Resource
    private CuratorFramework client;

    /**
     * 创建结点
     */
    @RequestMapping("/createZnode")
    public String createZnode(String path,@RequestParam(defaultValue = "")String data){
//        path = "/"+path;
        List<ACL> aclList = new ArrayList<>();
        Id id = new Id("world", "anyone");
        aclList.add(new ACL(ZooDefs.Perms.ALL, id));
        try {
            client.create()
                    .creatingParentsIfNeeded()  //没有父节点时 创建父节点
                    .withMode(CreateMode.PERSISTENT)  //节点类型
                    .withACL(aclList)   //配置权限
                    .forPath(path, data.getBytes());
        } catch (Exception e) {
            e.printStackTrace();
            return "节点创建失败"+e.getMessage();
        }
        return "节点创建成功";
    }

    @RequestMapping("/createAsyncZnode")
    public String createAsyncZnode(String path,@RequestParam(defaultValue = "")String data){
        String paths = "/"+path;
        try {
            client.create()
                    .creatingParentsIfNeeded()
                    .withACL(ZooDefs.Ids.OPEN_ACL_UNSAFE)
                    //异步回调   增删改都有异步方法
                    .inBackground(new BackgroundCallback() {
                        @Override
                        public void processResult(CuratorFramework client, CuratorEvent event) throws Exception {
                            System.out.println("异步回调--获取权限:"+client.getACL().forPath(paths));
                            System.out.println("异步回调--获取数据:"+new String(client.getData().forPath(paths)));
                            System.out.println("异步回调--获取事件名称:"+event.getName());
                            System.out.println("异步回调--获取事件类型:"+event.getType());
                        }
                    })
                    .forPath(paths, data.getBytes());
        } catch (Exception e) {
            e.printStackTrace();
            return "节点创建失败"+e.getMessage();
        }
        return "节点创建成功";
    }


    /**
     * 查看结点信息
     */
    @RequestMapping("/selectZnode")
    public JSONObject selectZnode(String path){
        JSONObject jsonObject = new JSONObject();
        Stat stat;
        try {
            stat = client.checkExists().forPath(path);
            if (stat == null) {
                jsonObject.put("error", "不存在该节点");
                return jsonObject;
            }
            String dataString = new String(client.getData().forPath(path));
            jsonObject.put(path, dataString);
            jsonObject.put("stat", stat);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return jsonObject;
    }

    /**
     * 查看子节点信息
     */
    @RequestMapping("/selectChildrenZnode")
    public Map<String,String> selectChildrenZnode(String path){
        Map<String, String> map = new HashMap<>();
        try {
            List<String> list = client.getChildren().forPath(path);
            for (String s : list) {
                String dataString = new String(client.getData().forPath(path+"/"+s));
                map.put(path+"/"+s, dataString);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

        return map;
    }


    /**
     * 更新结点信息
     */
    @RequestMapping("/setData")
    public JSONObject setData(String path,String data,Integer version) {
        JSONObject jsonObject = new JSONObject();
        try {
            Stat stat = client.setData().withVersion(version).forPath(path, data.getBytes());
            jsonObject.put("success", "修改成功");
            jsonObject.put("version", stat.getVersion());
        } catch (Exception e) {
            e.printStackTrace();
            jsonObject.put("error", "修改失败:"+e.getMessage());
            return jsonObject;
        }
        return jsonObject;
    }

    /**
     * 删除结点
     */
    @RequestMapping("/delete")
    public JSONObject delete(String path,Integer version,@RequestParam(defaultValue = "0")Integer isRecursive) {
        JSONObject jsonObject = new JSONObject();
        try {
            if (isRecursive == 1) {
                client.delete().deletingChildrenIfNeeded().withVersion(version).forPath(path);
            }else {
                client.delete().withVersion(version).forPath(path);
            }
            jsonObject.put("success", "删除成功");
        } catch (Exception e) {
            e.printStackTrace();
            jsonObject.put("error", "删除失败:"+e.getMessage());
            return jsonObject;
        }
        return jsonObject;
    }

    /**
     * 错误示范 - 不开启事务的情况下，抛出异常
     *
     */
    @SuppressWarnings("finally")
    @RequestMapping("/transactionDisabled")
    public String transactionDisabled(String createPath,String createData,String setPath,String setData) {

        try {
            //创建一个新的路径
            client.create().withMode(CreateMode.PERSISTENT).forPath(createPath,createData.getBytes());
            //修改一个没有的数据  让其报错
            client.setData().forPath(setPath, setData.getBytes());

        } catch (Exception e) {
            e.printStackTrace();
        }finally {
            return "执行完成";
        }

    }

    /**
     * 事务开启
     */
    @SuppressWarnings({ "deprecation", "finally" })
    @RequestMapping("/transactionEnabled")
    public String transactionEnabled(String createPath,String createData,String setPath,String setData) {
        try {
            client.inTransaction()
                    .create().withMode(CreateMode.PERSISTENT).forPath(createPath,createData.getBytes())
                    .and()
                    .create().withMode(CreateMode.PERSISTENT).forPath(createPath,createData.getBytes())
                    .and().commit();
        } catch (Exception e) {
            e.printStackTrace();
        }finally {
            return "执行完成";
        }

    }






}
