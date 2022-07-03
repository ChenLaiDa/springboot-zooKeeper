package com.example.controller;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.locks.InterProcessMutex;
import org.apache.curator.framework.recipes.locks.InterProcessReadWriteLock;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import javax.annotation.Resource;
import java.util.concurrent.TimeUnit;

/**
 * @Description:
 * @Author: chenchong
 * @Date: 2022/6/27 12:57
 */
@RestController
@RequestMapping("/lock")
public class LockController {
    @Resource
    private CuratorFramework client;

    @GetMapping("/InterProcessMutexUse")
    public String InterProcessMutexUse() {
        System.out.println("排它锁测试");
        InterProcessMutex lock = new InterProcessMutex(client, "/lock");
        System.out.println("占有锁中");
        try {
            lock.acquire(20L, TimeUnit.SECONDS);
            System.out.println("执行操作中");
            for (int i = 0; i < 20; i++) {
                TimeUnit.SECONDS.sleep(1);
                System.out.println(i);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }finally {
            try {
                lock.release();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        return "锁已释放";
    }

    @RequestMapping("/interProcessReadWriteLockUseWrite")
    public String interProcessReadWriteLockUseWrite() throws Exception {
        System.out.println("写锁");
        // 分布式读写锁
        InterProcessReadWriteLock lock = new InterProcessReadWriteLock(client, "/lock");
        // 开启两个进程测试，观察到写写互斥，特性同排它锁
        System.out.println("获取锁中");
        lock.writeLock().acquire();
        System.out.println("操作中");
        for (int i = 0; i < 10; i++) {
            TimeUnit.SECONDS.sleep(1);
            System.out.println(i);
        }
        lock.writeLock().release();
        return "释放写锁";
    }

    @RequestMapping("/interProcessReadWriteLockUseRead")
    public String interProcessReadWriteLockUseRead() throws Exception {
        System.out.println("读锁");
        // 分布式读写锁
        InterProcessReadWriteLock lock = new InterProcessReadWriteLock(client, "/lock");
        // 开启两个进程测试，观察得到读读共享，两个进程并发进行，注意并发和并行是两个概念，(并发是线程启动时间段不一定一致，并行是时间轴一致的)
        // 再测试两个进程，一个读，一个写，也会出现互斥现象
        System.out.println("获取锁中");
        lock.readLock().acquire();
        System.out.println("操作中");
        for (int i = 0; i < 10; i++) {
            TimeUnit.SECONDS.sleep(1);
            System.out.println(i);
        }
        lock.readLock().release();
        return "释放读锁";
    }
}
