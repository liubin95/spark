package com.liubin.iotest;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.Charset;

/**
 * io操作体现了装饰者模式.
 *
 * @author 刘斌
 * @version 0.0.1
 * @serial 2021-04-26 : base version.
 */
public class Main {

    public static void main(String[] args) throws IOException {
        final String fileName = "D:\\tmp\\input\\name.log";
        fileInputStreamRead(fileName);
        bufferedInputStreamRead(fileName);
        bufferedReaderRead(fileName);
    }

    /**
     * 读取文件的字节
     *
     * @param fileName 文件名
     * @throws IOException error
     */
    static void fileInputStreamRead(String fileName) throws IOException {
        final long now = System.currentTimeMillis();
        // 字节流，每次读取一个字节
        FileInputStream fileInputStream = new FileInputStream(fileName);
        int i;
        while ((i = fileInputStream.read()) != -1) {
            // 读取字节
            System.out.print((char) i);
        }
        fileInputStream.close();
        System.out.println();
        System.out.println(System.currentTimeMillis() - now);
    }

    /**
     * 读取文件的字节(带缓存区)
     *
     * @param fileName 文件名
     * @throws IOException error
     */
    static void bufferedInputStreamRead(String fileName) throws IOException {
        final long now = System.currentTimeMillis();
        // 缓存区
        byte[] bytes = new byte[2048];
        // 还是读取字节
        BufferedInputStream bufferedInputStream = new BufferedInputStream(new FileInputStream(fileName), bytes.length);
        while (bufferedInputStream.read(bytes) != -1) {
            // 读取字节
            System.out.print(new String(bytes));
        }
        bufferedInputStream.close();
        System.out.println();
        System.out.println(System.currentTimeMillis() - now);
    }

    /**
     * 读取字符
     *
     * @param fileName 文件名
     * @throws IOException error
     */
    static void bufferedReaderRead(String fileName) throws IOException {
        final long now = System.currentTimeMillis();
        // 读取字符，需要指定编码字符集，确定几个字节是一个字符
        final BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(new FileInputStream(fileName), Charset.defaultCharset()));

        String s;
        while ((s = bufferedReader.readLine()) != null) {
            // 读取一行
            System.out.println(s);
        }
        bufferedReader.close();
        System.out.println(System.currentTimeMillis() - now);
    }
}
