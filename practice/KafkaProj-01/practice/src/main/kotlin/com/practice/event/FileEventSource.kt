package com.practice.event

import org.slf4j.LoggerFactory
import java.io.File
import java.io.RandomAccessFile

class FileEventSource(
    private val updateInterval : Int,
    private val file : File,
    private val eventHandler: EventHandler
) : Runnable {


    private val keepRunning : Boolean = true
    private var filePointer : Long = 0

    companion object {
        private val logger = LoggerFactory.getLogger(FileEventSource::class.java)
    }


    override fun run() {

        try {
            while (keepRunning) {
                Thread.sleep(updateInterval.toLong())

                var len = file.length()

                if (len < filePointer) {
                    // 파일이 줄어들었을 때는 무시
                    logger.info("file was reset as file Pointer is longer than file")
                    filePointer = len
                    continue
                } else if (len > filePointer) {
                    readAppendAndSend()
                }

            }
        } catch (e: InterruptedException) {
            Thread.currentThread().interrupt()
        } catch (e: Exception) {
            e.printStackTrace()
        }
    }

    private fun readAppendAndSend() {
        var raf : RandomAccessFile = RandomAccessFile(file, "r")

        raf.seek(filePointer)

        var line: String? = null

        while (raf.readLine().also{ line = it} != null) {
            sendMessage(line)
        }

        filePointer = raf.filePointer
    }

    private fun sendMessage(line: String?) {
        if (line.isNullOrEmpty()) {
            return
        }

        val tokens = line.split(",")

        val key = tokens[0]
        val value = tokens.subList(1, tokens.size).joinToString(",")

        eventHandler.onMessage(MessageEvent(key, value))
//        logger.info("Sent message with key: {}, value: {}", key, value)
    }
}