package com.practice.event

interface EventHandler {
    fun onMessage(messageEvent : MessageEvent)
}