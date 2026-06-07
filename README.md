# 🚀 Mqtt4j

**零依赖、面向虚拟线程的极轻量 MQTT 客户端**

[![Java](https://img.shields.io/badge/Java-21%2B-orange)](https://openjdk.org/projects/jdk/21/)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](LICENSE)
[![MQTT](https://img.shields.io/badge/MQTT-3.1.1%20%7C%205.0-green)](https://mqtt.org/)

---

Mqtt4j 是一个用纯 Java 从零实现的 MQTT 客户端库，专为 Java 21 虚拟线程（Virtual Threads）设计。

## ✨ 特性

- 🪶 **零运行时依赖** — 纯 JDK 实现，无 Netty、无 SLF4J、无任何第三方库
- 🧵 **虚拟线程原生** — 使用阻塞 I/O + 虚拟线程，一行代码万级并发
- 📡 **双协议支持** — MQTT v3.1.1 和 v5.0 完整支持
- 🔒 **TLS/SSL** — 内置 `SSLSocket` 加密传输
- 🌐 **WebSocket** — 支持 MQTT over WebSocket（ws:// 和 wss://）
- 🔄 **自动重连** — Exponential Backoff + Jitter 智能重连
- ⚡ **QoS 0/1/2** — 完整的消息质量等级支持
- 🏗️ **现代 Java** — sealed interface、record、pattern matching

## 📦 安装

### Maven

```xml
<dependency>
    <groupId>io.mqtt4j</groupId>
    <artifactId>mqtt4j</artifactId>
    <version>0.1.0-SNAPSHOT</version>
</dependency>
```

### 从源码构建

```bash
git clone https://github.com/mqtt4j/mqtt4j.git
cd mqtt4j
mvn clean install
```

## 🚀 快速开始

### 基础用法

```java
import io.mqtt4j.*;
import io.mqtt4j.message.*;

// 创建并连接
var client = new MqttClient(MqttClientConfig.builder()
    .host("broker.emqx.io")
    .port(1883)
    .clientId("my-device-001")
    .build());

client.connect();

// 订阅
client.subscribe("sensor/temperature", MqttQoS.AT_LEAST_ONCE,
    (topic, msg) -> System.out.println("温度: " + msg.payloadAsString()));

// 发布
client.publish("sensor/temperature", "26.5".getBytes(),
    MqttQoS.AT_LEAST_ONCE, false);

// 断开
client.disconnect();
```

### MQTT v5.0

```java
var client = new MqttClient(MqttClientConfig.builder()
    .host("broker.emqx.io")
    .v5()                    // 使用 MQTT v5.0
    .clientId("v5-device")
    .cleanSession(true)
    .keepAlive(30)
    .build());
```

### TLS 加密连接

```java
var client = new MqttClient(MqttClientConfig.builder()
    .host("broker.emqx.io")
    .ssl()                   // 自动切换到 8883 端口
    .build());
```

### WebSocket 传输

```java
var client = new MqttClient(MqttClientConfig.builder()
    .host("broker.emqx.io")
    .port(8083)
    .webSocket()             // MQTT over WebSocket
    .build());
```

### 遗嘱消息 (LWT)

```java
var client = new MqttClient(MqttClientConfig.builder()
    .host("broker.emqx.io")
    .clientId("device-001")
    .will("device/001/status", "offline")  // 设备离线时自动发布
    .build());
```

### 自动重连

```java
var client = new MqttClient(MqttClientConfig.builder()
    .host("broker.emqx.io")
    .autoReconnect(true)              // 默认开启
    .reconnectInitialDelay(1000)      // 初始延迟 1 秒
    .reconnectMaxDelay(30000)         // 最大延迟 30 秒
    .reconnectMaxRetries(-1)          // 无限重试
    .build());

client.onConnectionLost(cause ->
    System.out.println("连接丢失: " + cause.getMessage()));

client.onConnected(() ->
    System.out.println("已连接/重连成功"));
```

## 🧵 虚拟线程高并发

Mqtt4j 的核心卖点：**一行代码，万级并发连接**。

```java
// 10,000 个虚拟线程，每个独立连接一个 MQTT 客户端
try (var executor = Executors.newVirtualThreadPerTaskExecutor()) {
    for (int i = 0; i < 10_000; i++) {
        final int deviceId = i;
        executor.submit(() -> {
            var client = new MqttClient(MqttClientConfig.builder()
                .host("broker.emqx.io")
                .clientId("vt-device-" + deviceId)
                .keepAlive(30)
                .build());

            client.connect();
            client.subscribe("sensor/" + deviceId + "/data",
                MqttQoS.AT_LEAST_ONCE,
                (topic, msg) -> System.out.printf(
                    "Device %d: %s%n", deviceId, msg.payloadAsString()));
        });
    }
    // 所有 10,000 个连接在虚拟线程上高效运行
    // 仅消耗少量平台线程资源
}
```

### 为什么虚拟线程适合 MQTT？

| 传统方式 | Mqtt4j + 虚拟线程 |
|---------|-----------------|
| Netty/NIO + 回调 | 阻塞 Socket + 虚拟线程 |
| 回调地狱，调试困难 | 顺序逻辑，栈帧清晰 |
| 线程池限制并发数 | 百万级虚拟线程 |
| synchronized → 死锁风险 | ReentrantLock，无 pinning |

## 📊 架构

```
┌─────────────────────────────────────────┐
│              MqttClient                 │
│         (用户面向的高层 API)              │
├─────────────────────────────────────────┤
│  KeepAliveManager │ InflightManager     │
│  ReconnectManager │ PacketIdAllocator   │
│              MqttSession                │
├─────────────────────────────────────────┤
│      MqttPacketEncoder/Decoder          │
│   ConnectPacket │ PublishPacket │ ...    │
│        (sealed interface 体系)          │
├─────────────────────────────────────────┤
│           MqttTransport                 │
│   TcpTransport │ SslTransport │ WS     │
└─────────────────────────────────────────┘
```

## 🆚 与 Paho 对比

| 特性 | Eclipse Paho | Mqtt4j |
|-----|-------------|--------|
| 运行时依赖 | 有 | **零** |
| 最低 Java 版本 | 8 | 21 |
| 虚拟线程支持 | ❌ synchronized 导致 pinning | ✅ 原生适配 |
| MQTT v5.0 | ✅ | ✅ |
| 代码风格 | Java 8 回调 | 现代 Java（record、sealed） |
| WebSocket | 需要额外依赖 | ✅ JDK 内置 |
| 线程模型 | 内部线程池 + 锁 | 虚拟线程 + ReentrantLock |
| 代码量 | ~50K LOC | ~5K LOC |

## 📋 QoS 级别

| QoS | 名称 | 保证 | 流程 |
|-----|------|------|------|
| 0 | At Most Once | 最多一次 | PUBLISH → |
| 1 | At Least Once | 至少一次 | PUBLISH → PUBACK |
| 2 | Exactly Once | 恰好一次 | PUBLISH → PUBREC → PUBREL → PUBCOMP |

## 🔧 配置参考

```java
MqttClientConfig.builder()
    // 连接
    .host("localhost")           // 默认: localhost
    .port(1883)                  // 默认: 1883
    .connectTimeout(10000)       // 默认: 10s

    // 协议
    .version(MqttVersion.V3_1_1) // 默认: v3.1.1
    .clientId("my-client")       // 默认: 自动生成
    .cleanSession(true)          // 默认: true
    .keepAlive(60)               // 默认: 60s

    // 认证
    .username("user")
    .password("pass")

    // 传输
    .ssl()                       // TLS 加密
    .webSocket()                 // WebSocket
    .sslContext(mySSLContext)     // 自定义 SSL 上下文

    // 重连
    .autoReconnect(true)         // 默认: true
    .reconnectInitialDelay(1000) // 默认: 1s
    .reconnectMaxDelay(30000)    // 默认: 30s
    .reconnectMaxRetries(-1)     // 默认: 无限

    // QoS
    .maxInflight(65535)          // 默认: 65535
    .publishTimeout(30000)       // 默认: 30s
    .maxRetries(3)               // 默认: 3

    .build();
```

## 📄 License

[Apache License 2.0](LICENSE)

---

> **Mqtt4j** — 让 MQTT 回归简单。
