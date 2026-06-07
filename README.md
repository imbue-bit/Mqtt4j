# 🚀 Mqtt4j

[![Java Version](https://img.shields.io/badge/Java-21%2B-orange)](https://openjdk.org/projects/jdk/21/)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](LICENSE)
[![MQTT Protocol](https://img.shields.io/badge/MQTT-3.1.1%20%7C%205.0-green)](https://mqtt.org/)

---

Mqtt4j 是一个使用纯 Java 实现的 MQTT 客户端库，专为 Java 21+ 虚拟线程设计。它不依赖于传统的反应式编程模型或复杂的异步回调，而是采用现代 Java 的阻塞 I/O 与虚拟线程相结合的方式，在保持代码逻辑简单、线性的同时，提供高并发连接能力。

## 设计初衷与核心优势

在 Java 21 引入虚拟线程后，传统的非阻塞 I/O + 反应式框架（如 Netty）在客户端开发中不再是高并发的唯一最优解。Mqtt4j 正是在这一背景下诞生的：

1. 避免载体线程钉死：传统的 MQTT 客户端（如 Eclipse Paho）大量使用 synchronized 关键字，这会导致虚拟线程在进行 Socket I/O 时钉死在 OS 线程上。Mqtt4j 内部全面采用 ReentrantLock 与并发工具类，确保虚拟线程能够正常调度和让出 CPU。
2. 零外部依赖：项目不引入 Netty、Jackson、SLF4J 等任何第三方库。这意味着更小的 Jar 包体积（约数百 KB）、更低的内存占用，以及完全免疫由于依赖冲突（Dependency Hell）引发的编译或运行时问题。
3. 现代 Java 特性集成：代码库全面采用 Java 21+ 的语言特性，包括 `sealed class/interface`（用于强类型报文解析）、`record`（用于不可变数据传输对象）、模式匹配以及 `HttpClient` WebSocket 实现。

---

## 特性

| 分类 | 支持特性 |
|---|---|
| 协议版本 | MQTT v3.1.1 与 v5.0 (完整支持属性、原因码等特性) |
| 传输通道 | TCP Socket、TLS/SSL (支持自定义 KeyStore)、WebSocket / Secure WebSocket |
| 服务质量 | QoS 0 (At most once), QoS 1 (At least once), QoS 2 (Exactly once) |
| 高可用性 | 具备抖动退避算法的自动重连机制 |
| 会话管理 | 离线消息缓存、飞行窗口控制 |
| 并发模型 | 内部读写循环完全运行于虚拟线程，无阻塞锁阻碍 |

---

## 📦 安装

### Maven

在您的 `pom.xml` 中添加以下依赖：

```xml
<dependency>
    <groupId>io.mqtt4j</groupId>
    <artifactId>mqtt4j</artifactId>
    <version>0.1.0</version>
</dependency>
```

### Gradle

在您的 `build.gradle` 中添加：

```groovy
implementation 'io.mqtt4j:mqtt4j:0.1.0'
```

---

## Quick Start

### 1. 基础发布与订阅 (MQTT v3.1.1)

```java
import io.mqtt4j.client.MqttClient;
import io.mqtt4j.client.MqttClientConfig;
import io.mqtt4j.model.MqttQoS;
import java.nio.charset.StandardCharsets;

public class QuickStart {
    public static void main(String[] args) {
        // 配置客户端
        MqttClientConfig config = MqttClientConfig.builder()
                .host("broker.emqx.io")
                .port(1883)
                .clientId("java-client-quickstart")
                .cleanSession(true)
                .build();

        // 使用 try-with-resources 自动管理资源
        try (MqttClient client = new MqttClient(config)) {
            // 同步建立连接
            client.connect();

            // 订阅主题
            client.subscribe("sensors/temperature", MqttQoS.AT_LEAST_ONCE, (topic, message) -> {
                String payload = new String(message.getPayload(), StandardCharsets.UTF_8);
                System.out.printf("收到主题 [%s] 的消息: %s%n", topic, payload);
            });

            // 发布消息
            client.publish("sensors/temperature", "23.5".getBytes(StandardCharsets.UTF_8), MqttQoS.AT_LEAST_ONCE);
            
            // 阻塞主线程以保持演示运行
            Thread.sleep(5000);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
```

### 2. TLS + MQTT v5.0

```java
MqttClientConfig config = MqttClientConfig.builder()
        .host("secure-broker.example.com")
        .port(8883)
        .v5() // 启用 MQTT 5.0
        .ssl() // 启用 TLS 加密，默认使用 JDK 信任证书链
        .username("admin")
        .password("secure_password_here")
        .will("status/offline", "device_disconnected".getBytes(), MqttQoS.AT_LEAST_ONCE, true) // 遗嘱消息
        .keepAlive(30)
        .build();
```

### 3. 基于 WebSocket 的连接

```java
MqttClientConfig config = MqttClientConfig.builder()
        .host("broker.emqx.io")
        .port(8084)
        .webSocket() // 使用内置的 WebSocket 适配层
        .ssl() // wss:// 协议
        .build();
```

---

## 虚拟线程下的并发表现

由于 Mqtt4j 移除了对平台线程池的依赖，在处理大规模连接时，可以直接利用虚拟线程执行并发任务，使每个连接的生命周期管理更贴近传统的顺序编程逻辑。

### 模拟万级并发客户端连接

```java
import io.mqtt4j.client.MqttClient;
import io.mqtt4j.client.MqttClientConfig;
import java.util.concurrent.Executors;

public class ScaleTest {
    public static void main(String[] args) {
        int clientCount = 10_000;
        
        // 创建一个基于虚拟线程的 Executor
        try (var executor = Executors.newVirtualThreadPerTaskExecutor()) {
            for (int i = 0; i < clientCount; i++) {
                final int index = i;
                executor.submit(() -> {
                    MqttClientConfig config = MqttClientConfig.builder()
                            .host("broker.emqx.io")
                            .clientId("client-vt-" + index)
                            .keepAlive(60)
                            .build();

                    try {
                        MqttClient client = new MqttClient(config);
                        client.connect();
                        
                        // 连接成功后，每个客户端可以独立进行读写工作，无需复杂的事件循环回调
                        client.subscribe("device/control/" + index, (topic, msg) -> {
                            // 业务处理逻辑
                        });
                    } catch (Exception e) {
                        // 异常处理
                    }
                });
            }
        } // 所有提交的虚拟线程任务在此处等待执行完毕
    }
}
```

### 为什么在虚拟线程环境下避免使用传统的异步客户端？

1.  Pinning 问题：在底层代码中使用 `synchronized` 方法或块时，若在该块内发生了 I/O 阻塞，虚拟线程将无法脱离其底层的操作系统载体线程（Carrier Thread），导致高并发优势失效。Mqtt4j 使用 `java.util.concurrent.locks.ReentrantLock` 替代了所有的同步锁。
2.  堆栈清晰度：当连接发生异常或数据解析出错时，Mqtt4j 的调用栈保持了从业务层到 Socket 读取层的完整同步链路，排查问题无需在异步回调或 Reactive Stream 的无序堆栈中进行回溯。

---

## 开源协议

本项目采用 [Apache License 2.0](LICENSE) 协议进行分发与使用。
