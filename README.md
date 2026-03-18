# SeaTalk 语义搜索

解决 SeaTalk 只支持关键词搜索的局限。通过向量语义检索，用模糊描述也能找到历史聊天记录。

> 🤖 本项目所有代码均由 [Claude Code](https://claude.com/product/claude-code) 生成，无任何手写代码。

## 功能特性

- **语义搜索**：基于向量相似度，输入"上周讨论的部署方案"也能搜到相关消息
- **中英文支持**：embedding 模型原生支持中文和英文
- **本地运行**：模型和数据全部本地存储，无需联网，不上传任何聊天内容
- **自动采集**：启动时自动重启 SeaTalk（调试模式），采集完成后立即可搜索
- **增量同步**：已采集的消息不重复写入，Web 界面支持随时手动触发同步
- **群组过滤**：通过正则配置忽略指定群（默认过滤含"告警"的群）
- **分页展示**：搜索结果按时间倒序分页，显示会话名、发送者、时间、相关度

## 快速开始

### 前提条件

- macOS（SeaTalk 自动重启依赖 `osascript` / `open` 命令）
- Python 3.10+
- SeaTalk 桌面客户端已安装

### 安装依赖

```bash
pip install -r requirements.txt
```

首次运行时，sentence-transformers 会自动下载 embedding 模型（约 470 MB）。

### 启动

```bash
python main.py [-h] [-w] [-s] [--days N]
```

| 命令 | 说明 |
|------|------|
| `python main.py` | 完整流程：重启 SeaTalk → 采集 → 索引 → 启动 Web |
| `python main.py -w` | 仅启动 Web，跳过一切采集步骤（SeaTalk 无需运行） |
| `python main.py -s` | 跳过采集，对库中未索引消息补充向量化后启动 Web |
| `python main.py --days 7` | 临时采集最近 7 天，不修改配置文件 |
| `python main.py -h` | 查看完整帮助 |

**完整流程**（`python main.py`）依次执行：

1. 关闭已运行的 SeaTalk
2. 以 CDP 调试模式重新启动 SeaTalk
3. 等待你登录并确认（按 Enter）
4. 采集最近 N 天的聊天记录
5. 写入 SQLite，生成向量索引
6. 启动搜索服务：[http://127.0.0.1:12345](http://127.0.0.1:12345)

> **日常使用**：首次运行用完整流程完成初始采集，之后直接 `python main.py -w` 即可打开搜索界面，需要更新数据时再用完整流程或点击页面顶栏的"启动采集最新聊天记录"按钮。

## 配置说明

项目提供 `config.example.yaml` 作为配置模板，首次使用时复制并重命名：

```bash
cp config.example.yaml config.yaml
```

`config.yaml` 已加入 `.gitignore`，其中的 `ignore_group_patterns` 群名等个人/公司信息不会被提交。

所有配置项说明：

```yaml
seatalk:
  cdp_port: 9222               # CDP 远程调试端口
  time_range_days: 3           # 采集最近 N 天，默认 3 天
  max_messages_per_session: 5000
  ignore_group_patterns:       # 正则匹配，命中的群名将被忽略
    - "告警"

web:
  host: "127.0.0.1"
  port: 12345                  # Web 界面监听端口
  page_size: 20                # 每页结果数
  search_top_k: 500            # 向量检索候选数量
```

## 项目结构

```
seatalk_message_search/
├── main.py          # 入口
├── launcher.py      # SeaTalk 进程管理（关闭/重启/验证）
├── collector.py     # CDP 消息采集与内容解析
├── storage.py       # SQLite 持久化
├── indexer.py       # 向量索引与语义搜索（ChromaDB）
├── web.py           # FastAPI Web 服务
├── logger.py        # 双轨日志（终端 + 文件）
├── config.yaml      # 配置文件
├── requirements.txt
├── templates/
│   ├── index.html   # 搜索界面
│   └── stats.html   # 聊天记录采集统计界面
├── data/
│   ├── messages.sqlite3  # SQLite 数据库（自动生成）
│   └── chroma/      # ChromaDB 向量库（自动生成）
└── logs/
    └── app.log      # 详细日志（自动生成）
```

## 常见问题

**Q: 采集后搜索结果很少？**
适当增大 `time_range_days`。SeaTalk 本地只缓存已加载过的消息，若历史消息未在客户端浏览过则不会被采集到。

**Q: CDP 连接失败？**
确认 SeaTalk 已完全加载并登录，再按 Enter。若仍失败，检查 `cdp_port` 配置是否与实际端口一致。

**Q: 搜索结果相关度低？**
尝试换用更长、更具体的描述，避免使用单个词语搜索。

**Q: 想忽略更多群？**
在 `config.yaml` 的 `ignore_group_patterns` 下添加正则表达式，例如 `"^通知$"` 或 `"运营|市场"`。

## 技术原理

### 架构总览

```
┌─────────────────────────────────────────────────────────┐
│                        main.py                          │
│  Entry point: orchestrates the full pipeline            │
└──────┬──────────────────┬───────────────────────────────┘
       │                  │
       ▼                  ▼
┌──────────────┐   ┌─────────────────────────────────────┐
│  launcher.py │   │          collector.py               │
│              │   │                                     │
│ 关闭并重启   │   │  CDPHelper ─── WebSocket ──► SeaTalk│
│ SeaTalk，    │   │                                     │
│ 开启 CDP     │   │  向渲染进程注入 JavaScript           │
│ 调试模式     │   │  ┌──────────────────────────────┐   │
│ port=9222    │   │  │ store.getState()  (Redux)    │   │
└──────────────┘   │  │ sqlite.all(SQL)   (internal) │   │
                   │  └──────────────┬───────────────┘   │
                   │                 │                    │
                   │    List[Message]│                    │
                   └─────────────────┼────────────────────┘
                                     │
                                     ▼
                         ┌───────────────────────┐
                         │      storage.py        │
                         │                        │
                         │  SQLite (messages)     │
                         │  ┌──────────────────┐  │
                         │  │ id (mid)         │  │
                         │  │ session_id/name  │  │
                         │  │ sender_id/name   │  │
                         │  │ timestamp        │  │
                         │  │ content          │  │
                         │  │ indexed ◄── flag │  │
                         │  └──────────────────┘  │
                         └───────────┬────────────┘
                                     │ unindexed rows
                                     ▼
                         ┌───────────────────────┐
                         │      indexer.py        │
                         │                        │
                         │  SentenceTransformer   │
                         │  (384-dim embeddings)  │
                         │         │              │
                         │         ▼              │
                         │    ChromaDB            │
                         │  (cosine similarity)   │
                         └───────────┬────────────┘
                                     │
                                     ▼
                         ┌───────────────────────┐
                         │        web.py          │
                         │                        │
                         │  FastAPI + Jinja2      │
                         │                        │
                         │  GET /api/search       │
                         │    ┌────────────────┐  │
                         │    │ vector search  │  │
                         │    │    +           │  │
                         │    │ keyword search │  │
                         │    │ (jieba 分词)   │  │
                         │    │    = merged    │  │
                         │    │    score       │  │
                         │    └────────────────┘  │
                         │  POST /api/sync        │
                         │  GET  /api/stats       │
                         └───────────────────────┘
```

SeaTalk 基于 Electron 构建，内部的 SQLite 数据库通过渲染进程暴露的 `window.sqlite` 对象可直接查询。通过 Chrome DevTools Protocol（CDP）向渲染进程注入 JavaScript，即可在不解密数据库文件的前提下读取聊天记录。

### 关键对象：`sqlite` 与 `store`

在 SeaTalk 渲染进程的 JavaScript 运行环境中，有两个全局对象被本项目直接利用：

**`window.sqlite`**

SeaTalk 将本地消息存储在一个 SQLite 数据库中，并在渲染进程里暴露了一个封装好的 `sqlite` 异步查询对象。本项目通过它执行 SQL 查询来读取聊天消息：

```js
// 示例：查询某个会话最近的消息
const rows = await sqlite.all(
  `SELECT mid, u, ts, t, c FROM chat_message WHERE sid = 'group-xxx' ORDER BY ts DESC LIMIT 100`
);
```

`sqlite.all()` 返回查询结果数组，字段包括消息 ID（`mid`）、发送者 ID（`u`）、时间戳（`ts`）、消息类型（`t`）、消息内容（`c`）等。

**`window.store`**

SeaTalk 前端基于 Redux 架构，全局状态树通过 `store` 对象暴露。本项目通过 `store.getState()` 读取内存中已加载的群组信息和用户信息，用于将 ID 映射为可读的名称：

```js
const state = store.getState();
const groupInfo = state.contact.groupInfo;  // { groupId -> { name, ... } }
const userInfo  = state.contact.userInfo;   // { userId  -> { name, ... } }
```

由于 `sqlite` 里的消息只记录了 ID，群名和用户名需要从 `store` 的内存状态中查找补全，两者配合才能还原出完整的聊天记录结构。
