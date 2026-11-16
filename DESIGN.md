# 广播左外关联换路交接设计（DESIGN）

本轮只做分析与设计：不实现换路。文中"实测"均指对未改动的业务源码重放公开录制/回归得到的事实；"预测"均指对方案的手工推演，并用独立小规模参照 `sink.reference`（sink.py:19-27）核对最终多重集。基线输出不构成换路已实现的证据。

## 0. 结论摘要

比较两个同契约可行的交接设计：**设计甲（排空式交接）** 与 **设计乙（软切式交接）**。两者共用同一套配对空间划分、状态归属与未匹配判据，差别只在交付调度：甲在切面完成前先排空旧代配对余量，乙让旧代余量与新代输出并行交付。推荐**甲**：契约明确"没有额外的停顿时限、持续计算要求"（CONTRACT.md 第 11 行），乙唯一的收益是交付重叠的时延，而其代价——双代在途生命周期与并发接纳边界——正落在本次事故（重复交付、提前 null）最需要的可审计性上。

## 1. 现状调用链与源码锚点

- `Query.read_one`（query.py:17-35）从 `Source` 的单次迭代器拉取事件；源不可重开（source.py:8-11），`source.position` 单调记录已读事件数（source.py:16）。行身份 `{side,id,key,index,seq}` 在 source.py:34-35 固定，两侧行 ID 稳定、键可重复。
- 分发：右行广播给两个工作者、左行按左源序号奇偶路由（query.py:31-32）；END 广播给全部工作者（query.py:25-27）。
- `Worker.ingest` 为每个新到行建一个任务，任务上界 `stop` 在建立时固定（broadcast.py:14-22，注释见 broadcast.py:18）；任务扫描同键且更早到达的对侧行（broadcast.py:37）。
- 交付生命周期：`Query.offer`（query.py:37-43，单在途槽 `flights`，query.py:13）→ `Query.accept` 调 `Sink.accept`（query.py:45-50；接收器不可撤回、不去重，sink.py:6-8）→ `Query.ack` 才推进执行器游标（query.py:52-57；broadcast.py:52-61）。**accept 先于 ack**，回归 checks.py:38-48 钉住这一点。
- 未匹配门：仅当双 END 且本地任务排空时才提出 null 扩展（broadcast.py:41-49），游标 `null_pos`（broadcast.py:12）保证每左行至多一次；`matched`（broadcast.py:10）按左行 ID 记账，规模随左输入而非配对数。
- 完成：`Worker.done`（broadcast.py:63-67）含 null 扫描完成；`Query.finish` 循环至全部工作者完成（query.py:68-73）。
- `Sink.observation` 仅供实验观察，执行路径不得读回（sink.py:10-12）；`sink.canonical`（sink.py:15-16）为规范排序。

## 2. 问题形式化与事故机制定位

### 2.1 配对空间象限（计算覆盖的推导）

设切面为请求时刻的 `source.position`（source.py:16），已 `ingest` 的行为**旧代**（L_old、R_old），之后到达的为**新代**（L_new、R_new）。全部应有配对 = 等键行对 {(l,r)}，按代际分为四象限：

- **Q1 = L_old × R_old**：恰为旧工作者现有任务区间的并。每个等键旧对由后到行的那个任务产生恰好一次（上界在 ingest 时固定，broadcast.py:18-20；左行只存在于其奇偶属主，右行副本在另一工作者处扫不到它）。**排空旧任务 = 覆盖 Q1，且不重不漏。**
- **Q2 = L_old × R_new**：契约规定换路完成后新右行只去键所属工作者，故该工作者必须持有 L_old → **旧左行必须按键重洗进新计划**。
- **Q3 = L_new × R_old**：新左行只去键所属工作者，故该工作者必须持有 R_old → **旧右行必须按键重洗进新计划**。
- **Q4 = L_new × R_new**：新计划自然覆盖。

结论：任何同契约设计都必须把 L_old、R_old 按键重洗进新计划（源只扫一次，source.py:8-11，重洗只能读执行器本地行集 broadcast.py:5-6 或顺序溢写文件），且新计划**不得对旧×旧建任务**，否则 Q1 被重算。

### 2.2 两类事故在源码中的机制点

- **重复已交付配对**：若新右行继续走旧广播分支（query.py:31），或新计划对重洗来的旧代行建任务，Q1/Q2 会被二次产生；接收器不去重（sink.py:6-8），重复即事故。在途一致性同样关键：accept 先于 ack（query.py:45-56），已接纳配对的任务游标尚未推进（实测见 7.1：accepted_before_ack 请求点 r0 任务 `pos=0`）；切面若重置或绕过游标，将重交或丢失。
- **提前发布未匹配行**：换路后 END 仍广播（query.py:25-27），旧工作者再也收不到新右行，而 broadcast.py:41 的 null 门只检查双 END 与本地任务排空——门会满足，于是对"将来会被新右行匹配"的左行提前发 null。旧计划的 null 判据在换路后失效，必须冻结。

## 3. 交接不变量（两设计共用）

- **I1 配对恰好一次**：每个等键行对由恰好一个执行器的一个任务产生一次。划分规则：旧×旧 → 旧代任务（Q1）；其余三象限 → 新计划中新代行的任务。两代任务区间不相交、并为全集。重数由行身份（source.py:34-35）与桶位置枚举（broadcast.py:37）保持。
- **I2 接纳不可撤回**：凡经 accept 进入接收器的行不可撤回、不去重（sink.py:6-8）；切面不得让任何已接纳配对/null 被另一代再次产生。
- **I3 null 不提前**：左行 l 的 null 只在 R 完整（END R 已见）且 l.key 在完整 R 中无行后发布。
- **I4 账不随配对长**：查询侧状态 = O(输入行数) + O(工作者数)；无任何以已输出对数为键的账；`Sink.observation` 不被执行路径读取（sink.py:10-12）。
- **I5 源一次扫描**：不重开源（source.py:8-11）；停顿窗内新到事件只顺序落盘或暂缓拉取；重洗读的是执行器行集/溢写文件。

## 4. 设计甲：排空式交接（推荐）

### 4.1 状态归属

- **调度器**（query.py 同构）：`mode ∈ {BROADCAST, DRAINING, PARTITIONED}`、切面 `cut`（请求时的 source.position，source.py:16）、键→新工作者哈希（两侧同一函数）、在途槽 `flights`（query.py:13）、停顿窗溢写文件句柄。全部 O(工作者数)。
- **旧执行器**（broadcast.py 的 Worker）：行集与键索引（broadcast.py:5-6）、任务与游标（broadcast.py:7-8）、`matched`（broadcast.py:10）、`null_pos`（broadcast.py:12）。freeze 后行集封存、null 门关闭；只交付 Q1 余量，随后退役。
- **新执行器**（键分区工作者）：重洗来的旧代行（只入存储，不建任务）+ 新代行（建任务）；`E` = 旧代已接纳 null 的左行 ID 导出集（|E| ≤ |L_old|）；可删去 `matched`——null 判据改为键级，不依赖配对履历。
- **不保存**：任何随配对数增长的账（实测依据见 7.3：8 行输入产 16 对，任务仅 12 个）。

### 4.2 接纳切面

单一时点、原子切面。切面完成前，接收器接纳的全部是旧代 Q1 配对，走 query.py:37-57 原生命周期（含请求时已提出/已接纳的在途行先完成 accept/ack）；切面完成后，接纳的全部是新代输出。切面完成的机械判定：所有旧工作者 `head == len(tasks)`（broadcast.py:33）且 `pending` 为空（broadcast.py:9）且 `flights` 全空（query.py:13）且重洗装载完成。两代输出在接收器时间线上不重叠，审计只需检查该判定。

### 4.3 计算覆盖

旧代：Q1（排空既有任务区间，不建新任务、不收新行、不发 null）。新代：Q2∪Q3∪Q4（新代行的任务扫描"旧代重洗行 + 已到新代行"的同键桶）。并集 = 全部应有配对，交集为空（2.1）。

### 4.4 未匹配发布条件（统一转移规则）

- freeze 时旧工作者先完成在途生命周期（含在途 null 的 accept/ack，query.py:45-56），导出 `E` = 已 ack 的 null 左行 ID 集；此后旧工作者 null 门（broadcast.py:41）永久关闭。
- 新计划拥有全部其余 null（旧代与新代左行）：对键所属新工作者上的左行 l，当 **END L、END R 均见 ∧ 装载完成 ∧ 本地任务排空** 后，若 `l.id ∉ E` 且 **l.key 的右桶为空**，发布 `[l.id, null]`；`null_pos` 游标保证每行至多一次。
- 正确性：l 无匹配 ⟺ 完整 R 中无同键行 ⟺ 键所属工作者右桶为空——该工作者持有全部 R(k)（旧代重洗 + 新代路由），判据是键级的、本地的，无需旧计划的 `matched` 账，也无随配对数增长的账。`E` 保证已接纳的 null 不重发（right_ended_partial_null 锚点，7.2）。
- 备选（条件归属）：若请求时已见 END R，旧计划的 null 判据本已终局，可让其发完 L_old 的 null、新计划只管 L_new，省去 `E` 导出；但旧工作者须留存至 END L，且多一个分支情形。统一转移规则更简单，两设计均采用。

### 4.5 关键伪代码

```
# ===== 调度器（query.py 同构，新增 mode 与切面）====
def on_request():                    # 对应录制动作 ["request"]
    cut = source.position            # 切面：seq < cut 为旧代          (source.py:16)
    for w in old_workers: w.freeze() # 关 null 门；行集封存；任务上界本已固定 (broadcast.py:18-20)
    mode = DRAINING                  # read_one 暂停：拉式源自然等待 (source.py:8-11)
                                     # 若部署要求源持续排空：新到事件只顺序写临时文件

def drain_and_load():                # (a)(b) 可并行：装载只读封存行集
    # (a) 旧代交付：只对旧工作者跑 offer/accept/ack（query.py:37-57 原样）
    #     在途 flights 先完成（query.py:13）；null 门已冻结，只交付配对
    # (b) 重洗装载：旧行按键迁往新执行器，标记 OLD，不建任务
    #     for w in old_workers: for l in w.rows["L"]: N[hash(l.key)%W].load_old(l)  (broadcast.py:5)
    #     for r in old_workers[0].rows["R"]:        N[hash(r.key)%W].load_old(r)  # 广播副本相同，取一份
    #     内存受限时旧行按键顺序溢写临时文件再读入（契约允许顺序落盘）
    # 切面完成：所有旧工作者 head==len(tasks)（broadcast.py:33）∧ pending 全空（broadcast.py:9）
    #           ∧ flights 全空（query.py:13）∧ 装载完成
    E = union(w.export_nulls_acked() for w in old_workers)   # 含刚完成的在途 null
    mode = PARTITIONED               # 重放窗内溢写事件 + 恢复 read_one

def route(row):                      # 替换 query.py:31-32
    return [N[hash(row.key) % W]]    # 两侧同一哈希；新右行只去键所属工作者
# END 仍广播给全部新执行器（query.py:25-27 形状不变）；旧执行器退役后可弃
# finish 完成条件改为：旧代退役 ∧ 新代 done（query.py:68-73 的触点）
# 旧代退役 := head==len(tasks) ∧ pending is None，不再用 broadcast.py:63-67 的 done（其含 null 扫描）
```

```
# ===== 新执行器（键分区工作者，broadcast.py 同构裁剪）====
def load_old(row):                   # 旧代行：只入存储与键索引，不建任务 → Q1 不重算
    rows[row.side].append(row); by_key[row.side][row.key].append(row)

def ingest_new(row):                 # 新代行：与 broadcast.py:14-22 相同地建任务
    tasks.append({row, stop: len(by_key[other].get(row.key, [])), pos: 0})
    load_old(row)

def offer():                         # 任务扫描同 broadcast.py:27-40
    ...                              # null 门（替换 broadcast.py:41-49）：
    if ends == {"L","R"} and load_complete and head == len(tasks):
        while null_pos < len(rows["L"]):
            l = rows["L"][null_pos]
            if l.id in E or by_key["R"].get(l.key):  # 已发过 或 键有右行
                null_pos += 1; continue
            pending = [l.id, None]; return pending
# ack 同 broadcast.py:52-61；matched 集删除：null 判据不依赖配对履历
```

### 4.6 成本（记 L1/R1、L2/R2 为切面两侧输入量，P1..P4 为象限配对数，P 为总配对数）

- 停顿窗：从请求到切面完成，窗长 = max(Q1 余量交付时间, 重洗装载时间)；窗内到达事件 O(w) 顺序写+读各一次（或暂缓拉取，零 I/O）。
- 交付工作：P1 在窗内、P2+P3+P4 在窗后；配对枚举总量 O(P)，任何计划不可免。
- 空间：旧行集 O(L1+R1) 与新行集 O(L1+R1+L2+R2) 瞬态并存（可溢写）；任务每行一个 O(L+R)（broadcast.py:19）；`E` ⊆ L_old；调度器 O(W)。**无随 P 增长的账**（I4）。
- I/O：重洗 O(L1+R1) 读 + O(L1+R1) 写（内存足够时写可省）。

## 5. 设计乙：软切式交接（双代并行）

与甲共用 2.1 的覆盖划分、第 3 章不变量、4.4 的 null 规则与重洗装载。差别仅在交付调度：

- **接纳切面**：不是时点而是划分。请求后只等装载完成（不等 Q1 排空）即开闸；此后旧代继续交付 Q1 余量，新代并行交付 Q2∪Q3∪Q4，接收器任意前缀是两代输出的混合，不相交性由代际规则保证。
- **状态归属**：旧执行器状态（O(L1+R1)）须留存至其余量交付完才退役；调度器需双代在途槽——`flights` 按代扩展（query.py:13 的触点），offer/accept/ack 按代路由（query.py:37-57 的触点），finish 条件改为"旧代退役 ∧ 新代 done"（query.py:68-73 的触点），旧代退役同样需重定义 broadcast.py:63-67 的 done。
- **伪代码**：

```
def on_request():
    cut = source.position; freeze(old_workers); mode = LOADING   # read_one 暂停或溢写
    load_new_workers()                # 与甲 (b) 相同：旧行按键重洗，不建任务
    mode = DUAL                       # 装载完成即开闸：停顿窗只含 O(L1+R1) 装载
# DUAL 期间：
#   read_one 恢复，新行只去新执行器（route 同甲）；旧执行器不收新行、不发 null
#   旧执行器继续 offer/accept/ack 至任务排空后退役；新执行器正常 ingest_new/offer/null
#   flights 需 flights[gen][wid]；finish = 旧代退役 ∧ 新代 done
```

- **成本**：停顿窗缩短为 O(L1+R1) 装载（不再含 P1 余量交付）；交付总量 O(P) 不变；代价是旧状态更久留存与调度器双代生命周期。

## 6. 比较与推荐依据

| 维度 | 甲（排空式） | 乙（软切式） |
|---|---|---|
| 停顿窗 | max(P1 余量交付, O(L1+R1) 装载) | O(L1+R1) 装载 |
| 接纳切面 | 单一时点，机械判定（4.2） | 时间上交错的划分，需同审两代进度 |
| 调度器改动面 | 仅 mode/route/finish 条件；query.py:37-57 原样复用 | flights 按代分槽、生命周期按代路由、finish 双代条件 |
| 旧状态留存 | 切面完成即退役 | 留存至 P1 余量交付完 |
| 重复/提前风险面 | 两代不并发，事故类bug只剩代际规则一处 | 代际规则 + 双代在途交叉 |

推荐**甲**。契约无停顿时限与持续计算要求（CONTRACT.md 第 11 行），乙的交付重叠收益没有对价；而本次要防的两类事故都要求接纳边界尽量可审计，甲的原子切面把"是否重复、是否提前"的判定压缩到一个机械条件上。若未来引入时延目标，乙是自然的演进方向，其不变量与甲一致。

## 7. 按录制的实测与预测

### 7.1 实测锚点（`python3 observe.py examples/cases.json`，基线固定计划）

- **accepted_before_ack**：请求点（step 3）`source_position=4`、`counts={L:3,R:1}`；`accepted=[["l0","r0"]]`；`flights[0]={pair:["l0","r0"],accepted:true}`——已接纳但工作者 0 的 r0 任务 `pos=0/stop=1` 未推进；工作者 1 的 r0 任务 `stop=1` 尚未交付。钉住"accept 先于 ack"（另见 checks.py:38-48 回归）。
- **offered_not_accepted**：请求点（step 2）`accepted=[]`；`flights[0]={pair:["a","x"],accepted:false}`——已提出未接纳。
- **late_right_match**：请求点（step 1）`accepted=[]`，双工作者 `ends` 为空，`counts={L:1,R:1}`——R 远未结束。
- **right_ended_partial_null**：请求点（step 3）`accepted=[["n0",null]]`；`flights[0]={pair:["n0",null],accepted:true}`；工作者 0 `pending=["n0",null]`、`null_pos=0`（ack 才推进，broadcast.py:57）；双工作者 `ends=["L","R"]`——null 门已开且已发一半。

### 7.2 逐录制方案推演（设计甲，预测；最终多重集均以 sink.reference 核对）

- **accepted_before_ack**（切面在 position=4）：L_old={l0,l1,l2}，R_old={r0}，E=∅（未见 END）。DRAINING：在途 [l0,r0] 先 ack（r0 任务 pos→1，排空）；工作者 1 交付 [l1,r0]。重洗：l0,l1,r0→键7工作者，l2→键9工作者。切面完成后恢复读取：r1→键7工作者，任务扫 {l0,l1} → [l0,r1]、[l1,r1]；l3→键8工作者；r2→键8工作者 → [l3,r2]。双 END 后 l2 键9右桶为空 → [l2,null]。预测最终 = {[l0,r0],[l0,r1],[l1,r0],[l1,r1],[l2,null],[l3,r2]}，与参照一致；[l0,r0] 不重复——Q1 已由旧代交付，新计划对旧×旧不建任务。
- **offered_not_accepted**（切面在 position=3）：L_old={a,b}，R_old={x}。DRAINING：在途 [a,x] 完成 accept/ack；工作者 1 交付 [b,x]。重洗 a,b,x→键1工作者。恢复后 y→键1工作者 → [a,y]、[b,y]。预测最终 = {[a,x],[a,y],[b,x],[b,y]}，与参照一致。
- **late_right_match**（切面在 position=2）：L_old={u}，R_old={p}，Q1=∅（任务上界均 0），无在途。重洗 u→键4、p→键5。恢复后：v→键5工作者 → [v,p]（Q3）；q→键4工作者 → [u,q]（Q2，旧左行重洗是二者相遇的唯一途径——契约禁止 q 再广播）；w→键6工作者；END 后 w 键6右桶为空 → [w,null]。预测最终 = {[u,q],[v,p],[w,null]}，与参照一致。
- **right_ended_partial_null**（切面在 position=5，全部已读、双 END 已见）：Q1=∅；DRAINING 完成在途 null 的 ack，导出 E={n0}。重洗 n0,n1→键3工作者，z→键4工作者；源已尽，无新行。新计划 null 门：n0∈E 跳过；n1 键3右桶为空 → [n1,null]。预测最终 = {[n0,null],[n1,null]}，与参照一致。这是"已交付输出不得重发"的 null 版锚点：没有 E 导出，n0 的 null 会被新计划重发；也可见即使请求晚到双 END 之后，统一转移规则仍正确（调度器也可合法地放弃此次换路，正确性不依赖该判断）。

### 7.3 探针录制：配对数与元数据分离（实测）

`python3 observe.py examples/probe_pairs.json`（自增情形，4 左 + 4 右同键，请求在 read 5 之后）：最终接纳 **16** 行配对，而双工作者任务总数仅 **12**、`matched` 合计 **4** 个左行 ID。实测确认：元数据随输入线性、配对随输入平方；任何"每对一账"的设计在重复键下即破产，故两设计只保留每行/每任务游标（I4）。

## 8. 成本核算汇总

| 账 | 随输入量 | 随配对数 |
|---|---|---|
| 行存储/键索引（旧+新，可溢写） | O(L+R) | — |
| 任务与游标（每行一个，broadcast.py:19） | O(L+R) | — |
| `E` 导出集 / `matched`（旧代） | O(L) | — |
| 调度器（mode、cut、flights、哈希） | O(W) | — |
| 重洗 I/O | O(L1+R1) | — |
| 停顿窗溢写 | O(窗内到达数) | — |
| 配对枚举与交付（CPU/接收器） | — | O(P)（任何计划不可免） |
| 查询侧保存的每对账 | — | **0**（I4） |

## 9. 核验方式

- `python3 checks.py`：业务回归（录制重放对参照、2+2 内穷尽键与交错、accept 先于 ack、源不可重开）。实测通过：4 tests OK。
- `python3 observe.py examples/cases.json`：基线实测重放；`python3 observe.py examples/probe_pairs.json`：探针实测。
- `python3 review.py DESIGN.md`：重放本文证据、核对 predicted_final 与独立参照（sink.py:19-27）一致、校验源码行号。
- `python3 verify.py DESIGN.md`：先跑原回归再核验本文（`.pointwise-runtime.json` 的 verify 已接通为该命令）。
- 业务源码与基础回归未改动；换路未实现，本文所有"预测"均为方案推演。

## 10. 证据（机器可重放）

```json evidence
[
  {
    "case": "accepted_before_ack",
    "observed": {
      "case": "accepted_before_ack",
      "final": [
        [
          "l0",
          "r0"
        ],
        [
          "l0",
          "r1"
        ],
        [
          "l1",
          "r0"
        ],
        [
          "l1",
          "r1"
        ],
        [
          "l2",
          null
        ],
        [
          "l3",
          "r2"
        ]
      ],
      "reference": [
        [
          "l0",
          "r0"
        ],
        [
          "l0",
          "r1"
        ],
        [
          "l1",
          "r0"
        ],
        [
          "l1",
          "r1"
        ],
        [
          "l2",
          null
        ],
        [
          "l3",
          "r2"
        ]
      ],
      "trace": [
        {
          "accepted": [],
          "action": [
            "read",
            4
          ],
          "state": {
            "counts": {
              "L": 3,
              "R": 1
            },
            "exhausted": false,
            "flights": [
              null,
              null
            ],
            "source_position": 4,
            "workers": [
              {
                "ends": [],
                "head": 0,
                "left": [
                  "l0",
                  "l2"
                ],
                "matched": [],
                "null_pos": 0,
                "pending": null,
                "right": [
                  "r0"
                ],
                "tasks": [
                  {
                    "pos": 0,
                    "seq": 0,
                    "side": "L",
                    "stop": 0
                  },
                  {
                    "pos": 0,
                    "seq": 2,
                    "side": "L",
                    "stop": 0
                  },
                  {
                    "pos": 0,
                    "seq": 3,
                    "side": "R",
                    "stop": 1
                  }
                ],
                "worker": 0
              },
              {
                "ends": [],
                "head": 0,
                "left": [
                  "l1"
                ],
                "matched": [],
                "null_pos": 0,
                "pending": null,
                "right": [
                  "r0"
                ],
                "tasks": [
                  {
                    "pos": 0,
                    "seq": 1,
                    "side": "L",
                    "stop": 0
                  },
                  {
                    "pos": 0,
                    "seq": 3,
                    "side": "R",
                    "stop": 1
                  }
                ],
                "worker": 1
              }
            ]
          },
          "step": 0
        },
        {
          "accepted": [],
          "action": [
            "offer",
            0
          ],
          "state": {
            "counts": {
              "L": 3,
              "R": 1
            },
            "exhausted": false,
            "flights": [
              {
                "accepted": false,
                "pair": [
                  "l0",
                  "r0"
                ]
              },
              null
            ],
            "source_position": 4,
            "workers": [
              {
                "ends": [],
                "head": 2,
                "left": [
                  "l0",
                  "l2"
                ],
                "matched": [],
                "null_pos": 0,
                "pending": [
                  "l0",
                  "r0"
                ],
                "right": [
                  "r0"
                ],
                "tasks": [
                  {
                    "pos": 0,
                    "seq": 0,
                    "side": "L",
                    "stop": 0
                  },
                  {
                    "pos": 0,
                    "seq": 2,
                    "side": "L",
                    "stop": 0
                  },
                  {
                    "pos": 0,
                    "seq": 3,
                    "side": "R",
                    "stop": 1
                  }
                ],
                "worker": 0
              },
              {
                "ends": [],
                "head": 0,
                "left": [
                  "l1"
                ],
                "matched": [],
                "null_pos": 0,
                "pending": null,
                "right": [
                  "r0"
                ],
                "tasks": [
                  {
                    "pos": 0,
                    "seq": 1,
                    "side": "L",
                    "stop": 0
                  },
                  {
                    "pos": 0,
                    "seq": 3,
                    "side": "R",
                    "stop": 1
                  }
                ],
                "worker": 1
              }
            ]
          },
          "step": 1
        },
        {
          "accepted": [
            [
              "l0",
              "r0"
            ]
          ],
          "action": [
            "accept",
            0
          ],
          "state": {
            "counts": {
              "L": 3,
              "R": 1
            },
            "exhausted": false,
            "flights": [
              {
                "accepted": true,
                "pair": [
                  "l0",
                  "r0"
                ]
              },
              null
            ],
            "source_position": 4,
            "workers": [
              {
                "ends": [],
                "head": 2,
                "left": [
                  "l0",
                  "l2"
                ],
                "matched": [],
                "null_pos": 0,
                "pending": [
                  "l0",
                  "r0"
                ],
                "right": [
                  "r0"
                ],
                "tasks": [
                  {
                    "pos": 0,
                    "seq": 0,
                    "side": "L",
                    "stop": 0
                  },
                  {
                    "pos": 0,
                    "seq": 2,
                    "side": "L",
                    "stop": 0
                  },
                  {
                    "pos": 0,
                    "seq": 3,
                    "side": "R",
                    "stop": 1
                  }
                ],
                "worker": 0
              },
              {
                "ends": [],
                "head": 0,
                "left": [
                  "l1"
                ],
                "matched": [],
                "null_pos": 0,
                "pending": null,
                "right": [
                  "r0"
                ],
                "tasks": [
                  {
                    "pos": 0,
                    "seq": 1,
                    "side": "L",
                    "stop": 0
                  },
                  {
                    "pos": 0,
                    "seq": 3,
                    "side": "R",
                    "stop": 1
                  }
                ],
                "worker": 1
              }
            ]
          },
          "step": 2
        },
        {
          "accepted": [
            [
              "l0",
              "r0"
            ]
          ],
          "action": [
            "request"
          ],
          "state": {
            "counts": {
              "L": 3,
              "R": 1
            },
            "exhausted": false,
            "flights": [
              {
                "accepted": true,
                "pair": [
                  "l0",
                  "r0"
                ]
              },
              null
            ],
            "source_position": 4,
            "workers": [
              {
                "ends": [],
                "head": 2,
                "left": [
                  "l0",
                  "l2"
                ],
                "matched": [],
                "null_pos": 0,
                "pending": [
                  "l0",
                  "r0"
                ],
                "right": [
                  "r0"
                ],
                "tasks": [
                  {
                    "pos": 0,
                    "seq": 0,
                    "side": "L",
                    "stop": 0
                  },
                  {
                    "pos": 0,
                    "seq": 2,
                    "side": "L",
                    "stop": 0
                  },
                  {
                    "pos": 0,
                    "seq": 3,
                    "side": "R",
                    "stop": 1
                  }
                ],
                "worker": 0
              },
              {
                "ends": [],
                "head": 0,
                "left": [
                  "l1"
                ],
                "matched": [],
                "null_pos": 0,
                "pending": null,
                "right": [
                  "r0"
                ],
                "tasks": [
                  {
                    "pos": 0,
                    "seq": 1,
                    "side": "L",
                    "stop": 0
                  },
                  {
                    "pos": 0,
                    "seq": 3,
                    "side": "R",
                    "stop": 1
                  }
                ],
                "worker": 1
              }
            ]
          },
          "step": 3
        },
        {
          "accepted": [
            [
              "l0",
              "r0"
            ]
          ],
          "action": [
            "read",
            1
          ],
          "state": {
            "counts": {
              "L": 3,
              "R": 2
            },
            "exhausted": false,
            "flights": [
              {
                "accepted": true,
                "pair": [
                  "l0",
                  "r0"
                ]
              },
              null
            ],
            "source_position": 5,
            "workers": [
              {
                "ends": [],
                "head": 2,
                "left": [
                  "l0",
                  "l2"
                ],
                "matched": [],
                "null_pos": 0,
                "pending": [
                  "l0",
                  "r0"
                ],
                "right": [
                  "r0",
                  "r1"
                ],
                "tasks": [
                  {
                    "pos": 0,
                    "seq": 0,
                    "side": "L",
                    "stop": 0
                  },
                  {
                    "pos": 0,
                    "seq": 2,
                    "side": "L",
                    "stop": 0
                  },
                  {
                    "pos": 0,
                    "seq": 3,
                    "side": "R",
                    "stop": 1
                  },
                  {
                    "pos": 0,
                    "seq": 4,
                    "side": "R",
                    "stop": 1
                  }
                ],
                "worker": 0
              },
              {
                "ends": [],
                "head": 0,
                "left": [
                  "l1"
                ],
                "matched": [],
                "null_pos": 0,
                "pending": null,
                "right": [
                  "r0",
                  "r1"
                ],
                "tasks": [
                  {
                    "pos": 0,
                    "seq": 1,
                    "side": "L",
                    "stop": 0
                  },
                  {
                    "pos": 0,
                    "seq": 3,
                    "side": "R",
                    "stop": 1
                  },
                  {
                    "pos": 0,
                    "seq": 4,
                    "side": "R",
                    "stop": 1
                  }
                ],
                "worker": 1
              }
            ]
          },
          "step": 4
        },
        {
          "accepted": [
            [
              "l0",
              "r0"
            ]
          ],
          "action": [
            "ack",
            0
          ],
          "state": {
            "counts": {
              "L": 3,
              "R": 2
            },
            "exhausted": false,
            "flights": [
              null,
              null
            ],
            "source_position": 5,
            "workers": [
              {
                "ends": [],
                "head": 2,
                "left": [
                  "l0",
                  "l2"
                ],
                "matched": [
                  "l0"
                ],
                "null_pos": 0,
                "pending": null,
                "right": [
                  "r0",
                  "r1"
                ],
                "tasks": [
                  {
                    "pos": 0,
                    "seq": 0,
                    "side": "L",
                    "stop": 0
                  },
                  {
                    "pos": 0,
                    "seq": 2,
                    "side": "L",
                    "stop": 0
                  },
                  {
                    "pos": 1,
                    "seq": 3,
                    "side": "R",
                    "stop": 1
                  },
                  {
                    "pos": 0,
                    "seq": 4,
                    "side": "R",
                    "stop": 1
                  }
                ],
                "worker": 0
              },
              {
                "ends": [],
                "head": 0,
                "left": [
                  "l1"
                ],
                "matched": [],
                "null_pos": 0,
                "pending": null,
                "right": [
                  "r0",
                  "r1"
                ],
                "tasks": [
                  {
                    "pos": 0,
                    "seq": 1,
                    "side": "L",
                    "stop": 0
                  },
                  {
                    "pos": 0,
                    "seq": 3,
                    "side": "R",
                    "stop": 1
                  },
                  {
                    "pos": 0,
                    "seq": 4,
                    "side": "R",
                    "stop": 1
                  }
                ],
                "worker": 1
              }
            ]
          },
          "step": 5
        },
        {
          "accepted": [
            [
              "l0",
              "r0"
            ],
            [
              "l0",
              "r1"
            ],
            [
              "l1",
              "r0"
            ],
            [
              "l2",
              null
            ],
            [
              "l1",
              "r1"
            ],
            [
              "l3",
              "r2"
            ]
          ],
          "action": [
            "finish"
          ],
          "state": {
            "counts": {
              "L": 4,
              "R": 3
            },
            "exhausted": true,
            "flights": [
              null,
              null
            ],
            "source_position": 9,
            "workers": [
              {
                "ends": [
                  "L",
                  "R"
                ],
                "head": 5,
                "left": [
                  "l0",
                  "l2"
                ],
                "matched": [
                  "l0"
                ],
                "null_pos": 2,
                "pending": null,
                "right": [
                  "r0",
                  "r1",
                  "r2"
                ],
                "tasks": [
                  {
                    "pos": 0,
                    "seq": 0,
                    "side": "L",
                    "stop": 0
                  },
                  {
                    "pos": 0,
                    "seq": 2,
                    "side": "L",
                    "stop": 0
                  },
                  {
                    "pos": 1,
                    "seq": 3,
                    "side": "R",
                    "stop": 1
                  },
                  {
                    "pos": 1,
                    "seq": 4,
                    "side": "R",
                    "stop": 1
                  },
                  {
                    "pos": 0,
                    "seq": 6,
                    "side": "R",
                    "stop": 0
                  }
                ],
                "worker": 0
              },
              {
                "ends": [
                  "L",
                  "R"
                ],
                "head": 5,
                "left": [
                  "l1",
                  "l3"
                ],
                "matched": [
                  "l1",
                  "l3"
                ],
                "null_pos": 2,
                "pending": null,
                "right": [
                  "r0",
                  "r1",
                  "r2"
                ],
                "tasks": [
                  {
                    "pos": 0,
                    "seq": 1,
                    "side": "L",
                    "stop": 0
                  },
                  {
                    "pos": 1,
                    "seq": 3,
                    "side": "R",
                    "stop": 1
                  },
                  {
                    "pos": 1,
                    "seq": 4,
                    "side": "R",
                    "stop": 1
                  },
                  {
                    "pos": 0,
                    "seq": 5,
                    "side": "L",
                    "stop": 0
                  },
                  {
                    "pos": 1,
                    "seq": 6,
                    "side": "R",
                    "stop": 1
                  }
                ],
                "worker": 1
              }
            ]
          },
          "step": 6
        }
      ]
    },
    "predicted_final": [
      [
        "l0",
        "r0"
      ],
      [
        "l0",
        "r1"
      ],
      [
        "l1",
        "r0"
      ],
      [
        "l1",
        "r1"
      ],
      [
        "l2",
        null
      ],
      [
        "l3",
        "r2"
      ]
    ]
  },
  {
    "case": "offered_not_accepted",
    "observed": {
      "case": "offered_not_accepted",
      "final": [
        [
          "a",
          "x"
        ],
        [
          "a",
          "y"
        ],
        [
          "b",
          "x"
        ],
        [
          "b",
          "y"
        ]
      ],
      "reference": [
        [
          "a",
          "x"
        ],
        [
          "a",
          "y"
        ],
        [
          "b",
          "x"
        ],
        [
          "b",
          "y"
        ]
      ],
      "trace": [
        {
          "accepted": [],
          "action": [
            "read",
            3
          ],
          "state": {
            "counts": {
              "L": 2,
              "R": 1
            },
            "exhausted": false,
            "flights": [
              null,
              null
            ],
            "source_position": 3,
            "workers": [
              {
                "ends": [],
                "head": 0,
                "left": [
                  "a"
                ],
                "matched": [],
                "null_pos": 0,
                "pending": null,
                "right": [
                  "x"
                ],
                "tasks": [
                  {
                    "pos": 0,
                    "seq": 0,
                    "side": "L",
                    "stop": 0
                  },
                  {
                    "pos": 0,
                    "seq": 1,
                    "side": "R",
                    "stop": 1
                  }
                ],
                "worker": 0
              },
              {
                "ends": [],
                "head": 0,
                "left": [
                  "b"
                ],
                "matched": [],
                "null_pos": 0,
                "pending": null,
                "right": [
                  "x"
                ],
                "tasks": [
                  {
                    "pos": 0,
                    "seq": 1,
                    "side": "R",
                    "stop": 0
                  },
                  {
                    "pos": 0,
                    "seq": 2,
                    "side": "L",
                    "stop": 1
                  }
                ],
                "worker": 1
              }
            ]
          },
          "step": 0
        },
        {
          "accepted": [],
          "action": [
            "offer",
            0
          ],
          "state": {
            "counts": {
              "L": 2,
              "R": 1
            },
            "exhausted": false,
            "flights": [
              {
                "accepted": false,
                "pair": [
                  "a",
                  "x"
                ]
              },
              null
            ],
            "source_position": 3,
            "workers": [
              {
                "ends": [],
                "head": 1,
                "left": [
                  "a"
                ],
                "matched": [],
                "null_pos": 0,
                "pending": [
                  "a",
                  "x"
                ],
                "right": [
                  "x"
                ],
                "tasks": [
                  {
                    "pos": 0,
                    "seq": 0,
                    "side": "L",
                    "stop": 0
                  },
                  {
                    "pos": 0,
                    "seq": 1,
                    "side": "R",
                    "stop": 1
                  }
                ],
                "worker": 0
              },
              {
                "ends": [],
                "head": 0,
                "left": [
                  "b"
                ],
                "matched": [],
                "null_pos": 0,
                "pending": null,
                "right": [
                  "x"
                ],
                "tasks": [
                  {
                    "pos": 0,
                    "seq": 1,
                    "side": "R",
                    "stop": 0
                  },
                  {
                    "pos": 0,
                    "seq": 2,
                    "side": "L",
                    "stop": 1
                  }
                ],
                "worker": 1
              }
            ]
          },
          "step": 1
        },
        {
          "accepted": [],
          "action": [
            "request"
          ],
          "state": {
            "counts": {
              "L": 2,
              "R": 1
            },
            "exhausted": false,
            "flights": [
              {
                "accepted": false,
                "pair": [
                  "a",
                  "x"
                ]
              },
              null
            ],
            "source_position": 3,
            "workers": [
              {
                "ends": [],
                "head": 1,
                "left": [
                  "a"
                ],
                "matched": [],
                "null_pos": 0,
                "pending": [
                  "a",
                  "x"
                ],
                "right": [
                  "x"
                ],
                "tasks": [
                  {
                    "pos": 0,
                    "seq": 0,
                    "side": "L",
                    "stop": 0
                  },
                  {
                    "pos": 0,
                    "seq": 1,
                    "side": "R",
                    "stop": 1
                  }
                ],
                "worker": 0
              },
              {
                "ends": [],
                "head": 0,
                "left": [
                  "b"
                ],
                "matched": [],
                "null_pos": 0,
                "pending": null,
                "right": [
                  "x"
                ],
                "tasks": [
                  {
                    "pos": 0,
                    "seq": 1,
                    "side": "R",
                    "stop": 0
                  },
                  {
                    "pos": 0,
                    "seq": 2,
                    "side": "L",
                    "stop": 1
                  }
                ],
                "worker": 1
              }
            ]
          },
          "step": 2
        },
        {
          "accepted": [
            [
              "a",
              "x"
            ],
            [
              "b",
              "x"
            ],
            [
              "a",
              "y"
            ],
            [
              "b",
              "y"
            ]
          ],
          "action": [
            "finish"
          ],
          "state": {
            "counts": {
              "L": 2,
              "R": 2
            },
            "exhausted": true,
            "flights": [
              null,
              null
            ],
            "source_position": 6,
            "workers": [
              {
                "ends": [
                  "L",
                  "R"
                ],
                "head": 3,
                "left": [
                  "a"
                ],
                "matched": [
                  "a"
                ],
                "null_pos": 1,
                "pending": null,
                "right": [
                  "x",
                  "y"
                ],
                "tasks": [
                  {
                    "pos": 0,
                    "seq": 0,
                    "side": "L",
                    "stop": 0
                  },
                  {
                    "pos": 1,
                    "seq": 1,
                    "side": "R",
                    "stop": 1
                  },
                  {
                    "pos": 1,
                    "seq": 3,
                    "side": "R",
                    "stop": 1
                  }
                ],
                "worker": 0
              },
              {
                "ends": [
                  "L",
                  "R"
                ],
                "head": 3,
                "left": [
                  "b"
                ],
                "matched": [
                  "b"
                ],
                "null_pos": 1,
                "pending": null,
                "right": [
                  "x",
                  "y"
                ],
                "tasks": [
                  {
                    "pos": 0,
                    "seq": 1,
                    "side": "R",
                    "stop": 0
                  },
                  {
                    "pos": 1,
                    "seq": 2,
                    "side": "L",
                    "stop": 1
                  },
                  {
                    "pos": 1,
                    "seq": 3,
                    "side": "R",
                    "stop": 1
                  }
                ],
                "worker": 1
              }
            ]
          },
          "step": 3
        }
      ]
    },
    "predicted_final": [
      [
        "a",
        "x"
      ],
      [
        "a",
        "y"
      ],
      [
        "b",
        "x"
      ],
      [
        "b",
        "y"
      ]
    ]
  },
  {
    "case": "late_right_match",
    "observed": {
      "case": "late_right_match",
      "final": [
        [
          "u",
          "q"
        ],
        [
          "v",
          "p"
        ],
        [
          "w",
          null
        ]
      ],
      "reference": [
        [
          "u",
          "q"
        ],
        [
          "v",
          "p"
        ],
        [
          "w",
          null
        ]
      ],
      "trace": [
        {
          "accepted": [],
          "action": [
            "read",
            2
          ],
          "state": {
            "counts": {
              "L": 1,
              "R": 1
            },
            "exhausted": false,
            "flights": [
              null,
              null
            ],
            "source_position": 2,
            "workers": [
              {
                "ends": [],
                "head": 0,
                "left": [
                  "u"
                ],
                "matched": [],
                "null_pos": 0,
                "pending": null,
                "right": [
                  "p"
                ],
                "tasks": [
                  {
                    "pos": 0,
                    "seq": 0,
                    "side": "L",
                    "stop": 0
                  },
                  {
                    "pos": 0,
                    "seq": 1,
                    "side": "R",
                    "stop": 0
                  }
                ],
                "worker": 0
              },
              {
                "ends": [],
                "head": 0,
                "left": [],
                "matched": [],
                "null_pos": 0,
                "pending": null,
                "right": [
                  "p"
                ],
                "tasks": [
                  {
                    "pos": 0,
                    "seq": 1,
                    "side": "R",
                    "stop": 0
                  }
                ],
                "worker": 1
              }
            ]
          },
          "step": 0
        },
        {
          "accepted": [],
          "action": [
            "request"
          ],
          "state": {
            "counts": {
              "L": 1,
              "R": 1
            },
            "exhausted": false,
            "flights": [
              null,
              null
            ],
            "source_position": 2,
            "workers": [
              {
                "ends": [],
                "head": 0,
                "left": [
                  "u"
                ],
                "matched": [],
                "null_pos": 0,
                "pending": null,
                "right": [
                  "p"
                ],
                "tasks": [
                  {
                    "pos": 0,
                    "seq": 0,
                    "side": "L",
                    "stop": 0
                  },
                  {
                    "pos": 0,
                    "seq": 1,
                    "side": "R",
                    "stop": 0
                  }
                ],
                "worker": 0
              },
              {
                "ends": [],
                "head": 0,
                "left": [],
                "matched": [],
                "null_pos": 0,
                "pending": null,
                "right": [
                  "p"
                ],
                "tasks": [
                  {
                    "pos": 0,
                    "seq": 1,
                    "side": "R",
                    "stop": 0
                  }
                ],
                "worker": 1
              }
            ]
          },
          "step": 1
        },
        {
          "accepted": [],
          "action": [
            "read",
            2
          ],
          "state": {
            "counts": {
              "L": 2,
              "R": 2
            },
            "exhausted": false,
            "flights": [
              null,
              null
            ],
            "source_position": 4,
            "workers": [
              {
                "ends": [],
                "head": 0,
                "left": [
                  "u"
                ],
                "matched": [],
                "null_pos": 0,
                "pending": null,
                "right": [
                  "p",
                  "q"
                ],
                "tasks": [
                  {
                    "pos": 0,
                    "seq": 0,
                    "side": "L",
                    "stop": 0
                  },
                  {
                    "pos": 0,
                    "seq": 1,
                    "side": "R",
                    "stop": 0
                  },
                  {
                    "pos": 0,
                    "seq": 3,
                    "side": "R",
                    "stop": 1
                  }
                ],
                "worker": 0
              },
              {
                "ends": [],
                "head": 0,
                "left": [
                  "v"
                ],
                "matched": [],
                "null_pos": 0,
                "pending": null,
                "right": [
                  "p",
                  "q"
                ],
                "tasks": [
                  {
                    "pos": 0,
                    "seq": 1,
                    "side": "R",
                    "stop": 0
                  },
                  {
                    "pos": 0,
                    "seq": 2,
                    "side": "L",
                    "stop": 1
                  },
                  {
                    "pos": 0,
                    "seq": 3,
                    "side": "R",
                    "stop": 0
                  }
                ],
                "worker": 1
              }
            ]
          },
          "step": 2
        },
        {
          "accepted": [
            [
              "u",
              "q"
            ],
            [
              "v",
              "p"
            ],
            [
              "w",
              null
            ]
          ],
          "action": [
            "finish"
          ],
          "state": {
            "counts": {
              "L": 3,
              "R": 2
            },
            "exhausted": true,
            "flights": [
              null,
              null
            ],
            "source_position": 7,
            "workers": [
              {
                "ends": [
                  "L",
                  "R"
                ],
                "head": 4,
                "left": [
                  "u",
                  "w"
                ],
                "matched": [
                  "u"
                ],
                "null_pos": 2,
                "pending": null,
                "right": [
                  "p",
                  "q"
                ],
                "tasks": [
                  {
                    "pos": 0,
                    "seq": 0,
                    "side": "L",
                    "stop": 0
                  },
                  {
                    "pos": 0,
                    "seq": 1,
                    "side": "R",
                    "stop": 0
                  },
                  {
                    "pos": 1,
                    "seq": 3,
                    "side": "R",
                    "stop": 1
                  },
                  {
                    "pos": 0,
                    "seq": 5,
                    "side": "L",
                    "stop": 0
                  }
                ],
                "worker": 0
              },
              {
                "ends": [
                  "L",
                  "R"
                ],
                "head": 3,
                "left": [
                  "v"
                ],
                "matched": [
                  "v"
                ],
                "null_pos": 1,
                "pending": null,
                "right": [
                  "p",
                  "q"
                ],
                "tasks": [
                  {
                    "pos": 0,
                    "seq": 1,
                    "side": "R",
                    "stop": 0
                  },
                  {
                    "pos": 1,
                    "seq": 2,
                    "side": "L",
                    "stop": 1
                  },
                  {
                    "pos": 0,
                    "seq": 3,
                    "side": "R",
                    "stop": 0
                  }
                ],
                "worker": 1
              }
            ]
          },
          "step": 3
        }
      ]
    },
    "predicted_final": [
      [
        "u",
        "q"
      ],
      [
        "v",
        "p"
      ],
      [
        "w",
        null
      ]
    ]
  },
  {
    "case": "right_ended_partial_null",
    "observed": {
      "case": "right_ended_partial_null",
      "final": [
        [
          "n0",
          null
        ],
        [
          "n1",
          null
        ]
      ],
      "reference": [
        [
          "n0",
          null
        ],
        [
          "n1",
          null
        ]
      ],
      "trace": [
        {
          "accepted": [],
          "action": [
            "read",
            5
          ],
          "state": {
            "counts": {
              "L": 2,
              "R": 1
            },
            "exhausted": false,
            "flights": [
              null,
              null
            ],
            "source_position": 5,
            "workers": [
              {
                "ends": [
                  "L",
                  "R"
                ],
                "head": 0,
                "left": [
                  "n0"
                ],
                "matched": [],
                "null_pos": 0,
                "pending": null,
                "right": [
                  "z"
                ],
                "tasks": [
                  {
                    "pos": 0,
                    "seq": 0,
                    "side": "L",
                    "stop": 0
                  },
                  {
                    "pos": 0,
                    "seq": 2,
                    "side": "R",
                    "stop": 0
                  }
                ],
                "worker": 0
              },
              {
                "ends": [
                  "L",
                  "R"
                ],
                "head": 0,
                "left": [
                  "n1"
                ],
                "matched": [],
                "null_pos": 0,
                "pending": null,
                "right": [
                  "z"
                ],
                "tasks": [
                  {
                    "pos": 0,
                    "seq": 1,
                    "side": "L",
                    "stop": 0
                  },
                  {
                    "pos": 0,
                    "seq": 2,
                    "side": "R",
                    "stop": 0
                  }
                ],
                "worker": 1
              }
            ]
          },
          "step": 0
        },
        {
          "accepted": [],
          "action": [
            "offer",
            0
          ],
          "state": {
            "counts": {
              "L": 2,
              "R": 1
            },
            "exhausted": false,
            "flights": [
              {
                "accepted": false,
                "pair": [
                  "n0",
                  null
                ]
              },
              null
            ],
            "source_position": 5,
            "workers": [
              {
                "ends": [
                  "L",
                  "R"
                ],
                "head": 2,
                "left": [
                  "n0"
                ],
                "matched": [],
                "null_pos": 0,
                "pending": [
                  "n0",
                  null
                ],
                "right": [
                  "z"
                ],
                "tasks": [
                  {
                    "pos": 0,
                    "seq": 0,
                    "side": "L",
                    "stop": 0
                  },
                  {
                    "pos": 0,
                    "seq": 2,
                    "side": "R",
                    "stop": 0
                  }
                ],
                "worker": 0
              },
              {
                "ends": [
                  "L",
                  "R"
                ],
                "head": 0,
                "left": [
                  "n1"
                ],
                "matched": [],
                "null_pos": 0,
                "pending": null,
                "right": [
                  "z"
                ],
                "tasks": [
                  {
                    "pos": 0,
                    "seq": 1,
                    "side": "L",
                    "stop": 0
                  },
                  {
                    "pos": 0,
                    "seq": 2,
                    "side": "R",
                    "stop": 0
                  }
                ],
                "worker": 1
              }
            ]
          },
          "step": 1
        },
        {
          "accepted": [
            [
              "n0",
              null
            ]
          ],
          "action": [
            "accept",
            0
          ],
          "state": {
            "counts": {
              "L": 2,
              "R": 1
            },
            "exhausted": false,
            "flights": [
              {
                "accepted": true,
                "pair": [
                  "n0",
                  null
                ]
              },
              null
            ],
            "source_position": 5,
            "workers": [
              {
                "ends": [
                  "L",
                  "R"
                ],
                "head": 2,
                "left": [
                  "n0"
                ],
                "matched": [],
                "null_pos": 0,
                "pending": [
                  "n0",
                  null
                ],
                "right": [
                  "z"
                ],
                "tasks": [
                  {
                    "pos": 0,
                    "seq": 0,
                    "side": "L",
                    "stop": 0
                  },
                  {
                    "pos": 0,
                    "seq": 2,
                    "side": "R",
                    "stop": 0
                  }
                ],
                "worker": 0
              },
              {
                "ends": [
                  "L",
                  "R"
                ],
                "head": 0,
                "left": [
                  "n1"
                ],
                "matched": [],
                "null_pos": 0,
                "pending": null,
                "right": [
                  "z"
                ],
                "tasks": [
                  {
                    "pos": 0,
                    "seq": 1,
                    "side": "L",
                    "stop": 0
                  },
                  {
                    "pos": 0,
                    "seq": 2,
                    "side": "R",
                    "stop": 0
                  }
                ],
                "worker": 1
              }
            ]
          },
          "step": 2
        },
        {
          "accepted": [
            [
              "n0",
              null
            ]
          ],
          "action": [
            "request"
          ],
          "state": {
            "counts": {
              "L": 2,
              "R": 1
            },
            "exhausted": false,
            "flights": [
              {
                "accepted": true,
                "pair": [
                  "n0",
                  null
                ]
              },
              null
            ],
            "source_position": 5,
            "workers": [
              {
                "ends": [
                  "L",
                  "R"
                ],
                "head": 2,
                "left": [
                  "n0"
                ],
                "matched": [],
                "null_pos": 0,
                "pending": [
                  "n0",
                  null
                ],
                "right": [
                  "z"
                ],
                "tasks": [
                  {
                    "pos": 0,
                    "seq": 0,
                    "side": "L",
                    "stop": 0
                  },
                  {
                    "pos": 0,
                    "seq": 2,
                    "side": "R",
                    "stop": 0
                  }
                ],
                "worker": 0
              },
              {
                "ends": [
                  "L",
                  "R"
                ],
                "head": 0,
                "left": [
                  "n1"
                ],
                "matched": [],
                "null_pos": 0,
                "pending": null,
                "right": [
                  "z"
                ],
                "tasks": [
                  {
                    "pos": 0,
                    "seq": 1,
                    "side": "L",
                    "stop": 0
                  },
                  {
                    "pos": 0,
                    "seq": 2,
                    "side": "R",
                    "stop": 0
                  }
                ],
                "worker": 1
              }
            ]
          },
          "step": 3
        },
        {
          "accepted": [
            [
              "n0",
              null
            ],
            [
              "n1",
              null
            ]
          ],
          "action": [
            "finish"
          ],
          "state": {
            "counts": {
              "L": 2,
              "R": 1
            },
            "exhausted": true,
            "flights": [
              null,
              null
            ],
            "source_position": 5,
            "workers": [
              {
                "ends": [
                  "L",
                  "R"
                ],
                "head": 2,
                "left": [
                  "n0"
                ],
                "matched": [],
                "null_pos": 1,
                "pending": null,
                "right": [
                  "z"
                ],
                "tasks": [
                  {
                    "pos": 0,
                    "seq": 0,
                    "side": "L",
                    "stop": 0
                  },
                  {
                    "pos": 0,
                    "seq": 2,
                    "side": "R",
                    "stop": 0
                  }
                ],
                "worker": 0
              },
              {
                "ends": [
                  "L",
                  "R"
                ],
                "head": 2,
                "left": [
                  "n1"
                ],
                "matched": [],
                "null_pos": 1,
                "pending": null,
                "right": [
                  "z"
                ],
                "tasks": [
                  {
                    "pos": 0,
                    "seq": 1,
                    "side": "L",
                    "stop": 0
                  },
                  {
                    "pos": 0,
                    "seq": 2,
                    "side": "R",
                    "stop": 0
                  }
                ],
                "worker": 1
              }
            ]
          },
          "step": 4
        }
      ]
    },
    "predicted_final": [
      [
        "n0",
        null
      ],
      [
        "n1",
        null
      ]
    ]
  }
]
```
