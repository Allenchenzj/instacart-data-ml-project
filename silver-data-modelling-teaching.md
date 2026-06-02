# Silver Layer 数据建模教学指南

## 为什么数据建模很重要？

先看一个对比，体会建模前后的差异。

**没有建模——直接查原始表：**

```sql
SELECT 
    p.product_name,
    d.department,
    COUNT(*) AS total_sold
FROM order_products op
JOIN orders o ON op.order_id = o.order_id
JOIN products p ON op.product_id = p.product_id
JOIN aisles a ON p.aisle_id = a.aisle_id
JOIN departments d ON p.department_id = d.department_id
GROUP BY p.product_name, d.department
-- 问题：5张表 JOIN，逻辑复杂，性能差，容易出错
```

**有建模——查 Silver 层：**

```sql
SELECT 
    product_name,
    department_name,
    COUNT(*) AS total_sold
FROM fact_order_products f
JOIN dim_products p ON f.product_id = p.product_id
GROUP BY product_name, department_name
-- 优势：2张表 JOIN，逻辑清晰，性能好
```

**Silver 层是核心**：它是原始数据和业务分析之间的桥梁。建模质量直接决定下游分析的效率。

## Bronze 层数据概览

我们的 Bronze 层有 5 张原始表：

| 表名 | 记录数（约） | 说明 |
|------|-------------|------|
| orders | 3.4M | 订单主表 |
| products | 50K | 产品主数据 |
| aisles | 134 | 货架分类 |
| departments | 21 | 部门分类 |
| order_products | 32M | 订单-产品明细 |


**Bronze 层有几个明显的问题：**

| 问题 | 说明 |
|------|------|
| 多层级结构 | products → aisles → departments 是雪花型，查询需要多次 JOIN |
| 没有用户表 | user_id 散落在 orders 表中，无法直接分析用户特征 |
| 缺少预计算指标 | 每次查询都要实时计算订单产品数、复购比例等 |
| fact_order_products 没有 user_id | 要知道谁买了什么，还得再 JOIN orders |


## 建模前的三个关键问题

在开始建模之前，先问自己这三个问题：

### 哪些表记录的是"业务事件"？

| 表名 | 是否记录事件？ | 说明 |
|------|---------------|------|
| orders | 是 | 记录"用户下单"这个事件 |
| order_products | 是 | 记录"用户购买某产品"这个事件 |
| products | 否 | 描述产品属性，静态数据 |
| aisles | 否 | 静态分类数据 |
| departments | 否 | 静态分类数据 |

记录业务事件的表，在维度建模里叫**事实表（Fact Table）**。
描述属性的表，叫**维度表（Dimension Table）**。

### 哪些字段是"度量值"（可以做聚合计算）？

| 字段 | 所属表 | 是否度量？ |
|------|--------|-----------|
| days_since_prior_order | orders | 是，可以算平均复购间隔 |
| add_to_cart_order | order_products | 是，可以算平均购物车位置 |
| reordered | order_products | 是，可以算复购率 |
| product_name | products | 否，文本属性，不能聚合 |

### 业务想回答什么问题？

1. 用户的复购周期是多少？→ 需要 orders
2. 哪些产品复购率最高？→ 需要 order_products + products
3. 用户一般什么时间下单？→ 需要 orders
4. 哪些产品经常一起购买？→ 需要 order_products（购物篮分析）

## 维度建模核心概念

### 事实表（Fact Table）

记录"发生了什么"——业务事件。

- 包含**外键**（指向维度表）
- 包含**度量值**（可聚合的数值）
- 数据量大，持续增长

### 维度表（Dimension Table）

描述"是什么"——业务实体的属性。

- 提供分析的"角度"（按产品、按用户、按时间切片）
- 数据量相对小
- 变化频率低

## 各表详细设计

### fact_orders（订单事实表）

**粒度**：每个 order_id 一行，代表一次完整的下单。

| 字段 | 类型 | 来源 | 说明 |
|------|------|------|------|
| order_id | INT | orders | 主键 |
| user_id | INT | orders | 外键 → dim_users |
| eval_set | STRING | orders | prior / train / test |
| order_number | INT | orders | 该用户第几次下单 |
| order_dow | INT | orders | 星期几（0-6） |
| order_hour_of_day | INT | orders | 小时（0-23） |
| days_since_prior_order | FLOAT | orders | 距上次下单天数 |
| total_products | INT | **预计算** | 该订单购买了多少种产品 |
| total_reordered | INT | **预计算** | 其中多少是复购 |
| reorder_ratio | FLOAT | **预计算** | total_reordered / total_products |

重点是后三个预计算字段，它们在 ETL 时就算好了，查询时不需要再 JOIN order_products。

### fact_order_products（订单产品事实表）

**粒度**：每个 order_id + product_id 的组合一行，代表"某次订单里买了某个产品"。

| 字段 | 类型 | 来源 | 说明 |
|------|------|------|------|
| order_id | INT | order_products | 外键 → fact_orders |
| product_id | INT | order_products | 外键 → dim_products |
| user_id | INT | **从 orders 关联进来** | 直接查用户不用再 JOIN |
| add_to_cart_order | INT | order_products | 加购顺序（第几个放进购物车） |
| reordered | INT | order_products | 是否复购（0 / 1） |


### dim_products（产品维度表）

**设计原则：反规范化（Denormalization）**

Bronze 层的 products、aisles、departments 是三张独立的表（雪花型）。我们把它们合并成一张扁平的维度表：

| 字段 | 类型 | 来源 |
|------|------|------|
| product_id | INT | products（主键） |
| product_name | STRING | products |
| aisle_id | INT | products |
| aisle_name | STRING | aisles（JOIN 进来） |
| department_id | INT | products |
| department_name | STRING | departments（JOIN 进来） |

**为什么要反规范化 (denormalise)？**
每次查询都要 JOIN 2 张小表, 通过少量数据冗余（可以接受， 优化查询简化的收益。


### dim_users（用户维度表）

**设计原则：聚合生成新维度**

Bronze 层根本没有用户表，user_id 散落在 orders 里。我们从 orders 按 user_id 聚合，生成一张描述用户特征的维度表。

| 字段 | 类型 | 计算逻辑 |
|------|------|---------|
| user_id | INT | 主键 |
| total_orders | INT | COUNT(order_id) |
| max_order_number | INT | MAX(order_number)，即该用户总共下了几单 |
| avg_days_between_orders | FLOAT | AVG(days_since_prior_order) |
| min_days_between_orders | FLOAT | MIN(days_since_prior_order) |
| max_days_between_orders | FLOAT | MAX(days_since_prior_order) |
| first_order_dow | INT | order_number = 1 的那条的 order_dow |
| first_order_hour | INT | order_number = 1 的那条的 order_hour_of_day |
| last_order_dow | INT | order_number = MAX 的那条的 order_dow |
| last_order_hour | INT | order_number = MAX 的那条的 order_hour_of_day |
