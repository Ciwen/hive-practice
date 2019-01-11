-- 创建日志表
CREATE TABLE IF NOT EXISTS `bigdata.weblog` (
    `time_tag`      bigint      COMMENT '时间',
    `active_name`   string      COMMENT '事件名称',
    `device_id`     string      COMMENT '设备id',
    `session_id`    string      COMMENT '会话id',
    `user_id`       string      COMMENT '用户id',
    `ip`            string      COMMENT 'ip地址',
    `address`       map<string, string> COMMENT '地址',
    `req_url`       string      COMMENT 'http请求地址',
    `action_path`   array<string>   COMMENT '访问路径',
    `product_id`    string      COMMENT '商品id',
    `order_id`      string      COMMENT '订单id' )
PARTITIONED BY (`day` string COMMENT '日期')
ROW FORMAT SERDE
    'org.openx.data.jsonserde.JsonSerDe'
STORED AS INPUTFORMAT
    'org.apache.hadoop.mapred.TextInputFormat'
OUTPUTFORMAT
    'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION -- 数据源所在目录
    '/user/hadoop/weblog';

-- 给分区表weblog增加分区
ALTER TABLE `bigdata.weblog` ADD PARTITION (day='2018-05-29') LOCATION '/user/hadoop/weblog/day=2018-05-29';
ALTER TABLE `bigdata.weblog` ADD PARTITION (day='2018-05-30') LOCATION '/user/hadoop/weblog/day=2018-05-30';
ALTER TABLE `bigdata.weblog` ADD PARTITION (day='2018-05-31') LOCATION '/user/hadoop/weblog/day=2018-05-31';

-- 给分区表删除分区
ALTER TABLE `big.weblog` DROP IF EXISTS PARTITION (day='2018-05-09');

-- 创建会员表
CREATE TABLE IF NOT EXISTS `bigdata.member`(
    `user_id`       string      COMMENT '用户id',
    `nick_name`     string      COMMENT '昵称',
    `name`          string      COMMENT '名称',
    `gender`        string      COMMENT '性别',
    `register_time` bigint      COMMENT '注册时间',
    `birthday`      bigint      COMMENT '生日',
    `device_type`   string      COMMENT '设别类型')
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
STORED AS INPUTFORMAT 
    'org.apache.hadoop.mapred.TextInputFormat'
OUTPUTFORMAT 
    'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION  -- 直接使用的外部数据
    '/user/hadoop/hive/member'; 

-- 查询下订单的user_id, gender, register_time
SELECT   -- jobs=1 maps=2 reduces=0
    t1.user_id,
    t2.name,
    gender,
    from_unixtime(cast(register_time/100 as bigint), 'yyyy-MM-dd HH:mm:ss') as register_date
FROM
  (SELECT user_id FROM weblog WHERE active_name='order') AS t1
JOIN
  (SELECT user_id, gender, name, register_time FROM member) AS t2
ON t1.user_id=t2.user_id;

-- 统计不同产品页面的访问次数
SELECT --jobs=1 maps=2 reduces=2
    regexp_extract(req_url, '.*/product/([0-9]+).*', 1) AS product_no,
    req_url,
    count(1)
FROM weblog
WHERE active_name='pageview' and req_url like '%/product/%'
GROUP BY req_url;

-- 创建订单表
CREATE EXTERNAL TABLE `bigdata.orders` (
    `order_id`      string      COMMENT '订单id',
    `user_id`       string      COMMENT '用户id',
    `product_id`    string      COMMENT '产品id',
    `order_time`    bigint      COMMENT '下单时间',
    `pay_amount`    double      COMMENT '金额')
ROW FORMAT SERDE 
    'org.openx.data.jsonserde.JsonSerDe'
STORED AS INPUTFORMAT 
    'org.apache.hadoop.mapred.TextInputFormat'
OUTPUTFORMAT 
    'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION 
    '/user/hadoop/hive/orders';

-- 男女花钱对比
SELECT
    gender,
    count(t2.user_id) as count_order,
    sum(pay_amount) as sum_amount,
    avg(pay_amount) as avg_amount
FROM
  (SELECT user_id, gender FROM member) AS t1
JOIN
  (SELECT user_id, pay_amount FROM orders) AS t2
ON  t1.user_id=t2.user_id
GROUP BY gender;


-- 查询出商品访问量
-- 按照product_id分组，统计每组的总数
SELECT
    regexp_extract(req_url, '.*/product/([0-9]+).*', 1) AS product_id,
    count(1) count_user
FROM `weblog`
WHERE active_name='pageview' 
GROUP BY regexp_extract(req_url, '.*/product/([0-9]+).*', 1)
SORT BY count_user DESC;


-- 窗口函数使用
-- 查询出下单用户的着陆页、下单前的最后页面以及下单前浏览了多少页面
SELECT user_id, session_id, active_name, landing_page, last_page, count_visit
FROM
  (SELECT -- 窗口函数所作用的行集是整个weblog
      user_id,
      session_id,
      active_name,
      first_value(req_url) OVER (PARTITION BY session_id ORDER BY time_tag ASC) AS landing_page,
      lag(req_url, 1) OVER (PARTITION BY session_id ORDER BY time_tag ASC) AS last_page,
      count(IF(active_name='pageview', req_url, null)) OVER (PARTITION BY session_id ORDER BY time_tag ASC ROWS between unbounded preceding and current row) AS count_visit
  FROM weblog) AS t1
-- 按照订单时间划分，每一行都是某一订单的用户名、会话ID、所在会话的第一个url、订单前一个url以及同一会话内该订单前所有浏览url的总数  
WHERE active_name='order';

-- 多行转一列
-- 从订单表查询用户购买的所有product
SELECT 
    user_id,
    collect_set(product_id),
    collect_list(product_id)
FROM orders
GROUP BY user_id;

-- 从订单表中查询购买商品数大于1的用户
SELECT 
    user_id,
    collect_set(product_id),
    collect_list(product_id)
FROM orders
GROUP BY user_id
HAVING size(collect_list(product_id))>1;


-- 从多个表中获取用户的标签

-- step1：建立中间表和大宽表
-- 创建中间表【标签名-标签值】
CREATE TABLE `user_tag_value` (
    `user_id`   string  COMMENT '用户ID',
    `tag`           string      COMMENT '标签',
    `value`         string      COMMENT '标签值'
)PARTITIONED BY (`module` string COMMENT '标签模块') 
ROW FORMAT SERDE
    'org.openx.data.jsonserde.JsonSerDe'
STORED AS INPUTFORMAT
    'org.apache.hadoop.mapred.TextInputFormat'
OUTPUTFORMAT
    'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';

-- 创建用户画像大宽表
CREATE TABLE `bigdata.user_profile` (
    `user_id`       string      COMMENT 'user id',
    `profile`       string      COMMENT '用户标签'
) ROW FORMAT SERDE
    'org.openx.data.jsonserde.JsonSerDe'
STORED AS INPUTFORMAT
    'org.apache.hadoop.mapred.TextInputFormat'
OUTPUTFORMAT
    'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';

-- step2：向中间表的各分区装载数据
-- partition1 性别、年龄、设备类型、注册日期 写入basic_info分区
INSERT OVERWRITE TABLE user_tag_value PARTITION (module='basic_info')
SELECT user_id, mp['key'], mp['value']
FROM  -- 多列构造为map数组，通过lateral view + UDTF函数【输入array分割为多行输出】
    (
    SELECT 
        user_id,
        array(map('key', 'gender', 'value', gender),
            map('key', 'age', 'value', cast(2018-from_unixtime(cast(birthday/1000 as bigint), 'yyyy') as string)),
            map('key', 'device_type', 'value', device_type),
            map('key', 'register_time', 'value', from_unixtime(cast(register_time/1000 as bigint), 'yyyy-MM-dd'))) AS arr
    FROM member
    ) s lateral view explode(arr) arrtable as mp;

-- partition2 首次下单时间、最近下单时间、下单次数、下单金额 写入consume_info分区
INSERT OVERWRITE TABLE user_tag_value PARTITION(module='consume_info')
SELECT user_id, mp['key'], mp['value']
FROM
    (SELECT 
        user_id,
        array(map('key', 'first_order_time', 'value', min(order_time)),
            map('key', 'last_order_time', 'value', max(order_time)),
            map('key', 'order_count', 'value', count(1)),
            map('key', 'order_sum', 'value', sum(pay_amount))) AS arr
    FROM orders 
    GROUP BY user_id)
AS s lateral view explode(arr) arrtable as mp;

-- partition3 地理位置信息
INSERT OVERWRITE TABLE user_tag_value PARTITION(module='geography_info')
SELECT user_id, 'province' as tag, province    -- 给表增加一列字段，字段值为常量'province'
FROM
    (SELECT 
        user_id,
        row_number() over(partition by user_id order by time_tag desc) as order_rank,
        address['province'] as province
    FROM bigdata.weblog
    WHERE active_name='pay'
    ) t1
WHERE order_rank = 1;

--step3：将标签表 “多行转一列” 为大宽表
INSERT OVERWRITE TABLE `user_profile`
SELECT 
    user_id,
    concat('{', concat_ws(',', collect_set(concat('"', tag, '"', ':', '"', value, '"'))), '}') as json_string
FROM user_tag_value
GROUP BY user_id;

-- 通过大宽表查询用户标签
SELECT 
    user_id, 
    get_json_object(profile, '$.gender'),
    get_json_object(profile, '$.province'),
    get_json_object(profile, '$.first_order_time'),
    get_json_object(profile, '$.age')
FROM user_profile
LIMIT 10;