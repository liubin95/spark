CREATE EXTERNAL TABLE city
(
    id   INT,
    name STRING,
    area STRING
) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t';

CREATE EXTERNAL TABLE prod
(
    id        INT,
    name      STRING,
    sell_type STRING
) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t';

CREATE EXTERNAL TABLE user_visit
(
    `date`             STRING,
    user_id            STRING,
    session_id         STRING,
    page_id            BIGINT,
    action_time        STRING,
    search_key_word    STRING,
    click_category_id  BIGINT,
    click_product_id   BIGINT,
    order_category_ids STRING,
    order_product_ids  STRING,
    pay_category_ids   STRING,
    pay_product_ids    STRING,
    city_id            BIGINT
) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t';

LOAD DATA INPATH '/city_info.txt' INTO TABLE city;
LOAD DATA INPATH '/product_info.txt' INTO TABLE prod;
LOAD DATA INPATH '/user_visit_action.txt' INTO TABLE user_visit;


SELECT *
FROM city;
SELECT *
FROM prod;
SELECT *
FROM user_visit;


-- 各个区域的热门商品（点击量）前三名
-- 并备注每个商品在主要城市种的分布比例
-- 华北 商品A 1000  北京20%，天津10%，其他70%

-- 地区市点击商品top3
SELECT t5.area,
       t6.name,
       area_num,
       -- 城市备注
       t7.city_num,
       CONCAT(STRING((CAST((SPLIT(city_num, '-')[1] / area_num) AS DECIMAL(13, 2))) * 100),
              '%') AS city_1,
       t7.city_num_2,
       CONCAT(STRING((CAST((SPLIT(city_num_2, '-')[1] / area_num) AS DECIMAL(13, 2))) * 100),
              '%') AS city_2,
       CONCAT(STRING((CAST(
               ((area_num - SPLIT(city_num, '-')[1] - SPLIT(city_num_2, '-')[1]) /
                area_num) AS DECIMAL(13, 2))) * 100),
              '%') AS city_o

FROM (
-- 地区点击商品排序
         SELECT t3.*,
                t4.*,
                ROW_NUMBER() OVER (PARTITION BY t4.area ORDER BY t3.num DESC ) AS area_rank,
                SUM(t3.num) OVER (PARTITION BY t3.click_product_id,t4.area )   AS area_num
         FROM (
                  -- 城市点击商品top3
                  SELECT *
                  FROM (
                           -- 城市点击商品排序
                           SELECT *,
                                  ROW_NUMBER()
                                          OVER (PARTITION BY t1.city_id ORDER BY num DESC ) AS city_rank
                           FROM (
                                    -- 城市点击商品数量
                                    SELECT city_id,
                                           click_product_id,
                                           COUNT(*) AS num
                                    FROM user_visit
                                    WHERE click_product_id != -1
                                    GROUP BY city_id, click_product_id
                                ) t1
                       ) t2
              ) t3
                  JOIN city t4 ON t3.city_id = t4.id
     ) t5
         JOIN prod t6 ON t6.id = t5.click_product_id
    -- 城市备注
-- （地区，点击商品，[城市1点击数量-城市2点击数量]）
         JOIN (
    -- 行转列
    SELECT click_product_id,
           area,
           city_num,
           LEAD(city_num)
                OVER (PARTITION BY click_product_id, area ORDER BY city_rank ) AS city_num_2
    FROM (
             -- 每个地区，每个商品点击数， 城市top2
             SELECT *,
                    CONCAT_WS('-', name, STRING(num)) AS city_num
             FROM (
                      SELECT *,
                             ROW_NUMBER()
                                     OVER (PARTITION BY t3.area,t3.click_product_id ORDER BY num DESC ) AS city_rank
                      FROM (
                               -- 每个地区,每个城市,每个商品点击数量
                               SELECT t1.*,
                                      t2.name,
                                      t2.area
                               FROM (
                                        -- 城市点击商品数量
                                        SELECT city_id,
                                               click_product_id,
                                               COUNT(*) AS num
                                        FROM user_visit
                                        WHERE click_product_id != -1
                                        GROUP BY city_id, click_product_id
                                    ) t1
                                        JOIN city t2 ON t1.city_id = t2.id
                           ) t3
                  ) t4
             WHERE t4.city_rank < 3
         ) t5
) t7 ON t7.click_product_id = t5.click_product_id AND t7.area = t5.area AND
        t7.city_num_2 IS NOT NULL

WHERE t5.area_rank < 4
  AND t5.city_rank < 4
