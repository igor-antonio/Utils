IF(OBJECT_ID('dbo.batch_resp_statistics') IS NOT NULL)
BEGIN
    DROP TABLE dbo.batch_resp_statistics;
END;
GO

CREATE TABLE
    dbo.batch_resp_statistics(
        id              TINYINT     NOT NULL
        , date_time     DATETIME    NOT NULL
        , object_name	NCHAR(128)  NOT NULL
        , counter_name	NCHAR(128)  NOT NULL
        , instance_name	NCHAR(128)  NOT NULL
        , cntr_value	BIGINT      NOT NULL);
GO

IF(OBJECT_ID('dbo.usp_batch_resp_statistics') IS NOT NULL)
BEGIN
    DROP PROCEDURE dbo.usp_batch_resp_statistics;
END;
GO

CREATE PROCEDURE dbo.usp_batch_resp_statistics AS
BEGIN
    
    SET NOCOUNT ON;

    INSERT INTO
        dbo.batch_resp_statistics(
            id
            , date_time
            , object_name	
            , counter_name
            , instance_name
            , cntr_value)
    SELECT
        2
        , GETDATE()
        , object_name	
        , counter_name	
        , instance_name	
        , cntr_value	
    FROM sys.dm_os_performance_counters AS a
    WHERE
        a.object_name = 'MSSQL$C:Batch Resp Statistics'
        AND 
            a.instance_name = 'Elapsed Time:Total(ms)';

    WITH cte_batch_resp_statistics AS(
        SELECT
            b.date_time
            , a.object_name	
            , a.counter_name	
            , a.instance_name	
            , b.cntr_value - a.cntr_value AS cntr_value
        FROM
            dbo.batch_resp_statistics AS a
        JOIN
            dbo.batch_resp_statistics AS b
            ON
                a.object_name = b.object_name
                AND
                    a.counter_name = b.counter_name
                AND
                    a.instance_name = b.instance_name
                AND
                    b.id = a.id + 1
        WHERE
            (b.cntr_value - a.cntr_value) > 0)

    , cte_batch_resp_statistics_sum AS(
        SELECT
            a.date_time
            , CAST(ISNULL(SUM(a.cntr_value), 0) AS DECIMAL(38,16)) AS cntr_value
        FROM
            cte_batch_resp_statistics AS a
        GROUP BY
            a.date_time)

/* end of ctes*/

        SELECT
            (SELECT date_time FROM cte_batch_resp_statistics_sum) AS date_time
            , '>=000000 & <000100' AS batch_time_ms
            , CAST(ROUND((CAST(ISNULL(SUM(a.cntr_value), 0) AS DECIMAL(38,16))/(SELECT cntr_value FROM cte_batch_resp_statistics_sum)) * CAST(100 AS DECIMAL(38,16)), 1, 1) AS DECIMAL(38,1)) AS [percent]
        FROM
            cte_batch_resp_statistics AS a
        WHERE
            a.counter_name IN(
                'Batches >=000000ms & <000001ms'
                , 'Batches >=000001ms & <000002ms'
                , 'Batches >=000002ms & <000005ms'
                , 'Batches >=000005ms & <000010ms'
                , 'Batches >=000010ms & <000020ms'
                , 'Batches >=000020ms & <000050ms'
                , 'Batches >=000050ms & <000100ms')

    UNION ALL

        SELECT
            (SELECT date_time FROM cte_batch_resp_statistics_sum) AS date_time
            , '>=000000 & <000200' AS batch_time_ms
            , CAST(ROUND((CAST(ISNULL(SUM(a.cntr_value), 0) AS DECIMAL(38,16))/(SELECT cntr_value FROM cte_batch_resp_statistics_sum)) * CAST(100 AS DECIMAL(38,16)), 1, 1) AS DECIMAL(38,1)) AS [percent]
        FROM
            cte_batch_resp_statistics AS a
        WHERE
            a.counter_name IN(
                'Batches >=000000ms & <000001ms'
                , 'Batches >=000001ms & <000002ms'
                , 'Batches >=000002ms & <000005ms'
                , 'Batches >=000005ms & <000010ms'
                , 'Batches >=000010ms & <000020ms'
                , 'Batches >=000020ms & <000050ms'
                , 'Batches >=000050ms & <000100ms'
                , 'Batches >=000100ms & <000200ms')

    UNION ALL

        SELECT
            (SELECT date_time FROM cte_batch_resp_statistics_sum) AS date_time
            , '>=000000 & <000500' AS batch_time_ms
            , CAST(ROUND((CAST(ISNULL(SUM(a.cntr_value), 0) AS DECIMAL(38,16))/(SELECT cntr_value FROM cte_batch_resp_statistics_sum)) * CAST(100 AS DECIMAL(38,16)), 1, 1) AS DECIMAL(38,1)) AS [percent]
        FROM
            cte_batch_resp_statistics AS a
        WHERE
            a.counter_name IN(
                'Batches >=000000ms & <000001ms'
                , 'Batches >=000001ms & <000002ms'
                , 'Batches >=000002ms & <000005ms'
                , 'Batches >=000005ms & <000010ms'
                , 'Batches >=000010ms & <000020ms'
                , 'Batches >=000020ms & <000050ms'
                , 'Batches >=000050ms & <000100ms'
                , 'Batches >=000100ms & <000200ms'
                , 'Batches >=000200ms & <000500ms');

    DELETE FROM  
        dbo.batch_resp_statistics
    WHERE
        id = 1;

    UPDATE
        dbo.batch_resp_statistics
    SET
        id = 1
    WHERE
        id = 2;
END;
