CREATE PROCEDURE [dbo].[Sync_AzureIPRangesFlat]
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE 
        @IP_Range_List NVARCHAR(MAX),
        @OfferingType NVARCHAR(100),
        @OfferingName NVARCHAR(100),
        @SubscriptionId NVARCHAR(100),
        @ResourceGroupName NVARCHAR(100);

    --------------------------------------------------------------------------------------------
    -- 1. Prepare Deleted IPs into temp table
    --------------------------------------------------------------------------------------------
    SELECT 
        t.ComponentName, 
        t.Region, 
        t.IPAddress, 
        t.StartIP, 
        t.EndIP
    INTO #DeletedIPs
    FROM dbo.AzureIPRangesFlat t
    LEFT JOIN Stage.AzureIPRangesFlat s
        ON s.ComponentName = t.ComponentName
       AND s.Region = t.Region
       AND s.IPAddress = t.IPAddress
    WHERE s.ComponentName IS NULL
      AND (t.IsDeleteFl IS NULL OR t.IsDeleteFl = 0);

    --------------------------------------------------------------------------------------------
    -- 2. Prepare grouped deleted IPs for Firewall removal
    --------------------------------------------------------------------------------------------
    SELECT 
        b.OfferingType,
        b.OfferingName,
        b.SubscriptionId,
        b.ResourceGroupName,
        STRING_AGG(CONCAT(d.StartIP,'-',d.EndIP), ',') AS IP_Range_List
    INTO #GroupedDeleted
    FROM #DeletedIPs d
    INNER JOIN Config.FirewallConfig b
        ON d.ComponentName = b.ComponentNameToBeWhitelisted
       AND d.Region = b.RegionToBeWhitelisted
    GROUP BY b.OfferingType, b.OfferingName, b.SubscriptionId, b.ResourceGroupName;

    --------------------------------------------------------------------------------------------
    -- 3. Mark deleted records
    --------------------------------------------------------------------------------------------
    UPDATE t
    SET t.IsDeleteFl = 1,
        t.UpdateTs = GETDATE()
    FROM dbo.AzureIPRangesFlat t
    INNER JOIN #DeletedIPs d
        ON t.ComponentName = d.ComponentName
       AND t.Region = d.Region
       AND t.IPAddress = d.IPAddress;

    --------------------------------------------------------------------------------------------
    -- 4. Loop through grouped deleted IPs → REMOVE from Firewall
    --------------------------------------------------------------------------------------------
    DECLARE curDel CURSOR LOCAL FAST_FORWARD FOR
        SELECT OfferingType, OfferingName, SubscriptionId, ResourceGroupName, IP_Range_List
        FROM #GroupedDeleted;

    OPEN curDel;

    FETCH NEXT FROM curDel INTO 
        @OfferingType, @OfferingName, @SubscriptionId, @ResourceGroupName, @IP_Range_List;

    WHILE @@FETCH_STATUS = 0
    BEGIN
        EXEC [dbo].[sp_ManageFirewallIP] 
            @OfferingType = @OfferingType,
            @OfferingName = @OfferingName,
            @SubscriptionId = @SubscriptionId,
            @ResourceGroupName = @ResourceGroupName,
            @IpRules = @IP_Range_List,
            @Action = 'remove';

        FETCH NEXT FROM curDel INTO 
            @OfferingType, @OfferingName, @SubscriptionId, @ResourceGroupName, @IP_Range_List;
    END

    CLOSE curDel;
    DEALLOCATE curDel;


    --------------------------------------------------------------------------------------------
    -- 5. Prepare New IPs into temp table
    --------------------------------------------------------------------------------------------
    SELECT 
        s.ComponentName, 
        s.Region, 
        s.IPAddress, 
        s.StartIP, 
        s.EndIP
    INTO #NewIPs
    FROM Stage.AzureIPRangesFlat s
    LEFT JOIN dbo.AzureIPRangesFlat t
        ON s.ComponentName = t.ComponentName
       AND s.Region = t.Region
       AND s.IPAddress = t.IPAddress
       AND t.IsDeleteFl = 0
    WHERE t.ComponentName IS NULL;

    --------------------------------------------------------------------------------------------
    -- 6. Prepare grouped new IPs for Firewall addition
    --------------------------------------------------------------------------------------------
    SELECT 
        b.OfferingType,
        b.OfferingName,
        b.SubscriptionId,
        b.ResourceGroupName,
        STRING_AGG(CONCAT(n.StartIP,'-',n.EndIP), ',') AS IP_Range_List
    INTO #GroupedNew
    FROM #NewIPs n
    INNER JOIN Config.FirewallConfig b
        ON n.ComponentName = b.ComponentNameToBeWhitelisted
       AND n.Region = b.RegionToBeWhitelisted
    GROUP BY b.OfferingType, b.OfferingName, b.SubscriptionId, b.ResourceGroupName;

    --------------------------------------------------------------------------------------------
    -- 7. Loop through grouped new IPs → ADD to Firewall
    --------------------------------------------------------------------------------------------
    DECLARE curAdd CURSOR LOCAL FAST_FORWARD FOR
        SELECT OfferingType, OfferingName, SubscriptionId, ResourceGroupName, IP_Range_List
        FROM #GroupedNew;

    OPEN curAdd;

    FETCH NEXT FROM curAdd INTO 
        @OfferingType, @OfferingName, @SubscriptionId, @ResourceGroupName, @IP_Range_List;

    WHILE @@FETCH_STATUS = 0
    BEGIN
        EXEC [dbo].[sp_ManageFirewallIP] 
            @OfferingType = @OfferingType,
            @OfferingName = @OfferingName,
            @SubscriptionId = @SubscriptionId,
            @ResourceGroupName = @ResourceGroupName,
            @IpRules = @IP_Range_List,
            @Action = 'add';

        FETCH NEXT FROM curAdd INTO 
            @OfferingType, @OfferingName, @SubscriptionId, @ResourceGroupName, @IP_Range_List;
    END

    CLOSE curAdd;
    DEALLOCATE curAdd;

    --------------------------------------------------------------------------------------------
    -- 8. Insert new IPs into target table
    --------------------------------------------------------------------------------------------
    INSERT INTO dbo.AzureIPRangesFlat (ComponentName, Region, IPAddress, StartIP, EndIP, IsDeleteFl, UpdateTs)
    SELECT ComponentName, Region, IPAddress, StartIP, EndIP, 0, GETDATE()
    FROM #NewIPs;

    --------------------------------------------------------------------------------------------
    -- 9. Clean up temp tables
    --------------------------------------------------------------------------------------------
    DROP TABLE IF EXISTS #DeletedIPs;
    DROP TABLE IF EXISTS #GroupedDeleted;
    DROP TABLE IF EXISTS #NewIPs;
    DROP TABLE IF EXISTS #GroupedNew;

END;
