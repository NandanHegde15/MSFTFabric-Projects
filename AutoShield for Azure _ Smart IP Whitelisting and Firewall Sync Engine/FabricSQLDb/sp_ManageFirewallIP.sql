CREATE PROCEDURE [dbo].[sp_ManageFirewallIP]
    @OfferingType NVARCHAR(20),          -- 'sql' or 'storage'
    @OfferingName NVARCHAR(100),         -- resource name
    @SubscriptionId NVARCHAR(100),       -- SubscriptionId
    @ResourceGroupName NVARCHAR(100),    -- resource group
    @Action NVARCHAR(10),                -- 'add' or 'remove'
    @IpRules NVARCHAR(MAX)               -- comma-separated IPs or ranges
AS
BEGIN
    SET NOCOUNT ON;
 
    DECLARE @Url NVARCHAR(500) = N'https://<<appname>>.azurewebsites.net/api/<<functionname>>';
    DECLARE @Payload NVARCHAR(MAX);
    DECLARE @IpJson NVARCHAR(MAX);
 
    SET @IpJson = (
        SELECT '[' + STRING_AGG(CONCAT('"', TRIM(value), '"'), ',') + ']'
        FROM STRING_SPLIT(@IpRules, ',')
    );
 
    SET @Payload = JSON_OBJECT(
    'subscriptionId': @SubscriptionId,
    'serviceType': @OfferingType,
    'serviceName': @OfferingName,
    'resourceGroup': @ResourceGroupName,
    'action': @Action,
    'ipRules': JSON_QUERY(@IpJson)
    );
   
    DECLARE @Response NVARCHAR(MAX);
 
    EXEC sp_invoke_external_rest_endpoint
        @url = @Url,
        @method = 'POST',
        @headers = N'{"Content-Type":"application/json"}',
        @payload = @Payload,
        @timeout = 120,  -- allow time for function execution
        @response = @Response OUTPUT;
 
   INSERT INTO LOG.TriggerResponse(OfferingType,OfferingName,SubscriptionId,ActionType,IpJson,Response)
   SELECT @OfferingType,@OfferingName,@SubscriptionId,@Action,@IpJson,@Response AS Response;
END;
