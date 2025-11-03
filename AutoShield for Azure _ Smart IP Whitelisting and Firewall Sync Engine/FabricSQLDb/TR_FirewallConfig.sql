CREATE TRIGGER [Config].[TR_FirewallConfig]
ON [Config].[FirewallConfig]
AFTER INSERT
AS
BEGIN
    SET NOCOUNT ON;

DECLARE 
    @IP_Range_List NVARCHAR(MAX),
    @OfferingType NVARCHAR(100),
    @OfferingName NVARCHAR(100),
    @SubscriptionId NVARCHAR(100),
    @ResourceGroupName NVARCHAR(100);

Select @IP_Range_List= STRING_AGG(CONCAT(StartIP, '-', EndIP), ','), @OfferingType=b.OfferingType,@OfferingName=b.OfferingName,@SubscriptionId=b.SubscriptionId,@ResourceGroupName=b.ResourceGroupName
from dbo.AzureIPRangesFlat a
Left join Config.FirewallConfig b on a.ComponentName=b.ComponentNameToBeWhitelisted and a.Region=b.RegionToBeWhitelisted
Inner Join inserted c on c.OfferingType=b.OfferingType and c.OfferingName=b.OfferingName and c.ResourceGroupName=b.ResourceGroupName
group by b.OfferingType,b.OfferingName,b.SubscriptionId,b.ResourceGroupName


Exec [dbo].[sp_ManageFirewallIP] @OfferingType=@OfferingType,@OfferingName=@OfferingName,@SubscriptionId=@SubscriptionId,@ResourceGroupName=@ResourceGroupName,@IpRules=@IP_Range_List,@Action='add'

END;
