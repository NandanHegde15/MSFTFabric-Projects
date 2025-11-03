import fabric.functions as fn

udf = fn.UserDataFunctions()

@udf.connection(argName="sqlDB", alias="ipdb")
@udf.function()
def InsertIntoConfig(
    sqlDB: fn.FabricSqlConnection,
    OfferingType: str,
    OfferingName: str,
    SubscriptionId: str,
    ResourceGroupName: str,
    ComponentNameToBeWhitelisted: str,
    RegionToBeWhitelisted: str
) -> str:
    """
    Inserts data into Config.FirewallConfig table in the connected SQL DB.
    """

    # Tuple of data to insert
    data = (
        OfferingType,
        OfferingName,
        SubscriptionId,
        ResourceGroupName,
        ComponentNameToBeWhitelisted,
        RegionToBeWhitelisted
    )

    # Establish SQL connection
    connection = sqlDB.connect()
    cursor = connection.cursor()

    # Insert data
    insert_query = '''
        INSERT INTO Config.FirewallConfig
        (
            OfferingType,
            OfferingName,
            SubscriptionId,
            ResourceGroupName,
            ComponentNameToBeWhitelisted,
            RegionToBeWhitelisted
        )
        VALUES (?, ?, ?, ?, ?, ?);
    '''
    cursor.execute(insert_query, data)

    # Commit the transaction
    connection.commit()

    # Close connections
    cursor.close()
    connection.close()

    return "Record inserted successfully into Config.FirewallConfig."
