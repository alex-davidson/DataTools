namespace DataTools.SqlBulkData.Columns
{
    public interface IColumnDefinition
    {
        string Name { get; }
        IColumnSerialiser GetSerialiser();
    }
}
