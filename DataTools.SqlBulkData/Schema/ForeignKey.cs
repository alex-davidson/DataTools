using DataTools.SqlBulkData.PersistedModel;

namespace DataTools.SqlBulkData.Schema
{
    public class ForeignKey
    {
        public string Name { get; set; }
        public string Description { get; set; }

        public TableIdentifier PrimaryTable { get; set; }
        public string[] PrimaryColumns { get; set; }

        public TableIdentifier ForeignTable { get; set; }
        public string[] ForeignColumns { get; set; }

        public bool EnforceConstraint { get; set; } = true;
        public bool EnforceForReplication { get; set; } = true;
        
        public Rule UpdateRule { get; set; }
        public Rule DeleteRule { get; set; }

        public override string ToString() => Name;

        public enum Rule
        {
            NoAction,
            Cascade,
            SetNull,
            SetDefault
        }
    }
}
