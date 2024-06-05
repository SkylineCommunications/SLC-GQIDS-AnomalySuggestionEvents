using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Skyline.DataMiner.Analytics.GenericInterface;
using Skyline.DataMiner.Net.Filters;
using Skyline.DataMiner.Net.Helper;
using Skyline.DataMiner.Net.Messages;
using Skyline.DataMiner.Net.MetaData.DataClass;
using Skyline.DataMiner.Net;
using System;

[GQIMetaData(Name = "Anomaly Suggestion Events")]
public class ActiveAlarmsFilter : IGQIDataSource, IGQIOnInit, IGQIOnPrepareFetch
{
    private static readonly GQIStringColumn IDColumn = new GQIStringColumn("ID");
    private static readonly GQIStringColumn ElementColumn = new GQIStringColumn("Element");
    private static readonly GQIStringColumn ParameterColumn = new GQIStringColumn("Parameter");
    private static readonly GQIStringColumn ValueColumn = new GQIStringColumn("Value");
    private static readonly GQIDateTimeColumn TimeColumn = new GQIDateTimeColumn("Time");
    private static readonly GQIStringColumn SeverityColumn = new GQIStringColumn("Severity");
    private static readonly GQIStringColumn AnomalyTypeColumn = new GQIStringColumn("Anomaly Type");
    private static readonly GQIDateTimeColumn StartColumn = new GQIDateTimeColumn("Start");
    private static readonly GQIDateTimeColumn EndColumn = new GQIDateTimeColumn("End");
    private static readonly GQIStringColumn GuidColumn = new GQIStringColumn("Guid");

    private GQIDMS _dms;
    private Task<List<AlarmEventMessage>> _alarms;

    public OnInitOutputArgs OnInit(OnInitInputArgs args)
    {
        _dms = args.DMS;
        return new OnInitOutputArgs();
    }

    public OnArgumentsProcessedOutputArgs OnArgumentsProcessed(OnArgumentsProcessedInputArgs args)
    {
        return new OnArgumentsProcessedOutputArgs();
    }

    public OnPrepareFetchOutputArgs OnPrepareFetch(OnPrepareFetchInputArgs args)
    {
        // Getting events from source suggestion engine
        _alarms = Task.Factory.StartNew(() =>
        {
            var filterItemSourceIsSuggestionEngine = new AlarmFilterItemInt(AlarmFilterField.SourceID, AlarmFilterCompareType.Equality, new int[] { 63 });
            var filter = new AlarmFilter(filterItemSourceIsSuggestionEngine);
            var msg = new GetActiveAlarmsMessage() { Filter = filter };

            var alarmsResponse = _dms.SendMessage(msg) as ActiveAlarmsResponseMessage;
            if (alarmsResponse != null)
            {
                return alarmsResponse.ActiveAlarms.WhereNotNull().ToList();
            }

            return null;
        });
        return new OnPrepareFetchOutputArgs();
    }

    public GQIColumn[] GetColumns()
    {
        return new GQIColumn[]
        {
                IDColumn,
                ElementColumn,
                ParameterColumn,
                ValueColumn,
                TimeColumn,
                SeverityColumn,
                AnomalyTypeColumn,
                GuidColumn
        };
    }

    public GQIPage GetNextPage(GetNextPageInputArgs args)
    {
        if (_alarms == null)
            return new GQIPage(new GQIRow[0]);

        _alarms.Wait();

        var alarms = _alarms.Result;
        if (alarms == null)
            throw new GenIfException("No alarms found.");

        if (alarms.Count == 0)
            return new GQIPage(new GQIRow[0]);

        var rows = new List<GQIRow>(alarms.Count);

        foreach (var alarm in alarms)
        {
            if (!(alarm.MetaData is BehavioralAnomalyDetectionMetaData))
                continue;

            var cells = new[]
            {
                    new GQICell {Value= $"{alarm.DataMinerID}/{alarm.AlarmID }"}, // IDColumn
                    new GQICell {Value= alarm.ElementName }, // ElementColumn,
                    new GQICell {Value= alarm.ParameterName }, // ParameterColumn,
                    new GQICell {Value= alarm.DisplayValue }, // ValueColumn,
                    new GQICell {Value= alarm.TimeOfArrival.ToUniversalTime() }, // TimeColumn,
                    new GQICell {Value= alarm.Severity }, // SeverityColumn,
                    new GQICell {Value = (alarm.MetaData as BehavioralAnomalyDetectionMetaData)?.AnomalyType.ToString() ?? "Not an anomaly"}, // AnomalyType
                    new GQICell {Value = (alarm.MetaData as BehavioralAnomalyDetectionMetaData)?.Guid.ToString() ?? "Not an anomaly"} // Guid
                };


            var elementID = new ElementID(alarm.DataMinerID, alarm.ElementID);
            var elementMetadata = new ObjectRefMetadata { Object = elementID };

            var paramID = new ParamID(alarm.DataMinerID, alarm.ElementID, alarm.ParameterID, alarm.TableIdxPK);
            var paramMetadata = new ObjectRefMetadata { Object = paramID };


            DateTime start = alarm.CreationTime;
            DateTime end = alarm.TimeOfArrival;
            var timeRangeMetadata = new TimeRangeMetadata { StartTime = start, EndTime = end };
            var rowMetadata = new GenIfRowMetadata(new RowMetadataBase[] { elementMetadata, paramMetadata, timeRangeMetadata });

            rows.Add(new GQIRow(cells) { Metadata = rowMetadata });
        }

        return new GQIPage(rows.ToArray()) { HasNextPage = false };
    }
}