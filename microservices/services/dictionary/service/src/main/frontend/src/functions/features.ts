import { Notify} from 'quasar';
import * as Formatters from './formatters';

// Feature - Copies the Label to Clipboard
export async function copyLabel(colValue: any) {
  await navigator.clipboard.writeText(colValue);
  Notify.create({
    type: 'info',
    message: Formatters.maxSubstring(
                  Formatters.parseVal('CopyPaste', colValue), 'CopyPaste'
                ) + ' Copied to Clipboard.',
    color: 'cyan-8',
    icon: 'bi-clipboard-fill'
  });
}

export function toolTipGen(colLabel: any): string {
  const tooltips: Record<string, string> = {
    'fieldName': 'Name of the field returned in the results. Can also be used as the field to query.',
    'internalFieldName': 'The raw name as it is stored in the database. This field can be used in a query.',
    'dataType': 'Used to partition the data for ingest and query. Not all fields are valid for all data types.',
    'indexOnly': 'Field is only found in the Index Tables, not in the Shard Table. This field will not be returned in results and cannot be used in a filter function (#INCLUDE, #EXCLUDE, #ISNULL, #ISNOTNULL)',
    'forwardIndexed': 'Value is in Forward Index (lexicographical ordered listing of values). Fields that are indexed can be queried as a stand alone term or anchor term in a query. Fields that are not indexed must be paired with an indexed field in the query.',
    'reverseIndexed': 'Value is in the Reverse Index (lexicographical ordered listing of values stored in reverse "esrever" order). Fields that are reverse indexed can be queried with a leading wildcard as a stand alone term or anchor term in a query.',
    'normalized': 'Indicates the value stored in the Shard Table is normalized.',
    'Types': 'Indicates what type of normalization is applied.',
    'tokenized': 'Indicates the value stored in the index/searched is tokenized.',
    'Descriptions': 'Description of the data contained in the field.',
    'lastUpdated': 'The last time data has been received for the field.'
  };

  return tooltips[colLabel] || colLabel;
}
