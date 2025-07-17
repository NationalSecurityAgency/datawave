import { Ref, WritableComputedRef, computed, defineComponent, ref } from 'vue';

const regexDataType = /^[1-9]\d*\s+types?$/;

interface Entry {
  key: string;
  value: string;
}

interface Markings {
  entry: Entry[];
}

interface Record {
  description: string;
  markings: Markings;
}

/*
*  Parses a Value to remove uncessessary 'undefined' or empty values and filters Description, here is how it works:
*  In this function, colValue can either be a populated array, an array that is null, or a string.
*  In the Descriptions and Types column, the value here will always be an array. Every other column is a string (hence 'any' type)
*  Specifically, for the descriptions block, it checks to see if the value is undefined/null (doesn't have a description).
*  Or if the description value has a length of 0 (if something was pulled incorrectly in the JSON).
*/
export function parseVal(colName: string, colValue: any, colDataTypeCount?: any) : string {
  if (colName === 'Types') {
    if (colValue == undefined) {
      return '';
    } else {
      return colValue.toString();
    }
  } else if (colName === 'Descriptions') {
    if (colValue == undefined || colValue.length === 0) {
      return '';
    }

    const firstEntry = colValue[0] as Record;
    if (!firstEntry.markings || !firstEntry.markings.entry) {
      return '';
    }

    const markingsEntry = firstEntry.markings.entry;

    const marking = markingsEntry.length > 0 ? markingsEntry[0].value : '';
    const markingAccess = markingsEntry.length > 1 ? markingsEntry[1].value : '';
    const description = firstEntry.description || '';

    return `${marking} ${markingAccess} ${description}`;
  } else if (colName === 'dataType' && regexDataType.test(colDataTypeCount)) {
    return colDataTypeCount;
  } else {
    return colValue.toString();
  }
}

// Produces the max substring for the table, adds '...' if above 34 chars.
export function maxSubstring(str: any, colName: any): any {
  if (str === undefined) {
    return;
  }

  switch (colName) {
    case 'fieldName':
    case 'internalFieldName':
      return str.length > 32 ? str.substring(0, 30) + ' ...' : str;
    case 'Types':
      // Types is offset by 2 to prevent overlapping in 'Tokenized' Column
      return str.length > 14 ? str.substring(0, 9) + ' ...' : str;
    case 'Descriptions':
      return str.length > 24 ? str.substring(0, 22) + ' ...' : str;
    case 'CopyPaste':
      return str.length > 42 ? str.substring(0, 40) + ' ...' : str;
    case 'dataType':
      return str.length > 12 ? str.substring(0, 10) + ' ...' : str;
    default:
      return str;
  }
}

// Defines how the expandability is parsed on the table.
export function buttonParse(col: any, row: any): boolean {
  return row.button == 1;
}

// Toggles how the row collapses based on the DOM. Filters visible rows.
export function toggleVisibility(row: any) {
  row.toggleVisibility();
}

// Set the Visibility in DOM, sorts and filters by lastUpdated, and the respective row to render button.
export function setVisibility(rows: readonly any[]) {
  const fieldVisibility: Map<string, Ref<boolean>> = new Map<
    string,
    Ref<boolean>
  >();
  const buttonValues: Map<string, number> = new Map<string, number>();
  const countValues: Map<string, number> = new Map<string, number>();

  for (const row of rows) {
    let rowMostRecentUpdated: number = row.lastUpdated;
    const currentRowInternalFieldName: any = row.internalFieldName;
    const currentRowDataType: any = row.dataType;

    if (currentRowDataType) {
      let currentValue = countValues.get(currentRowInternalFieldName) || 0;
      countValues.set(currentRowInternalFieldName, ++currentValue);
    }

    for (const scan of rows) {
      if (currentRowInternalFieldName === scan.internalFieldName && rowMostRecentUpdated < scan.lastUpdated) {
        rowMostRecentUpdated = scan.lastUpdated;
        buttonValues.set(currentRowInternalFieldName, rowMostRecentUpdated);
      }
    }
  }

  // This is how a Button is Rendered
  for (const row of rows) {
    // Checks to Render button
    if (
      buttonValues.has(row.internalFieldName) &&
      row.lastUpdated == buttonValues.get(row.internalFieldName)
    ) {
      row['duplicate'] = 1;
      row['button'] = true;

      row['dataTypeCount'] = countValues.get(row.internalFieldName) + ' types';
    }
    // Checks to Render Collapsible Row - Refreshes on Search
    else if (
      buttonValues.has(row.internalFieldName) &&
      row.lastUpdated != buttonValues.get(row.internalFieldName)
    ) {
      row['duplicate'] = 1;
      row['button'] = false;

      row['dataTypeCount'] = 0 + ' types';
    }
    // Renders a Normal Row (No Button, not Collapsible)
    else {
      row['duplicate'] = 0;
      row['button'] = false;

      row['dataTypeCount'] = 0 + ' types';
    }

    const internalFieldName = row.internalFieldName;
    if (!fieldVisibility.has(internalFieldName)) {
      fieldVisibility.set(internalFieldName, ref<boolean>(false));
    }

    const visibility = fieldVisibility.get(internalFieldName);

    row['toggleVisibility'] = () => {
      visibility!.value = !visibility?.value;
    };
    row['isVisible'] = visibility;
  }

  return rows;
}

// Lets the DOM know what is visible and what is not based on setVisibility filters.
export function isVisible(row: any) {
  return row.duplicate == 0 || row.isVisible.value;
}
