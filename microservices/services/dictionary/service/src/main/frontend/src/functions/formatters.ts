/* eslint-disable no-var */
import { Ref, WritableComputedRef, computed, defineComponent, ref } from 'vue';

// Parses a Value to remove uncessessary 'undefined' or empty values.
export function parseVal(colName: any, colValue: any): string {
  if (colName === 'Types' || colName === 'Descriptions') {
    if (colValue == undefined) {
      return '';
    } else {
      return colValue.toString();
    }
  } else {
    return colValue;
  }
}

// Produces the max substring for the table, adds '...' if above 34 chars.
export function maxSubstring(str: any, colName: any): any {
  if (str == undefined) {
    return;
  } else if ((colName === 'fieldName' || colName === 'internalFieldName') && str.length > 32) {
    return str.substring(0, 30) + ' ...';
  } else if ((colName === 'Types') && str.length > 14) {
    // Types is offset by 2 to prevent overlapping in 'Tokenized' Column
    return str.substring(0, 9) + ' ...';
  } else if ((colName === 'Descriptions') && str.length > 24) {
    return str.substring(0, 22) + ' ...';
  } else if ((colName === 'CopyPaste') && str.length > 42) {
    return str.substring(0, 40) + ' ...';
  } else {
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

  for (const row of rows) {
    let rowMostRecentUpdated: number = row.lastUpdated;
    const currentRowInternalFieldName: any = row.internalFieldName;
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
      row['duplicate'] = 0;
      row['button'] = true;
    }
    // Checks to Render Collapsible Row - Refreshes on Search
    else if (
      buttonValues.has(row.internalFieldName) &&
      row.lastUpdated != buttonValues.get(row.internalFieldName)
    ) {
      row['duplicate'] = 1;
      row['button'] = false;
    }
    // Renders a Normal Row (No Button, not Collapsible)
    else {
      row['duplicate'] = 0;
      row['button'] = false;
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
