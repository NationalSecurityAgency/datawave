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
